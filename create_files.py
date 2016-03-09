import urllib2
import json
import sys,os
import ftplib
import cStringIO
import datetime
import time
import wget
import unicodecsv
import zipfile
import pprint
import re
import kalkatireader
import urllib2
import shutil
#import shapefile

NAVICI_FTP_URL = os.environ.get('NAVICI_FTP_URL')

assert NAVICI_FTP_URL

TMP_PATH = os.path.realpath(os.path.join(os.path.dirname(os.path.realpath(__file__)),'..','jltikku','temp'))

def parseFlag(flaginfo):
    flaginfo = flaginfo.split(',')
    flaginfo[0] = flaginfo[0].split('-')
    flaginfo[0] = map(lambda d: datetime.datetime.strptime(d,'%Y%m%d'),flaginfo[0])
    flaginfo[1] = datetime.datetime.strptime(flaginfo[1],'%Y%m%d%H%M%S')

    return flaginfo
kalkati_flags = {}
kalkati_flags_parsed = {}
if os.path.exists(os.path.join(TMP_PATH,'kalkati_flags.txt')):
    with open(os.path.join(TMP_PATH,'kalkati_flags.txt'),'r') as f:
        kalkati_flags = json.load(f)

for k in kalkati_flags:
    kalkati_flags_parsed[k] = parseFlag(kalkati_flags[k])

if not os.path.exists(TMP_PATH):
    os.makedirs(TMP_PATH)
naviciftp = None

opener = urllib2.build_opener(urllib2.CacheFTPHandler)
urllib2.install_opener(opener)
for kfn in ('vakio','ep','pika','linjausok'):
    print 'Retrieving %s flag' % kfn
    zfn = os.path.join(TMP_PATH,'%s.zip' % kfn)

    if kfn in kalkati_flags_parsed and os.path.exists(zfn):
        if datetime.datetime.now()-kalkati_flags_parsed[kfn][1] < datetime.timedelta(days=6):
            print kfn,'flag less than 6 hours. Skipping'
            continue

    r = urllib2.urlopen(NAVICI_FTP_URL + '/production/%s/flag.txt' % kfn)
    origflag = flaginfo = r.read()
    r.close()

    flaginfo = parseFlag(flaginfo)
    if kfn in kalkati_flags_parsed and os.path.exists(zfn):
        if kalkati_flags_parsed[kfn][1] >= flaginfo[1]:
            print kfn,'is up to date. Skipping'
            continue
    kalkati_flags[kfn] = origflag

    print 'Loading file %s.zip' % kfn
    st = time.time()
    zf = open(zfn,'wb')
    r = urllib2.urlopen(NAVICI_FTP_URL + '/production/%s/all.zip' % kfn)
    shutil.copyfileobj(r,zf)
    r.close()
    print zf.tell(),'bytes. Took',time.time()-st
    zf.close()

if naviciftp:
    naviciftp.quit()
print 'FTP done. Kalkati up to date'

with open(os.path.join(TMP_PATH,'kalkati_flags.txt'),'wb') as f:
    json.dump(kalkati_flags,f)

###########
for fn in ('vuoro.csv','pysakkiketjut.zip','linjaukset.zip'):
    print fn
    if os.path.exists(os.path.join(TMP_PATH,fn)):
        print 'exists. skipping'
        continue
        os.unlink(os.path.join(TMP_PATH,fn))
    wget.download('https://koontikartta.navici.com/tiedostot/%s' % fn,os.path.join(TMP_PATH,fn))
    print
    print 'done'


##########
## KALKATIT ##
trnsattrs = {}
footnotes = {}
vuoro_wkdays = {}
def parseFootnoteAsWkdDays(firstday,vector):
    day = datetime.datetime.strptime(firstday,'%Y-%m-%d')
    days = {
        'totals':{}
    }
    for c in vector:
        wkd = day.isoweekday()

        if not wkd in days:
            days[wkd] = 0
            days['totals'][wkd] = 0.0

        days['totals'][wkd]+=1
        if c == '1':
            days[wkd] += 1

        day+=datetime.timedelta(days=1)

    return days


a = False
def wkddayCallback(tag,data):
    if not tag in ('trnsattr','footnote','service'):
        return True

    if tag == 'trnsattr':
        vdata = kalkatireader.parseValluTrnsattr(data['@Name'])
        if vdata == None:
            return True
        trnsattrs[data['@TrnsattrId']] = vdata
        #sys.exit(1)
    elif tag == 'footnote':
        pf =  parseFootnoteAsWkdDays(data['@Firstdate'],data['@Vector'])

        if len(pf) == 1:
            return True
        #pf['ratios'] = {}
        for k in (('arki',(1,2,3,4,5)),('la',(6,)),('su',(7,))):
            ratios = (pf[d]/pf['totals'][d] for d in k[1] if d in pf)
            pf[k[0]] = any((r > 0.5 for r in ratios))

        footnotes[data['@FootnoteId']] = pf
    elif tag == 'service':

        if not isinstance(data['ServiceAttribute'],list):
            data['ServiceAttribute'] = [data['ServiceAttribute'],]
        #
        vuoroinfo = None
        for sa in data['ServiceAttribute']:

            if not sa['@AttributeId'] in trnsattrs:
                continue
            if 'vuoroid' in trnsattrs[sa['@AttributeId']]:
                vuoroinfo = trnsattrs[sa['@AttributeId']]
                break

        if not vuoroinfo:
            print service['@ServiceId'],'no info'
            return True

        footnoteid = data['ServiceValidity']['@FootnoteId']
        #print repr(vuoroinfo['vuoroid']),'wkd data'
        vuoro_wkdays[int(vuoroinfo['vuoroid'])] = (footnotes[footnoteid]['arki'],footnotes[footnoteid]['la'],footnotes[footnoteid]['su'])

for kfn in ('vakio','ep','pika','linjausok'):
    trnsattrs = {}
    footnotes = {}
    zfn = os.path.join(TMP_PATH,'%s.zip' % kfn)
    kalkatireader.parseKalkatifile(zfn,wkddayCallback)
    print kfn,len(trnsattrs),'trnsattrs'
    print kfn,len(footnotes),'footnotes'
    print len(vuoro_wkdays)

##########
## VUORO.CSV ##

vuorodata = {}
with open(os.path.join(TMP_PATH,'vuoro.csv'), 'rb') as csvfile:
    reader = unicodecsv.reader(csvfile, encoding='utf-8-sig',delimiter=';')
    header = None
    for l in reader:
        if not header:
            l[0] = l[0].strip('"')
            header = l
            continue

        try:
            l[9] = int(l[9])
            l[11] = int(l[11])
            l[18] = int(l[18])
            l[24] = int(l[24])
            l[28] = False if l[28] == 'ei' else True
            l[29] = False if l[29] == 'ei' else True
        except:
            raise
            #print l
            continue
        dictl = dict(zip(header,l))

        dictl['arki'] = dictl['la'] = dictl['su'] = None
        if dictl['vuorotunniste_pysyva'] in vuoro_wkdays:
                dictl['arki'], dictl['la'], dictl['su'] = vuoro_wkdays[dictl['vuorotunniste_pysyva']]
        else:
            print 'No wkd data',repr(dictl['vuorotunniste_pysyva'])
        dictl['lupasoptyyppi'], dictl['lupasopnumero'] = dictl['lupasoptunnus'].split('-')
        vuorodata[int(l[18])] = dictl
        vuorodata[int(l[24])] = dictl

print len(vuorodata)/2,'vuoroinfo loaded'
########
## shapet ##


vuoro_fields = (
('vuoro_lisa','N',4,0),
('tyyppi','C',10),
('lu_viranro_myontaa','C',4),
('lu_viranro_myontaa','C',4),
('viranomaisnimi','C',40),
('lu_viranro_valvoo','C',4),
('viranomaisnimi_1','C',40),
('lu_voim_pvm','C',19),
('lu_lop_pvm','C',19),
('lu_tod_loppvm','C',19),
('muokattu_pvm','C',19),
('lupasoptunnus','C',20),
('liikharjnro','N',5,0),
('liikharj_nimi','C',50),
('reittinro_pysyva','N',8,0),
('ajosuunta','C',5),
('reittinimi','C',200),
('linjan_tunnus','C',10),
('reitti_voimaan_pvm','C',19),
('reitti_paattyy_pvm','C',19),
('reittia_muokattu_pvm','C',19),
('vuorotunniste_pysyva','C',19),
('vuoromerk','C',10),
('lahtoaika','C',4),
('perilla','C',4),
('kausi','C',6),
('vuorotyyppi','C',5),
('vuoro_lisatunniste','N',10,0),
('vuoro_voimaan_pvm','C',19),
('vuoro_paattyy_pvm','C',19),
('vuoroa_muokattu_pvm','C',19),
('kasitelty_koontikartassa','B',1),
('siirtyy_matka_fi','B',1),
('vuoron_url_interpoloitu','C',140),
('lupasoptyyppi','C',10),
('lupasopnumero','N',8,0),
('arkipaiva','B',1),
('lauantai','B',1),
('sunnuntai','B',1)

)


fname_trans = {
 'lupasoptyyppi':'lupsoptyyp',
 'lupasopnumero':'lupsopnum',
 'arkipaiva':'arkipaiva',
 'lauantai':'lauantai',
 'sunnuntai':'sunnuntai',
 'ajosuunta': 'ajosuunta',
 'kasitelty_koontikartassa': 'koontikart',
 'kausi': 'kausi',
 'lahtoaika': 'lahto',
 'liikharj_nimi': 'liikhar',
 'liikharjnro': 'liikharnro',
 'linjan_tunnus': 'otsatunnus',
 'lu_lop_pvm': 'lu_lop_pvm',
 'lu_tod_loppvm': 'lu_tod_lop',
 'lu_viranro_myontaa': 'vira_myon',
 'lu_viranro_valvoo': 'vira_valv',
 'lu_voim_pvm': 'lu_voim_pv',
 'lupasoptunnus': 'lupasoptun',
 'muokattu_pvm': 'lupsopmuok',
 'perilla': 'perilla',
 'reitti_paattyy_pvm': 'reittipaat',
 'reitti_voimaan_pvm': 'reittivoim',
 'reittia_muokattu_pvm': 'reittimuok',
 'reittinimi': 'reittinimi',
 'reittinro_pysyva': 'reittinrop',
 'siirtyy_matka_fi': 'siir_matka',
 'tyyppi': 'tyyppi',
 'viranomaisnimi': 'myon_viran',
 'viranomaisnimi_1': 'valv_viran',
 'vuoro_lisatunniste': 'vuoro_lisa',
 'vuoro_lisa': 'vuorolisa2',
 'vuoro_paattyy_pvm': 'vuoro_paat',
 'vuoro_voimaan_pvm': 'vuoro_voim',
 'vuoroa_muokattu_pvm': 'vuoro_muo',
 'vuoromerk': 'vuoromerk',
 'vuoron_url_interpoloitu': 'katsel_url',
 'vuorotunniste_pysyva': 'vuoro_pys',
 'vuorotyyppi': 'vuorotyyp',
 'arki':'arkipaiva',
 'la':'lauantai',
 'su':'sunnuntai'
}
from osgeo import ogr
from osgeo import osr



SIMP_TOL = 2.0

drv = ogr.GetDriverByName('ESRI Shapefile')

elyds = drv.Open('/vsizip/%s' % (os.path.join(sys.path[0],'tvv_kokonaan.zip')),0)

elylyr = elyds.GetLayer()


lrds = drv.Open('/vsizip/%s'  % (os.path.join(TMP_PATH,'linjaukset.zip')),0)


rlyr = lrds.GetLayer()


rdef = rlyr.GetLayerDefn()
rfielddef = []
for i in xrange(rdef.GetFieldCount()):
    fd = rdef.GetFieldDefn(i)
    rfielddef.append((fd.GetName(),fd.GetType()))

lcount = float(rlyr.GetFeatureCount())

for filter_feat in list(elylyr)+[None,]:

    if filter_feat:
        filter_area_name = filter_feat.GetFieldAsString(0).replace('\xe4','a').lower()
        filter_area_name = re.sub(r'[^\w]','_',filter_area_name)
        rlyr.SetSpatialFilter(filter_feat.GetGeometryRef())
    else:
        filter_area_name = 'kaikki'
        rlyr.SetSpatialFilter(None)

    print 'Processing',filter_area_name
    odspath = os.path.join(TMP_PATH,'linjaukset_%s.shp' % (filter_area_name,))
    if os.path.exists(os.path.join(TMP_PATH,'linjaukset_%s.zip' % (filter_area_name,))):
        drv.DeleteDataSource(odspath)



    lods = drv.CreateDataSource(odspath)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(3067)
    olyr = lods.CreateLayer('linjaukset',srs,rlyr.GetGeomType())


    for i,fd in enumerate(vuoro_fields):
        fn = fname_trans[fd[0]]
        if fd[1] == 'C':
            f = ogr.FieldDefn(fn,ogr.OFTString)
            f.SetWidth(fd[2])
        elif fd[1] == 'N':
            if len(fd) == 4:
                f = ogr.FieldDefn(fn,ogr.OFTInteger if fd[3] == 0 else ogr.OFTReal)
            else:
                f = ogr.FieldDefn(fn,ogr.OFTInteger)
        elif fd[1] == 'B':
                f = ogr.FieldDefn(fn,ogr.OFTInteger)
                f.SetWidth(1)
                f.SetPrecision(0)

        olyr.CreateField(f)
        #fname_trans[fd[0]]= olyr.GetLayerDefn().GetFieldDefn(i).GetName()


    #pprint.pprint(fname_trans)

    z = 0
    st = stt = time.time()
    for feat in rlyr:

        if z % 100 == 0 and z != 0:
            took = time.time()-stt

            totalt = took/(z/lcount)
            print '%d' % lcount,z,'%.2f' % (z/lcount),'%.2f' % took,'%.2f' % (time.time()-st),'%.2f' % totalt,'%.2f' % (totalt-took)
            st = time.time()
        z+=1

        attr = {}
        for i,fd in enumerate(rfielddef):
            if fd[1] == ogr.OFTInteger:
                attr[fd[0]] = eat.GetFieldAsInteger(i)
            elif fd[1] == ogr.OFTReal:
                attr[fd[0]] = feat.GetFieldAsDouble(i)
            elif fd[1] == ogr.OFTString:
                attr[fd[0]] = feat.GetFieldAsString(i)
            else:
                attr[fd[0]] = feat.GetFieldAsString(i)


        attr['VUORO_LISA'] = int(attr['VUORO_LISA'])


        if not attr['VUORO_LISA'] in vuorodata:
            print attr['VUORO_LISA'],'missing from vuorodata'
            continue

        nfeat = ogr.Feature(olyr.GetLayerDefn())

        for k in attr:
            #if k == 'VUORO_LISA':
            #    continue
            nfeat.SetField(k.lower(),attr[k])

        vd = vuorodata[attr['VUORO_LISA']]

        vd['kasitelty_koontikartassa'] = 1 if vd['kasitelty_koontikartassa'] else 0
        vd['siirtyy_matka_fi'] = 1 if vd['siirtyy_matka_fi'] else 0
        for k in vd:
            v = vd[k]
            if hasattr(v,'encode'):
                v = v.encode('utf-8')
            #print k,repr(v)
            k=fname_trans[k]
            nfeat.SetField(k,v)

        nfeat.SetGeometry(feat.GetGeometryRef().SimplifyPreserveTopology(SIMP_TOL))

        olyr.CreateFeature(nfeat)
        nfeat.Destroy()
        feat.Destroy()

    lods.Destroy()

    zf = zipfile.ZipFile(os.path.join(sys.path[0],'tvv_alueet','linjaukset_%s.zip' % (filter_area_name,)),'w')
    print 'Creating zip'
    for fn in os.listdir(TMP_PATH):
        if fn.endswith('.zip'):
            continue
        if not fn.startswith('linjaukset_%s'  % (filter_area_name,)):
            continue
        print fn
        st = time.time()
        zf.write(os.path.join(TMP_PATH,fn),fn)
        os.unlink(os.path.join(TMP_PATH,fn))
        print 'done',time.time()-st

    zf.close()

'''
if not os.path.exists(os.path.join(TMP_PATH,'linjaukset')):
    lzf = zipfile.ZipFile(os.path.join(TMP_PATH,'linjaukset.zip'))
    lzf.extractall(os.path.join(TMP_PATH,'linjaukset'))
r = shapefile.Reader(os.path.join(TMP_PATH,'linjaukset','linjaukset.shp'))

lshpo = shapefile.Writer()
lshpo.autoBalance = 1

lshpo.field('vuoro_lisa','N',10,0)
lshpo.field('vuoro_tyyppi','C',10)
for vf in vuoro_fields:
    lshpo.field(*vf)

for f in r.iterShapeRecords():

    record,shape = f.record,f.shape
    lshpo.shapeType = shape.shapeType
    if not record[0] in vuorodata:
        print record[0],'missing from vuorodata'
        continue
    fields = record + vuorodata[record[0]]
    for i,f in enumerate(fields):
        if hasattr(f,'encode'):
            fields[i] = fields[i].encode('utf-8')

    #pprint.pprint(fields)
    lshpo.line(parts=[shape.points,])
    lshpo.record(*fields)
    #break

    #break

lshpo.save('test')
'''
#######
print 'done'
