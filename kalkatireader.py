import sys,pprint
import re
import zipfile

try:
  from lxml import etree
  print("running with lxml.etree")
except ImportError:
  try:
    # Python 2.5
    import xml.etree.cElementTree as etree
    print("running with cElementTree on Python 2.5+")
  except ImportError:
    try:
      # Python 2.5
      import xml.etree.ElementTree as etree
      print("running with ElementTree on Python 2.5+")
    except ImportError:
      try:
        # normal cElementTree install
        import cElementTree as etree
        print("running with cElementTree")
      except ImportError:
        try:
          # normal ElementTree install
          import elementtree.ElementTree as etree
          print("running with ElementTree")
        except ImportError:
          print("Failed to import ElementTree from any known place")



from collections import defaultdict

def etree_to_dict(t):
    d = {t.tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.iteritems():
                dd[k].append(v)
        d = {t.tag: {k:v[0] if len(v) == 1 else v for k, v in dd.iteritems()}}
    if t.attrib:
        d[t.tag].update(('@' + k, v) for k, v in t.attrib.iteritems())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
              d[t.tag]['#text'] = text
        else:
            d[t.tag] = text
    return d

def parseKalkatifile(fn,callback):
    wanted_tags = (
        'station',
        'trnsattr',
        'service',
        'footnote',
        'company'
        )


    zf = None
    if (zipfile.is_zipfile(fn)):
        zf = zipfile.ZipFile(fn)
        f = zf.open('LVM.xml')
    else:
        f = open(fn,'rb')

        # get an iterable
    context = etree.iterparse(f,events=('end',))

    # turn it into an iterator
    context = iter(context)

    # get the root element
    event, root = context.next()
    for event,elem in context:
        tagname = elem.tag.lower()

        if tagname in wanted_tags:
            if callback(tagname,etree_to_dict(elem)[elem.tag]) == False:
                elem.clear()
                root.clear()
                break
            elem.clear()
            root.clear()
    del context

vuorore = re.compile(u'Liikenn\xf6itsij\xe4(.*?) Sopimustyyppi\(([\w]*?)\)  LupaSopTunnus\((.*?)\) ([\d]*?)/([\d]*?)/([\d]*?)/\[([\w]*?)\]')
def parseRealValluTrnsattr(attr):
    global vuorore
    rem = vuorore.match(attr)
    if not rem:
        return None
    info = dict(zip(('nimi','tyyppi','sopimustunnus','sopimusid','reittiid','vuoroid','laji'),rem.groups()))
    info['vuoroid'] = int(info['vuoroid'])
    info['reittiid'] = int(info['reittiid'])
    info['sopimusid'] = int(info['sopimusid'])
    return info
def parseValluTrnsattr(attr):
    if not attr.startswith('Liikenn'):
        return None
    info = {}
    try:
        info['tunnisteet'] = map(int,attr[attr.rfind(' '):].split('/'))
    except:
        return parseRealValluTrnsattr(attr)
    if len(info['tunnisteet']) != 3:
        print 'bsd'

        return parseRealValluTrnsattr(attr)

    info['sopimusid'],info['reittiid'],info['vuoroid'] = info['tunnisteet']


    asp = attr.split(' ')
    info['sopimustunnus'] = None
    nimi_last = -1
    if asp[-2].find('-') != -1 and asp[-2][asp[-2].find('-')+1:].isdigit():
        info['sopimustunnus'] = asp[-2]
        nimi_last = -2

    info['nimi'] = ' '.join(asp[1:nimi_last])

    return info


def extractServiceData(srvdata):
    global currfile
    if not isinstance(srvdata,list):
        srvdata = [srvdata,]

    vuorodata = None
    for servattr in srvdata:
        attrid = '%s%s' % (currfile,servattr['@AttributeId'])

        if not attrid in trnsattrs:
            continue

        info = kalkatireader.parseValluTrnsattr(trnsattrs[attrid])
        if info:
            return info
    return vuorodata

if __name__ == '__main__':

    def testCb(tag,data):
        if tag != 'trnsattr':
            return True

        print data

    parseKalkatifile('kalkati\\vakio_20140809\\LVM.xml',testCb)
