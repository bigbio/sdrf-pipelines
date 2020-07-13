import re
import xml.etree.ElementTree as et
import pkg_resources


class PTMSite(object):

  def __init__(self, site: str, position: str) -> None:
    self._site = site
    self._position = position


class OntologyTerm(object):

  def __init__(self, accession: str, name: str) -> None:
    self._accession = accession
    self._name = name

  def get_name(self):
    return self._name

  def get_accession(self):
    return self._accession


class PostTranslationalModification(object):

  def __init__(self, ontology_term: OntologyTerm, delta_composition: str, sites, delta_mono_mass) -> None:
    self._ontology_term = ontology_term
    self._delta_composition = delta_composition
    self._site = sites
    self._delta_mono_mass = delta_mono_mass

  def get_name(self):
    return self._ontology_term.get_name()

  def get_accession(self):
    return self._ontology_term.get_accession()


class UnimodDatabase:
  """Wrapper for the Unimod database"""
  xmlns = '{http://www.unimod.org/xmlns/schema/unimod_2}'
  unimodfile = 'unimod.xml'
  hidden = True

  def __init__(self, **kwargs):
    self.unimodfile = pkg_resources.resource_filename(__name__, "unimod.xml")
    self.hidden = kwargs.get("hidden", True)
    node = et.parse(self.unimodfile)
    root = node.getroot()
    self.elements = {}
    self.residues = {}
    self.labels = {}
    self.modifications = []
    self._get_elements(node)
    self._get_modifications(node)

  def search_mods_by_keyword(self, keyword: str = None):
    found_list = self.modifications
    if keyword is not None and len(keyword) > 0:
      found_list = [x for x in self.modifications if re.search(keyword, x.to_str(), re.IGNORECASE)]
    return found_list

  def _get_elements(self, node):
    for e in node.findall('%selements/%selem' % (self.xmlns, self.xmlns)):
      ea = e.attrib
      self.elements[ea['title']] = ea
      if re.match(r'[A-Z]', ea['title'][:1]):
        self.elements["%s%s" % (int(round(float(ea['mono_mass']))), ea['title'])] = ea

  def _get_modifications(self, node):
    for e in node.findall('%smodifications/%smod' % (self.xmlns, self.xmlns)):
      ma = e.attrib
      d = e.findall("%sdelta" % self.xmlns)[0]
      for k in d.attrib.keys():
        ma['delta_%s' % k] = d.attrib[k]
      ma['sites'] = {}
      ma['spec_group'] = {}
      for r in e.findall('%sspecificity' % self.xmlns):
        if self.hidden == True or r.attrib['hidden'] == False:
          ma['sites'][r.attrib['site']] = r.attrib
          ma['sites'][r.attrib['site']]['NeutralLoss'] = []
          # add NeutralLoss
          for n in r.findall('%sNeutralLoss' % self.xmlns):
            ma['sites'][r.attrib['site']]['NeutralLoss'].append(n.attrib)
          # add to aa mods list.

          if r.attrib['site'] in self.residues:
            self.residues[r.attrib['site']].append(ma['title'])
          else:
            self.residues[r.attrib['site']] = [ma['title'], ]

          if r.attrib['spec_group'] in ma['spec_group']:
            ma['spec_group'][r.attrib['spec_group']].append(r.attrib['site'])
          else:
            ma['spec_group'][r.attrib['spec_group']] = [r.attrib['site'], ]

      ontology_accession = "UNIMOD:" + ma['record_id']
      ontology_term = OntologyTerm(ontology_accession, ma['title'])
      sites = []
      for old_site in ma['sites'].values():
        site = PTMSite(old_site['site'], old_site['position'])
        sites.append(site)
      mod = PostTranslationalModification(ontology_term, ma['delta_composition'], sites, ma['delta_mono_mass'])
      self.modifications.append(mod)

  def get_label(self, label):
    mod = self.modifications.get(label, None)
    return mod

  def get_element(self, name):
    el = self.elements.get(name, None)
    return el

  def list_labels(self, search):
    labels = []
    lre = re.compile(search)
    for k in self.modifications.keys():
      l = lre.search(k)
      if l is not None:
        labels.append(k)
    return labels

  def get_neutral_loss(self, label, site):
    mod = self.modifications.get(label, None)
    if mod is not None:
      try:
        nl = []
        for n in mod['sites'][site]['NeutralLoss']:
          if n['composition'] != '0':
            nl.append(n)
        return nl
      except:
        return []
    return []

  def get_delta_mono(self, label):
    mod = self.modifications.get(label, None)
    if mod is not None:
      try:
        val = float(mod['delta_mono_mass'])
        return val
      except:
        pass

  def get_by_accession(self, accession):
    for mod in self.modifications:
      if mod.get_accession() == accession:
        return mod
    return None
