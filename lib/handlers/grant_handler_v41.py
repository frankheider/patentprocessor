#!/usr/bin/env python
"""
Copyright (c) 2013 The Regents of the University of California, AMERICAN INSTITUTES FOR RESEARCH
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
"""
@author Gabe Fierro gt.fierro@berkeley.edu github.com/gtfierro
"""

"""
Uses the extended ContentHandler from xml_driver to extract the needed fields
from patent grant documents
"""

from io import StringIO
from datetime import datetime
from unidecode import unidecode
from lib.handlers.handler import PatentHandler
import regex as re
import uuid
import xml.sax
from lib.handlers import xml_util
from lib.handlers import xml_driver

claim_num_regex = re.compile(r'^\d+\. *') # removes claim number from claim text


class Patent(PatentHandler):

    def __init__(self, xml_string, is_string=False):
        xh = xml_driver.XMLHandler()
        
        try:
            parser = xml_driver.make_parser()
            parser.setContentHandler(xh)
            parser.setFeature(xml_driver.handler.feature_external_ges, False)
            l = xml.sax.xmlreader.Locator()
            xh.setDocumentLocator(l)
            if is_string:
                parser.parse(StringIO(xml_string))
            else:
                parser.parse(xml_string)
        except xml.sax.SAXParseException as msg:
            print("Exception ", msg) 
        self.attributes = ['pat','app','assignee_list','patent','inventor_list','lawyer_list',
                     'us_relation_list','us_classifications','ipcr_classifications',
                     'citation_list','claims']

        self.xml = xh.root.PATDOC

        self.country = self.xml.B190.contents_of('PDAT', upper=False)[0]
        self.patent = self.xml.B110.contents_of('PDAT')[0]
        self.kind = self.xml.B130.contents_of('PDAT')[0]
        self.date_grant = self.xml.B140.contents_of('PDAT')[0]
        self.pat_type = None
        
        self.date_app = self.xml.B220.contents_of('PDAT')[0]
        self.country_app = '' 
        #self.xml.application_reference.contents_of('country')[0]
        #TODO country app 

        self.patent_app = self.xml.B210.contents_of('PDAT')[0]

        self.code_app = self.xml.B211US.contents_of('PDAT')[0]
        
        self.clm_num = self.xml.B577.contents_of('PDAT')[0]
        self.abstract = self.xml.SDOAB.contents_of('PDAT', '', as_string=True, upper=False)
        self.invention_title = self.xml.B540.contents_of('PDAT', '', as_string=True, upper=False)

        self.pat = {
            "id": self.patent,
            "type": self.pat_type,
            "number": self.patent,
            "country": self.country,
            "date": self._fix_date(self.date_grant),
            "abstract": self.abstract,
            "title": self.invention_title,
            "kind": self.kind,
            "num_claims": self.clm_num
        }
        self.app = {
            "type": self.code_app,
            "number": self.patent_app,
            "country": self.country_app,
            "date": self._fix_date(self.date_app)
        }
        self.app["id"] = str(self.app["date"])[:4] + "/" + self.app["number"]

    def _invention_title(self):
        original = self.xml.contents_of('invention_title', upper=False)[0]
        if isinstance(original, list):
            original = ''.join(original)
        return original

    def _name_helper(self, tag_root):
        """
        Returns dictionary of firstname, lastname with prefix associated
        with lastname
        """
        firstname = tag_root.FNM.contents_of('PDAT', as_string=True, upper=False)
        lastname = tag_root.SNM.contents_of('PDAT', as_string=True, upper=False)
        return firstname, lastname

    def _name_helper_dict(self, tag_root):
        """
        Returns dictionary of firstname, lastname with prefix associated
        with lastname
        """
        firstname = tag_root.FNM.contents_of('PDAT', as_string=True, upper=False)
        lastname = tag_root.SNM.contents_of('PDAT', as_string=True, upper=False)
        return {'name_first': firstname, 'name_last': lastname}

    def _adr_helper_dict(self, tag_root):
        """
        Returns dictionary of firstname, lastname with prefix associated
        with lastname
        """
        city = tag_root.CITY.contents_of('PDAT', as_string=True, upper=False)
        state = tag_root.STATE.contents_of('PDAT', as_string=True, upper=False)
        country = tag_root.CTRY.contents_of('PDAT', as_string=True, upper=False)
        street = tag_root.STR.contents_of('PDAT', as_string=True, upper=False)
        pcode = tag_root.PCODE.contents_of('PDAT', as_string=True, upper=False)
        
        return {'city': city, 'state': state, 'country':country, 'street':street, 'pcode':pcode}


    def _fix_date(self, datestring):
        """
        Converts a number representing YY/MM to a Date
        """
        if not datestring:
            return None
        elif datestring[:4] < "1900":
            return None
        # default to first of month in absence of day
        if datestring[-4:-2] == '00':
            datestring = datestring[:-4] + '01' + datestring[-2:]
        if datestring[-2:] == '00':
            datestring = datestring[:6] + '01'
        try:
            datestring = datetime.strptime(datestring, '%Y%m%d')
            return datestring
        except Exception as inst:
            print (inst, datestring)
            return None

    @property
    def assignee_list(self):
        """
        Returns list of dictionaries:
        assignee:
          name_last
          name_first
          residence
          nationality
          type
          organization
          sequence
        location:
          id
          city
          state
          country
        """
        assignees = self.xml.B730
        if not assignees:
            return []
        res = []
        for i, assignee in enumerate(assignees):
            # add assignee data
            asg = {}
            asg.update(self._name_helper_dict(assignee))  # add firstname, lastname
            asg['organization'] = assignee.ONM.contents_of('PDAT', as_string=True, upper=False)
            asg['type'] = str(int(assignee.B732US.contents_of('PDAT', as_string=True)))
            asg['nationality'] = assignee.CTRY.nationality.contents_of('PDAT')[0]
            asg['residence'] = assignee.CTRY.nationality.contents_of('PDAT')[0]
            # add location data for assignee
            loc = {}
            
            loc.update(self._adr_helper_dict(assignee))
            
            if not loc['country']:
                loc['country'] = 'US'

            #this is created because of MySQL foreign key case sensitivities
            loc['id'] = unidecode(u"|".join([loc['city'], loc['state'], loc['country']]).lower())
            if any(asg.values()) or any(loc.values()):
                asg['sequence'] = i
                asg['uuid'] = str(uuid.uuid1())
                res.append([asg, loc])
        return res

    @property
    def citation_list(self):
        """
        Returns a list of two lists. The first list is normal citations,
        the second is other citations.
        citation:
          date
          name
          kind
          country
          category
          number
          sequence
        OR
        otherreference:
          text
          sequence
        """
        citations = self.xml.B560
        if not citations:
            return [[], []]
        regular_cits = []
        other_cits = []
        ocnt = 0
        ccnt = 0
        for citation in citations:
            data = {}
            if citation.NCIT:
                data['text'] = citation.NCIT.contents_of('PDAT', as_string=True, upper=False)
                if any(data.values()):
                    data['sequence'] = ocnt
                    data['uuid'] = str(uuid.uuid1())
                    other_cits.append(data)
                    ocnt += 1
            else:
                data['kind'] = citation.KIND.contents_of('PDAT', as_string=True, upper=False)
                data['category'] = citation.contents_of('PDAT', as_string=True, upper=False)
                data['date'] = self._fix_date(citation.DATE.contents_of('PDAT', as_string=True))
                data['country'] = citation.CTRY.contents_of('PDAT', default=[''])[0]
                doc_number = citation.DNUM.contents_of('PDAT', as_string=True)
                data['number'] = xml_util.normalize_document_identifier(doc_number)
                if any(data.values()):
                    data['sequence'] = ccnt
                    data['uuid'] = str(uuid.uuid1())
                    regular_cits.append(data)
                    ccnt += 1
        return [regular_cits, other_cits]

    @property
    def inventor_list(self):
        """
        Returns list of lists of inventor dictionary and location dictionary
        inventor:
          name_last
          name_first
          sequence
        location:
          id
          city
          state
          country
        """
        inventors = self.xml.B720
        if not inventors:
            return []
        res = []
        for i, inventor in enumerate(inventors):
            # add inventor data
            inv = {}
            inv.update(self._name_helper_dict(inventor.B721))
            # add location data for inventor
            loc = {}
            
            loc.update(self._adr_helper_dict(inventor))
            
            if not loc['country']:
                loc['country'] = 'US'
            
            #this is created because of MySQL foreign key case sensitivities
            loc['id'] = unidecode("|".join([loc['city'], loc['state'], loc['country']]).lower())
            if any(inv.values()) or any(loc.values()):
                inv['sequence'] = i
                inv['uuid'] = str(uuid.uuid1())
                res.append([inv, loc])
        return res

    @property
    def lawyer_list(self):
        """
        Returns a list of lawyer dictionary
        lawyer:
            name_last
            name_first
            organization
            country
            sequence
        """
        lawyers = self.xml.B740
        if not lawyers:
            return []
        res = []
        for _i, lawyer in enumerate(lawyers):
            law = {}
            law.update(self._name_helper_dict(lawyer))
            law['country'] = lawyer.CTRY.contents_of('PDAT', as_string=True)
            law['organization'] = lawyer.ONM.contents_of('PDAT', as_string=True, upper=False)
            if any(law.values()):
                law['uuid'] = str(uuid.uuid1())
                res.append(law)
        return res

    def _get_doc_info(self, root):
        """
        Accepts an XMLElement root as an argument. Returns list of
        [country, doc-number, kind, date] for the given root
        """
        res = {}

        res['country'] = root.CTRY.contents_of('PDAT')
        res['kind'] = root.KIND.contents_of('PDAT')
        res['date'] = root.DATE.contents_of('PDAT')
        res['number'] = xml_util.normalize_document_identifier(root.DNUM.contents_of('PDAT'))
        return res


    @property
    def us_relation_list(self):
        """
        returns list of dictionaries for us reldoc:
        usreldoc:
          doctype
          status (parent status)
          date
          number
          kind
          country
          relationship
          sequence
        """
        root = self.xml.B600
        if not root:
            return []
        root = root[0]
        res = []
        i = 0
        for reldoc in root.children:
            if reldoc._name == 'B650':
                data = {'doctype': 'related_publication'}
                data.update(self._get_doc_info(reldoc))
                data['date'] = self._fix_date(data['date'])
                if any(data.values()):
                    data['sequence'] = i
                    data['uuid'] = str(uuid.uuid1())
                    i = i + 1
                    res.append(data)
            elif reldoc._name == 'B680US':
                data = {'doctype': 'us_provisional_application'}
                data.update(self._get_doc_info(reldoc))
                data['date'] = self._fix_date(data['date'])
                if any(data.values()):
                    data['sequence'] = i
                    data['uuid'] = str(uuid.uuid1())
                    i = i + 1
                    res.append(data)
            elif reldoc._name == 'B660':                 
                data = {'doctype': 'parent_grant_document'}
                data.update(self._get_doc_info(reldoc))
                data['date'] = self._fix_date(data['date'])
                data['status'] = reldoc.PSTA.contents_of('PDAT', as_string=True)
                data['relationship'] = 'parent_grant_document'  # parent/child
                if any(data.values()):
                    data['sequence'] = i
                    data['uuid'] = str(uuid.uuid1())
                    i = i + 1
                    res.append(data)
                    
        if self.xml.B600:
            for reldoc in root.children:
                if reldoc._name == 'B860':           
                    data = {'doctype': 'parent_pct_document'}
                    data.update(self._get_doc_info(reldoc))
                    data['date'] = self._fix_date(data['date'])
                    data['relationship'] = 'parent_pct_document'  # parent/child
                    if any(data.values()):
                        data['sequence'] = i
                        data['uuid'] = str(uuid.uuid1())
                        i = i + 1
                        res.append(data)
                elif reldoc._name == 'B870':
                    data = {'doctype': 'pct_publication_document'}
                    data.update(self._get_doc_info(reldoc))
                    data['date'] = self._fix_date(data['date'])
                    data['relationship'] = 'pct_publication_document'  # parent/child
                    if any(data.values()):
                        data['sequence'] = i
                        data['uuid'] = str(uuid.uuid1())
                        i = i + 1
                        res.append(data)                                       

        return res

    @property
    def us_classifications(self):
        """
        Returns list of dictionaries representing us classification
        main:
          class
          subclass
        """
        classes = []
        i = 0
        main = self.xml.B521.contents_of('PDAT')
        data = {'class': main[0][:3].replace(' ', ''),
                'subclass': main[0][3:].replace(' ', '')}
        if any(data.values()):
            classes.append([
                {'uuid': str(uuid.uuid1()), 'sequence': i},
                {'id': data['class'].upper()},
                {'id': "{class}/{subclass}".format(**data).upper()}])
            i = i + 1
        if self.xml.classification_national.further_classification:
            further = self.xml.B522.contents_of('PDAT')
            for classification in further:
                data = {'class': classification[:3].replace(' ', ''),
                        'subclass': classification[3:].replace(' ', '')}
                if any(data.values()):
                    classes.append([
                        {'uuid': str(uuid.uuid1()), 'sequence': i},
                        {'id': data['class'].upper()},
                        {'id': "{class}/{subclass}".format(**data).upper()}])
                    i = i + 1
        return classes

    @property
    def ipcr_classifications(self):
        """
        Returns list of dictionaries representing ipcr classifications
        ipcr:
          ipc_version_indicator
          classification_level
          section
          class
          subclass
          main_group
          subgroup
          symbol_position
          classification_value
          action_date
          classification_status
          classification_data_source
          sequence
        """
        ipcr_classifications = self.xml.B580
        if not ipcr_classifications:
            return []
        res = []
        # we can safely use [0] because there is only one ipcr_classifications tag
        for i, ipcr in enumerate(ipcr_classifications):
            if ipcr.B581:
                data = {}
                code = ipcr.B581.contents_of('PDAT')[0]
                data['classification_level'] =''
                data['section'] = code[0]
                data['class'] = code[1:2]
                data['subclass'] = code[3]
                data['main_group'] =code[5:6] 
                data['subgroup'] = code[7:8]
                if any(data.values()):
                    data['sequence'] = i
                    data['uuid'] = str(uuid.uuid1())
                    res.append(data)
        return res

    @property
    def claims(self):
        """
        Returns list of dictionaries representing claims
        claim:
          text
          dependent -- -1 if an independent claim, else this is the number
                       of the claim this one is dependent on
          sequence
        """
        claims = self.xml.SDOCL.CL[0]
        res = []
        i = 0
        for claim in claims.children:
            data = {}
            text = ''
            for txt in claim.children:
                text += txt.contents_of('PDAT', as_string=True, upper=False)
            # remove leading claim num from text
            data['text'] = claim_num_regex.sub('', text)
            data['sequence'] = i+1 # claims are 1-indexed
            if claim.claim_ref:
                # claim_refs are 'claim N', so we extract the N
                data['dependent'] = int(claim.contents_of('claim_ref',\
                                        as_string=True).split(' ')[-1])
            data['uuid'] = str(uuid.uuid1())
            res.append(data)
            i = i + 1
        return res
