#!/usr/bin/env Python
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
Performs a basic lawyer disambiguation
NOTE: this file is currently broken
"""
from collections import defaultdict, deque

import string

import regex as re

import hashlib as md5

import pickle

#import lib.alchemy

from collections import Counter
from Levenshtein import jaro_winkler

from lib.handlers.xml_util import normalize_utf8
from datetime import datetime
from lib.alchemy.schema import Lawyer, RawLawyer, patentlawyer
from lib.alchemy import get_config, get_dbtype, fetch_session
from sqlalchemy.sql import or_
from sqlalchemy.sql.expression import bindparam
from unidecode import unidecode
from lib.tasks import bulk_commit_inserts, bulk_commit_updates
import sys

from lib.alchemy import is_mysql
config = get_config()

THRESHOLD = config.get("lawyer").get("threshold")

# bookkeeping for blocks
blocks = defaultdict(list)
id_map = defaultdict(list)

nodigits = re.compile(r'[^\d]+')

lawyer_dict = {}

def get_lawyer_id(obj):
    """
    Returns string representing an lawyer object. Returns obj.organization if
    it exists, else returns concatenated obj.name_first + '|' + obj.name_last
    """
    if obj.organization:
        return obj.organization
    try:
        return obj.name_first + '|' + obj.name_last
    except:
        return ''

def clean_lawyers(list_of_lawyers):
    """
    Removes the following stop words from each lawyer:
    the, of, and, a, an, at
    Then, blocks the lawyer with other lawyers that start
    with the same letter. Returns a list of these blocks
    """
    stoplist = ['the', 'of', 'and', 'a', 'an', 'at']
    #alpha_blocks = defaultdict(list)
    block = []
    print ('Removing stop words, blocking by first letter...')
    for lawyer in list_of_lawyers:
        lawyer_dict[lawyer.uuid] = lawyer
        a_id = get_lawyer_id(lawyer)
        # removes stop words, then rejoins the string
        a_id = ' '.join(filter(lambda x:
                            x.lower() not in stoplist,
                            a_id.split(' ')))
        a_id = ''.join(nodigits.findall(a_id)).strip()
        id_map[a_id].append(lawyer.uuid)
        block.append(a_id)
    print ('lawyers cleaned!')
    return block


def without_digits(word):
    return ''.join([x for x in word if not x.isdigit()])

def create_jw_blocks(list_of_lawyers):
    """
    Receives list of blocks, where a block is a list of lawyers
    that all begin with the same letter. Within each block, does
    a pairwise jaro winkler comparison to block lawyers together
    """
    global blocks
    consumed = defaultdict(int)
    print ('Doing pairwise Jaro-Winkler...', len(list_of_lawyers))
    for i, primary in enumerate(list_of_lawyers):
        if consumed[primary]: continue
        consumed[primary] = 1
        blocks[primary].append(primary)
        for secondary in list_of_lawyers[i:]:
            if consumed[secondary]: continue
            if primary == secondary:
                blocks[primary].append(secondary)
                continue
            if jaro_winkler(primary, secondary, 0.0) >= THRESHOLD:
                consumed[secondary] = 1
                blocks[primary].append(secondary)
    pickle.dump(blocks, open('lawyer.pickle', 'wb'))
    print ('lawyer blocks created!')


lawyer_insert_statements = []
patentlawyer_insert_statements = []
update_statements = []
def create_lawyer_table(session):
    """
    Given a list of lawyers and the redis key-value disambiguation,
    populates the lawyer table in the database
    """
    print ('Disambiguating lawyers...')
    if is_mysql():
        session.execute('set foreign_key_checks = 0;')
        session.commit()
    i = 0
    for lawyer in blocks.keys():
        ra_ids = (id_map[ra] for ra in blocks[lawyer])
        for block in ra_ids:
            i += 1
            rawlawyers = [lawyer_dict[ra_id] for ra_id in block]
            if i % 20000 == 0:
                print (i, datetime.now())
                lawyer_match(rawlawyers, session, commit=True)
            else:
                lawyer_match(rawlawyers, session, commit=False)
    bulk_commit_inserts(lawyer_insert_statements, Lawyer.__table__, session, get_dbtype(), 20000)
    bulk_commit_inserts(patentlawyer_insert_statements, patentlawyer, session, get_dbtype(), 20000)
    bulk_commit_updates('lawyer_id', update_statements, RawLawyer.__table__, session, get_dbtype(), 20000)
                
                
#     t1 = bulk_commit_inserts(lawyer_insert_statements, Lawyer.__tablename__, is_sql(), 20000)
#     t2 = bulk_commit_inserts(patentlawyer_insert_statements, patentlawyer, is_sql(), 20000)
#     t3 = bulk_commit_updates('lawyer_id', update_statements, RawLawyer.__tablename__, is_sql(), 20000)
#     t1.get()
#     t2.get()
#     t3.get()
    session.commit()
    print (i, datetime.now())

def lawyer_match(objects, session, commit=False):
    freq = defaultdict(Counter)
    _param = {}
    raw_objects = []
    _clean_objects = []
    _clean_cnt = 0
    _clean_main = None
    _class_type = None
    for obj in objects:
        if not obj: continue
        _class_type = obj.__related__
        raw_objects.append(obj)
        break

    param = {}
    for obj in raw_objects:
        for k, v in obj.summarize.items():
            freq[k][v] += 1
        if "id" not in param:
            param["id"] = obj.uuid
        param["id"] = min(param["id"], obj.uuid)

    # create parameters based on most frequent
    for k in freq:
        if None in freq[k]:
            freq[k].pop(None)
        if "" in freq[k]:
            freq[k].pop("")
        if freq[k]:
            param[k] = freq[k].most_common(1)[0][0]
    if 'organization' not in param:
        param['organization'] = ''
    if 'type' not in param:
        param['type'] = ''
    if 'name_last' not in param:
        param['name_last'] = ''
    if 'name_first' not in param:
        param['name_first'] = ''
    if 'residence' not in param:
        param['residence'] = ''
    if 'nationality' not in param:
        param['nationality'] = ''

    if param["organization"]:
        param["id"] = md5.md5(param["organization"].encode('utf-8')).hexdigest()
    if param["name_last"]:
        param["id"] = md5.md5(param["name_last"].encode('utf-8')+param["name_first"].encode('utf-8')).hexdigest()
    
    lawyer_insert_statements.append(param)
    tmpids = map(lambda x: x.uuid, objects)
    patents = map(lambda x: x.patent_id, objects)
    patentlawyer_insert_statements.extend([{'patent_id': x, 'lawyer_id': param['id']} for x in patents])
    update_statements.extend([{'pk':x,'update':param['id']} for x in tmpids])

def examine():
    lawyers = session.query(patentlawyer).all()
    for a in lawyers:
        print (get_lawyer_id(a), len(a.rawlawyers))
        for ra in a.rawlawyers:
            if get_lawyer_id(ra) != get_lawyer_id(a):
                print (get_lawyer_id(ra))
            print ('-'*10)
    print (len(lawyers))


def printall():
    lawyers = session.query(patentlawyer).all()
    with open('out.txt', 'wb') as f:
        for a in lawyers:
            f.write(normalize_utf8(get_lawyer_id(a)).encode('utf-8'))
            f.write('\n')
            for ra in a.rawlawyers:
                f.write(normalize_utf8(get_lawyer_id(ra)).encode('utf-8'))
                f.write('\n')
            f.write('-'*20)
            f.write('\n')


def run_letter(letter, session, doctype='grant'):
    schema = RawLawyer
    if doctype == 'application':
        schema = RawLawyer
    letter = letter.upper()
    clause1 = schema.organization.startswith(bindparam('letter',letter))
    clause2 = schema.name_first.startswith(bindparam('letter',letter))
    clauses = or_(clause1, clause2)
    lawyers = (lawyer for lawyer in session.query(schema).filter(clauses))
    block = clean_lawyers(lawyers)
    create_jw_blocks(block)
    create_lawyer_table(session)


def run_disambiguation(doctype='grant'):
    # get all lawyers in database
    global blocks
    global lawyer_insert_statements
    global patentlawyer_insert_statements
    global update_statements
    session = fetch_session(dbtype=doctype)
    if doctype == 'grant':
        lawyers = deque(session.query(RawLawyer))
    if doctype == 'application':
        lawyers = deque(session.query(RawLawyer))
    lawyer_alpha_blocks = clean_lawyers(lawyers)
    for letter in list(string.ascii_lowercase):
        print (letter, datetime.now())
        blocks = defaultdict(list)
        lawyer_insert_statements = []
        patentlawyer_insert_statements = []
        update_statements = []
        letterblock = [x for x in lawyer_alpha_blocks if x.lower().startswith(letter)]
        create_jw_blocks(letterblock)
        create_lawyer_table(session)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print ("Need doctype")
        sys.exit(0)
    elif len(sys.argv) < 3:
        doctype = sys.argv[1]
        print ('Running ' + doctype)
        run_disambiguation(doctype)
    else:
        doctype = sys.argv[1]
        letter = sys.argv[2]
        session = fetch_session(dbtype=doctype)
        print('Running ' + letter + ' ' + doctype)
        run_letter(letter, session, doctype)
