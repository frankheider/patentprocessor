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
Performs a basic assignee disambiguation
"""
from collections import defaultdict, Counter

import uuid
import string
import regex as re
import hashlib as md5
import lib.alchemy as alchemy
from Levenshtein import jaro_winkler
from lib.alchemy import get_config, session_generator
import lib.alchemy.schema as schema
from datetime import datetime
from lib.tasks import bulk_commit_inserts, bulk_commit_updates
import itertools
import json

config = get_config()

THRESHOLD = config.get("assignee").get("threshold")

uuid_to_object = {}
uuid_to_cleanid = {}
letter_to_cleanid = {}
uuids_by_cleanidletter = defaultdict(list)

grant_uuids = set()
app_uuids = set()
grantsessiongen = session_generator(dbtype='grant')
appsessiongen = session_generator(dbtype='application')

nodigits = re.compile(r'[a-z ]')
stoplist = ['the','of','and','a','an','at']
substitutions = json.load(open('nber_substitutions.json'))

def isgrant(obj):
    """
    returns True of obj is from Grant table, False if from App table
    """
    return hasattr(obj, 'patent')

def get_cleanid(obj):
    """
    Returns a cleaned string version of the object representation:

    if obj has an organization, uses that. If obj has first/last name,
    uses "firstname|lastname"

    Changes severything to lowercase and removes everything that isn't [a-z ]
    """
    cleanid = ''
    if obj.organization:
        cleanid = obj.organization
    else:
        try:
            cleanid = obj.name_first + obj.name_last
        except:
            cleanid = ''
    cleanid = cleanid.lower()
    cleanid = ' '.join(filter(lambda x:
                        x not in stoplist,
                        cleanid.split()))
    cleanid = ''.join(nodigits.findall(cleanid)).strip()
    for pair in substitutions:
        cleanid = cleanid.replace(pair[0], pair[1])
    return cleanid

def get_similarity(uuid1, uuid2):
    clean1 = uuid_to_cleanid[uuid1]
    clean2 = uuid_to_cleanid[uuid2]
    if clean1 == clean2:
        return 1.0
    return jaro_winkler(clean1,clean2,0.0)

def disambiguate_letter(letter):
    groups = defaultdict(list)
    bucket = uuids_by_cleanidletter[letter]
    uuidsremaining = bucket[:]
    groupkeys = []
    print (len(bucket),'raw assignees for letter:', letter)
    i = 1
    while True:
        if not uuidsremaining:
            break
        i += 1
        uuid = uuidsremaining.pop()
        if i%10000 == 0:
            print (i, datetime.now())
        matcheduuid = False
        for groupkey in groupkeys:
            if get_similarity(uuid, groupkey) >= THRESHOLD:
                groups[groupkey].append(uuid)
                matcheduuid = True
                break
        if matcheduuid: continue
        groups[uuid].append(uuid)
        groupkeys.append(uuid)
    return groups

def create_disambiguated_record_for_block(block):
    grant_assignee_inserts = []
    app_assignee_inserts = []
    patentassignee_inserts = []
    applicationassignee_inserts = []
    grant_rawassignee_updates = []
    app_rawassignee_updates = []
    ra_objs = [uuid_to_object[uuid] for uuid in block]
    # vote on the disambiguated assignee parameters
    _freq = defaultdict(Counter)
    param = {}

    for ra in ra_objs:
        for k,v in ra.summarize.items():
            if not v:
                v = ''
            param[k] = v

    if 'organization'not in param:
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
    if 'type' in param:
        if not param['type'].isdigit():
            param['type'] = ''

    # create persistent identifier
    if param["organization"]:
        param["id"] = md5.md5(param["organization"].encode('utf-8')).hexdigest()
    elif param["name_last"]:
        param["id"] = md5.md5(param["name_last"].encode('utf-8')+param["name_first"].encode('utf-8')).hexdigest()
    else:
        param["id"] = md5.md5(''.encode('utf-8')).hexdigest()


    grant_assignee_inserts.append(param)
    app_assignee_inserts.append(param)

    # inserts for patent_assignee and appliation_assignee tables
    patents = filter(lambda x: x, map(lambda x: getattr(x,'patent_id',None), ra_objs))
    patentassignee_inserts.extend({'patent_id': x, 'assignee_id': param['id']} for x in patents)
    applications = filter(lambda x: x, map(lambda x: getattr(x,'application_id',None), ra_objs))
    applicationassignee_inserts.extend([{'application_id': x, 'assignee_id': param['id']} for x in applications])

    # update statements for rawassignee tables
    for ra in ra_objs:
        if isgrant(ra):
            grant_rawassignee_updates.append({'pk': ra.uuid, 'update': param['id']})
        else:
            app_rawassignee_updates.append({'pk': ra.uuid, 'update': param['id']})
    return grant_assignee_inserts, app_assignee_inserts, patentassignee_inserts, applicationassignee_inserts, grant_rawassignee_updates, app_rawassignee_updates



def run_disambiguation():
    """
    Runs disambiguation algorithm on grant and application assignees from
    the database indicated by lib/alchemy/config
    """
    # retrieve database connections and pull in all assignees from
    # both grant and application databases
    grtsesh = grantsessiongen()
    appsesh = appsessiongen()
    
    dbs_type = alchemy.get_dbtype()
    
    print ('fetching raw assignees',datetime.now())
    rawassignees = list(grtsesh.query(schema.RawAssignee))
    rawassignees.extend(list(appsesh.query(schema.App_RawAssignee)))
    
    # clear the destination tables
    if dbs_type == "mysql" or dbs_type == "postgres":
        if dbs_type == "postgres":
            #TODO Need to recreate the constraints !!!
            #truncate is not working for pg -ALTER TABLE tablename DISABLE/ENABLE TRIGGER ALL;
            grtsesh.execute('delete from assignee; delete from patent_assignee;')
            appsesh.execute('delete from assignee; delete from application_assignee;')
        else:
            grtsesh.execute('truncate assignee; truncate patent_assignee;')
            appsesh.execute('truncate assignee; truncate application_assignee;')
    else:
        grtsesh.execute('delete from assignee; delete from patent_assignee;')
        appsesh.execute('delete from assignee; delete from patent_assignee;')
    print ('cleaning ids', datetime.now())
    # uses the get_cleanid method to remove undesirable characters and
    # normalize to case and group by first letter
    for ra in rawassignees:
        uuid_to_object[ra.uuid] = ra
        cleanid = get_cleanid(ra)
        uuid_to_cleanid[ra.uuid] = cleanid
        if not cleanid:
            continue
        firstletter = cleanid[0]
        uuids_by_cleanidletter[firstletter].append(ra.uuid)

    print ('disambiguating blocks', datetime.now())
    # disambiguates each of the letter blocks using
    # the list of assignees as a stack and only performing
    # jaro-winkler comparisons on the first item of each block
    allrecords = []
    for letter in list(string.ascii_lowercase):
        print ('disambiguating','({0})'.format(letter),datetime.now())
        lettergroup = disambiguate_letter(letter)
        print ('got',len(lettergroup),'records')
        print ('creating disambiguated records','({0})'.format(letter),datetime.now())
        allrecords.extend(lettergroup.values())
    # create the attributes for the disambiguated assignee record from the
    # raw records placed into a block in the disambiguation phase
    res = map(create_disambiguated_record_for_block, allrecords)
    mid = itertools.zip_longest(*res)
        
    grant_assignee_inserts = list(itertools.chain.from_iterable(next(mid)))                                
    app_assignee_inserts = list(itertools.chain.from_iterable(next(mid)))
    patentassignee_inserts = list(itertools.chain.from_iterable(next(mid)))
    applicationassignee_inserts = list(itertools.chain.from_iterable(next(mid)))
    grant_rawassignee_updates = list(itertools.chain.from_iterable(next(mid)))
    app_rawassignee_updates = list(itertools.chain.from_iterable(next(mid)))

    # write out the insert counts for each table into a text file
    with open('mid.txt','w') as f:
        f.write(str(len(grant_assignee_inserts))+'\n')
        f.write(str(len(app_assignee_inserts))+'\n')
        f.write(str(len(patentassignee_inserts))+'\n')
        f.write(str(len(applicationassignee_inserts))+'\n')
        f.write(str(len(grant_rawassignee_updates))+'\n')
        f.write(str(len(app_rawassignee_updates))+'\n')
    
    print ('insert disambiguated grant assignee records','({0})'.format(str(len(grant_assignee_inserts))),datetime.now())
    
    bulk_commit_inserts(grant_assignee_inserts, schema.Assignee.__table__, grtsesh, dbs_type, 20000, 'grant')

    print ('insert disambiguated App_assignee records','({0})'.format(str(len(app_assignee_inserts))),datetime.now())

    bulk_commit_inserts(app_assignee_inserts, schema.App_Assignee.__table__, appsesh, dbs_type, 20000, 'application')
    # insert patent/assignee link records
    
    print ('insert disambiguated patentassignee records','({0})'.format(str(len(patentassignee_inserts))),datetime.now())

    bulk_commit_inserts(patentassignee_inserts, schema.patentassignee, grtsesh,  dbs_type, 20000, 'grant')
    
    print ('insert disambiguated applicationassignee records','({0})'.format(str(len(applicationassignee_inserts))),datetime.now())

    bulk_commit_inserts(applicationassignee_inserts, schema.applicationassignee, appsesh,  dbs_type, 20000, 'application')
    # update rawassignees with their disambiguated record
    print ('insert disambiguated grant_rawassignee records','({0})'.format(str(len(grant_rawassignee_updates))),datetime.now())
    
    bulk_commit_updates('assignee_id', grant_rawassignee_updates, schema.RawAssignee.__table__, grtsesh,  dbs_type, 20000, 'grant')
    
    print ('insert disambiguated app_rawassignee records','({0})'.format(letter),datetime.now())

    bulk_commit_updates('assignee_id', app_rawassignee_updates, schema.App_RawAssignee.__table__, appsesh, dbs_type, 20000, 'application')

    print ('insert disambiguated complete all records','({0})'.format(str(len(app_rawassignee_updates))),datetime.now())

    
if __name__=='__main__':
    run_disambiguation()
