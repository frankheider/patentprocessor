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
import os
import sys
import parse
import time

import datetime
import logging
import threading
import requests
import queue
import zipfile
import codecs

from lib.config_parser import get_config_options

from bs4 import BeautifulSoup as bs

from lib.assignee_disambiguation import run_disambiguation as ass_disambiguation  
from lib.lawyer_disambiguation import run_disambiguation as law_disambiguation
from lib.geoalchemy import run_geo
from consolidate import consolidate

sys.path.append('lib')

headers = {
'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
}


logfile = "./" + 'xml-parsing.log'
logging.basicConfig(filename=logfile, level=logging.DEBUG)

def get_year_list(yearstring):
    """
    Given a [yearstring] of forms
    year1
    year1-year2
    year1,year2,year3
    year1-year2,year3-year4
    Expands into a list of year integers, and returns
    """
    years = []
    for subset in yearstring.split(','):
        if subset == 'latest':
            years.append('latest')
            continue
        sublist = subset.split('-')
        start = int(sublist[0])
        end = int(sublist[1])+1 if len(sublist) > 1 else start+1
        years.extend(range(start,end))
    return years

def generate_download_list(year, doctype='grant'):
    """
    Given the year string from the configuration file, return
    a list of urls to be downloaded
    """
    """
    logging.basicConfig() 
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True
    """
    urls = []
     
    if doctype == 'grant':
        url = parse_config['urlgrants'] + '/' + str(year)
    if doctype == 'application':
        url = parse_config['urlapps'] + '/' + str(year)

    ext_list = ['tar', 'TAR', 'zip', 'ZIP','doc','DOC']
   
      
    print (str(year) + ' ' + doctype + ' from ' + url)

    session = requests.Session()
    session.trust_env = False
    page = session.get(url, headers=headers)
    soup = bs(page.content, "html.parser")
    for ext in ext_list:
        urls += [url + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]
    print (urls)

    return (urls)

#Downloader class - reads queue and downloads each file in succession
class Downloader(threading.Thread):
    """Threaded File Downloader"""

    def __init__(self, lqueue, output_directory):
        threading.Thread.__init__(self,name=codecs.encode(os.urandom(16), 'hex').decode())
        self.queue = lqueue
        self.output_directory = output_directory
  
    def run(self):
        while True:
            # gets the url from the queue
            url = self.queue.get()
 
            # download the file
            print ("* Thread " + self.name + " - processing URL")
            self.download_file(url)
  
            # send a signal to the queue that the job is done
            self.queue.task_done()
 
    def download_file(self, url):
        t_start = time.clock()

        session = requests.Session()
        session.trust_env = False
        r = session.get(url, stream=True)

        if (r.status_code == 200):
            fname = os.path.basename(url)
            if fname in os.listdir(self.output_directory):
                print ('already have ',fname)
                filesize = int(r.headers['Content-length'])
                filesize_is = int(os.path.getsize (self.output_directory + "/" + fname))
                if filesize == filesize_is:
                    print ('same size skipping download',filesize)
                    return
                else:
                    print ("differnd size (is: %10d expected %10d) removing and continue...." % (filesize_is, filesize))
                    os.remove(self.output_directory + "/" + fname)

            
            fname = self.output_directory + "/" + os.path.basename(url)

            filesize = int(r.headers['Content-length'])
            print ("Downloading: %s Bytes: %s" % (url, filesize))
            handle = open(fname, "wb")
            filesize_dl = 0
            progress_s = 100
            for chunk in r.iter_content(chunk_size=200*1024):
                filesize_dl += len(chunk)
            
                if chunk:  # filter out keep-alive new chunks
                    handle.write(chunk)
                    progress = filesize_dl * 100. / filesize
                    if int(progress_s) != int(progress):
                        status = r"%10d  [%3.2f%%]" % (filesize_dl, filesize_dl * 100. / filesize)
                        status = time.strftime("%H:%M:%S") + ' ' + fname + ' ' + status + chr(8)*(len(status)+1)
                        print (status)
                        progress_s = int(progress)  
            t_elapsed = time.clock() - t_start  
            print ("Done * Thread: " + self.name + " Downloaded " + url + " in " + str(filesize/t_elapsed) + " bytes/sec")                       
        else:
            print ("* Thread: " + self.name + " Bad URL: " + url)
 
# Spawns dowloader threads and manages URL downloads queue
class DownloadManager():

    def __init__(self, download_dict, output_directory, thread_count=5):
        self.thread_count = thread_count
        self.download_dict = download_dict
        self.output_directory = output_directory

    # Start the downloader threads, fill the queue with the URLs and
    # then feed the threads URLs via the queue
    def begin_downloads(self):
        q = queue.Queue ()
    
        # Create a thread pool and give them a queue
        for _i in range(self.thread_count):
            t = Downloader(q, self.output_directory)
            t.setDaemon(True)
            t.start()
 
        # Load the queue from the download dict
        for linkname in self.download_dict:
            print (linkname)
            q.put(self.download_dict[linkname])
 
        # Wait for the queue to finish
        q.join()
 
        return

def run_unzip (download_dict, output_directory):

    for zipname in download_dict:
        fn = os.path.basename(zipname)    
        z = zipfile.ZipFile(output_directory+ '/' + fn)
        print ('unzipping ',output_directory+ '/' + fn)
        z.extractall(output_directory+ '/tmp')


def run_parse(files, doctype='grant'):

    logfile = "./" + 'xml-parsing.log'
    logging.basicConfig(filename=logfile, level=logging.DEBUG)
    parse.parse_files(files, doctype)

def run_clean(process_config):
    if not process_config['clean']:
        return
    _doctype = process_config['doctype']
    command = 'bash run_clean.sh'
    os.system(command)

def run_consolidate(process_config):
    if not process_config['consolidate']:
        return
    _doctype = process_config['doctype']
    # TODO: optionally include previous disambiguation
    command = 'bash run_consolidation.sh'
    os.system(command)

if __name__=='__main__':
    s = datetime.datetime.now()
    # accepts path to configuration file as command line option
    if len(sys.argv) < 2:
        print('Please specify a configuration file as the first argument')
        exit()
    process_config, parse_config = get_config_options(sys.argv[1])
    doctype = process_config['doctype']

    # download the files to be parsed
 
    doctype_list = []
    should_process_grants = doctype in ['all', 'grant']
    should_process_applications = doctype in ['all', 'application']

    if should_process_applications:
        doctype_list.extend (['application'])
    if should_process_grants:
        doctype_list.extend (['grant'])


    print (doctype_list)

    years = parse_config['years'] 
    years = get_year_list(years)

    print (years)
    if 'latest' in years:
        years.remove('latest')

    for dt in doctype_list:
        for year in years:
            urls = generate_download_list(year, dt)
            if dt == 'grant':        
                downloaddir = parse_config['downloaddirgrants'] + '/' + str(year)
            if dt == 'application':        
                downloaddir = parse_config['downloaddirapps'] + '/' + str(year)
            print ("downloading to " + downloaddir)

            if downloaddir and not os.path.exists(downloaddir):
                os.makedirs(downloaddir)
            
            print ('Downloading files at {0}'.format(str(datetime.datetime.today())))

            download_dict = {}

            for f in urls:
                download_dict[str(f)] = f
            if len(download_dict) is 0:
                print ("* No URLs to download -> EXIT")
                sys.exit(2)

            #download_manager = DownloadManager(download_dict, downloaddir, 10)
            #download_manager.begin_downloads()
            #run_unzip(download_dict, downloaddir)
    
            f = datetime.datetime.now()
            print ('Finished downloading in {0}'.format(str(f-s)))
 
#        find files
            print ("Starting parse on {0} on directory {1}".format(str(datetime.datetime.today()),parse_config['downloaddirgrants'] + '/' + str(year)))
            if should_process_grants:
                files = parse.list_files(parse_config['downloaddirgrants'] + '/' + str(year) + '/tmp',parse_config['grantregex'])
                print ('Running grant parse...')
                run_parse(files, 'grant')
                f = datetime.datetime.now()
                print ("Found {2} files matching {0} in directory {1}"\
                         .format(parse_config['grantregex'], parse_config['downloaddirgrants'], len(files)))
            if should_process_applications:
                files = parse.list_files(parse_config['downloaddirapps']  + '/' + str(year) + '/tmp' ,parse_config['applicationregex'])
                print ('Running application parse...')
                run_parse(files, 'application')
                f = datetime.datetime.now()
                print ("Found {2} files matching {0} in directory {1}"\
                         .format(parse_config['applicationregex'], parse_config['downloaddirapps'], len(files)))
            print ('Finished parsing in {0}'.format(str(f-s)))
     
    ass_disambiguation ()
         
    for dt in doctype_list:
        law_disambiguation (dt)
    run_geo (doctype = 'grant')
    
#     #run_clean(process_config)
#     for year in years:
#         consolidate(year='2019', doctype = 'grant')
