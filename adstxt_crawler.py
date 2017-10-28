#!/usr/bin/env python
# encoding=utf8

##########################################################################
# Copyright 2017 Hebbian Labs, LLC
# Copyright 2017 IAB TechLab & OpenRTB Group
#
# Author: Neal Richter, neal@hebbian.io
#
# Redistribution and use in source and binary forms, with or without modification, are permitted provided
# that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the
#    following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
#    the following disclaimer in the documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
# PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
##########################################################################


##########################################################################
# See README.md file
#
# This is a reference implemenation of an ads.txt crawler that downloads, parses and dumps the data to a SQLiteDB
# The code assumes that you have SQLLite installed and created the DB with the associated SQL file
#
# This code would be suitable for a small scale crawler, however it is missing many production features
# for large scale use, such as parallel HTTP download and parsing of the data files, stateful recovery
# of target servers being down, usage of a real production DB server etc.  Use as a reference for your own
# implementations or harden and enhance this code as needed.
#
##########################################################################

import sys
import csv
import pycurl
import socket
import sqlite3
import threading
import Queue
import logging
from optparse import OptionParser
from urlparse import urlparse
from string import join
from string import split

reload(sys)
sys.setdefaultencoding('utf8')

#################################################################
# FUNCTION process_row_to_db.
#  handle one row and push to the DB
#
#################################################################


def process_row_to_db(conn, data_row, crawl):
  insert_stmt = "INSERT OR REPLACE INTO adstxt (SITE_DOMAIN, EXCHANGE_DOMAIN, SELLER_ACCOUNT_ID, ACCOUNT_TYPE, TAG_ID) VALUES (?, ?, ?, ?, ?);"
  exchange_host = ''
  seller_account_id = ''
  account_type = ''
  tag_id = ''

  if len(data_row) >= 3:
    exchange_host = data_row[0].strip()
    seller_account_id = data_row[1].strip()
    account_type = data_row[2].lower().strip()

  if len(data_row) == 4:
    tag_id = data_row[3].strip()

  if "#" in account_type or tag_id.startswith('#'):
    tag_id = ''
  if "#" in tag_id:
    tag_id = tag_id.rsplit('#', 1)[0]

  tag_id = tag_id.strip()

  if "direct" in account_type:
    account_type = 'DIRECT'
  elif "reseller" in account_type:
    account_type = 'RESELLER'

  # data validation heurstics
  data_valid = 1

  # Minimum length of a domain name is 1 character, not including extensions.
  # Domain Name Rules - Nic AG
  # www.nic.ag/rules.htm
  if(len(crawl.hostname) < 3):
    data_valid = 0

  if(len(exchange_host) < 3):
    data_valid = 0

  # could be single digit integers
  if(len(seller_account_id) < 1):
    data_valid = 0

  # ads.txt supports 'DIRECT' and 'RESELLER'
  if(len(account_type) < 6):
    data_valid = 0

  if(data_valid > 0):
    logging.debug("%s | %s | %s | %s | %s" % (
        crawl.hostname, exchange_host, seller_account_id, account_type, tag_id))

    # Insert a row of data using bind variables (protect against sql injection)
    c = conn.cursor()
    try:
      c.execute(insert_stmt, (crawl.hostname, exchange_host,
                              seller_account_id, account_type, tag_id))
      # Save (commit) the changes
      conn.commit()
    except sqlite3.Error as e:
      print "An error occurred:", e.args[0]

    return 1

  return 0

# end process_row_to_db  #####

#################################################################
# FUNCTION crawl_to_db.
#  crawl the URLs, parse the data, validate and dump to a DB
#
#################################################################


def crawl_to_db(conn, crawl_url_queue):

  rowcnt = 0

  myheaders = {
      'User-Agent': 'AdsTxtCrawler/1.0; + https://github.com/InteractiveAdvertisingBureau/adstxtcrawler',
      'Accept': 'text/plain',
  }

  for aurl in crawl_url_queue:
    ahost = crawl_url_queue[aurl]
    logging.info(" Crawling  %s : %s " % (aurl, ahost))
    r = requests.get(aurl, headers=myheaders)
    logging.info("  %d" % r.status_code)

    if(r.status_code == 200):
      logging.debug("-------------")
      logging.debug(r.request.headers)
      logging.debug("-------------")
      logging.debug("%s" % r.text)
      logging.debug("-------------")

      tmpfile = 'tmpads.txt'
      with open(tmpfile, 'wb') as tmp_csv_file:
        tmp_csv_file.write(r.text)
        tmp_csv_file.close()

      with open(tmpfile, 'rU') as tmp_csv_file:
        data_reader = csv.reader(
            tmp_csv_file, delimiter=',', quotechar='|', strict=False)

        for row in data_reader:
          if len(row) == 0 or row[0].startswith('#'):
            continue

          rowcnt = rowcnt + process_row_to_db(conn, row, ahost)
  return rowcnt

# end crawl_to_db  #####

#################################################################
# FUNCTION load_url_queue
#  Load the target set of URLs and reduce to an ads.txt domains queue
#
#################################################################


def load_url_queue(csvfilename, url_queue):
  cnt = 0

  with open(csvfilename, 'rb') as csvfile:
    targets_reader = csv.reader(csvfile, delimiter=',', quotechar='|')
    for row in targets_reader:

      if len(row) < 1 or row[0].startswith('#'):
        continue

      for item in row:
        host = "localhost"

        if "http:" in item or "https:" in item:
          logging.info("URL: %s" % item)
          parsed_uri = urlparse(row[0])
          host = parsed_uri.netloc
        else:
          host = item
          logging.info("HOST: %s" % item)

      ads_txt_url = 'http://{thehost}/ads.txt'.format(thehost=host)
      logging.info("  pushing %s" % ads_txt_url)
      filename = "adstxt_%03d.txt" % (len(queue.queue) + 1)
      crawl = Crawl(host, ads_txt_url, filename)
      queue.put((ads_txt_url, crawl))
      cnt = cnt + 1

  return cnt

# end load_url_queue  #####


class WorkerThread(threading.Thread):

  def __init__(self, queue):
    threading.Thread.__init__(self)
    self.queue = queue

  def run(self):
    if options.target_database and (len(options.target_database) > 1):
      try:
        conn = sqlite3.connect(options.target_database)
        conn.text_factory = str
      except sqlite3.Error as e:
        print "An error occurred:", e.args[0]
    while 1:
      try:
        url, crawl = self.queue.get_nowait()
      except Queue.Empty:
        conn.commit()
        conn.close()
        raise SystemExit
      retrieved_headers = Storage()
      retrieved_body = Storage()
      curl = pycurl.Curl()
      curl.setopt(pycurl.URL, url)
      curl.setopt(pycurl.USERAGENT, "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36")
      curl.setopt(pycurl.ENCODING, 'gzip')
      curl.setopt(pycurl.FOLLOWLOCATION, 1)
      curl.setopt(pycurl.MAXREDIRS, 5)
      curl.setopt(pycurl.CONNECTTIMEOUT, 30)
      curl.setopt(pycurl.TIMEOUT, 300)
      curl.setopt(pycurl.NOSIGNAL, 1)
      curl.setopt(pycurl.WRITEFUNCTION, retrieved_body.store)
      curl.setopt(pycurl.HEADERFUNCTION, retrieved_headers.store)
      try:
        print "Curling %s" % url
        curl.perform()
        effective_url = curl.getinfo(pycurl.EFFECTIVE_URL)
        status_code = curl.getinfo(pycurl.HTTP_CODE)
        if(status_code == 200):
          logging.debug("-------------")
          logging.debug("%s" % effective_url)
          logging.debug("-------------")
          logging.debug(retrieved_headers)
          logging.debug("-------------")
          logging.debug("%s" % retrieved_body)
          logging.debug("-------------")

          if "ads.txt" not in effective_url:
            continue;

          tmpfile = crawl.filename
          with open(tmpfile, 'wb') as tmp_csv_file:
            tmp_csv_file.write("%s" % retrieved_body)
            tmp_csv_file.close()

          if "<html" in "%s" % retrieved_body:
            continue

          with open(tmpfile, 'rU') as tmp_csv_file:
            data_reader = csv.reader(
                tmp_csv_file, delimiter=',', quotechar='|', strict=False)

            for row in data_reader:
              if len(row) == 0 or row[0].startswith('#'):
                continue

              process_row_to_db(conn, row, crawl)
      except:
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
      curl.close()
      sys.stdout.write(".")
      sys.stdout.flush()

class Storage:
  def __init__(self):
    self.contents = ''
    self.line = 0

  def store(self, buf):
    self.line = self.line + 1
    self.contents = "%s%i: %s" % (self.contents, self.line, buf)

  def __str__(self):
    return self.contents

class Crawl:
  def __init__(self, hostname, url, filename):
    self.hostname = hostname
    self.url = url
    self.filename = filename

  def host(self):
    return host

  def url(self):
    return url

  def filename(url):
    return filename


#### MAIN ####

arg_parser = OptionParser()
arg_parser.add_option("-t", "--targets", dest="target_filename",
                      help="list of domains to crawl ads.txt from", metavar="FILE")
arg_parser.add_option("-d", "--database", dest="target_database",
                      help="Database to dump crawled data into", metavar="FILE")
arg_parser.add_option("-v", "--verbose", dest="verbose", action='count',
                      help="Increase verbosity (specify multiple times for more)")

(options, args) = arg_parser.parse_args()

if len(sys.argv) == 1:
  arg_parser.print_help()
  exit(1)

log_level = logging.WARNING  # default
if options.verbose == 1:
  log_level = logging.INFO
elif options.verbose >= 2:
  log_level = logging.DEBUG
logging.basicConfig(filename='adstxt_crawler.log', level=log_level,
                    format='%(asctime)s %(filename)s:%(lineno)d:%(levelname)s  %(message)s')

crawl_url_queue = {}
cnt_urls = 0
cnt_records = 0

queue = Queue.Queue()

cnt_urls = load_url_queue(options.target_filename, queue)

threads = []
for dummy in range(20):
  t = WorkerThread(queue)
  t.start()
  threads.append(t)

for thread in threads:
  thread.join()

logging.warning("Finished.")
