#!/bin/sh

set -x

rm adstxt.db
sqlite3 adstxt.db < adstxt_crawler.sql
rm adstxt_crawler.log adstxt_*.txt
