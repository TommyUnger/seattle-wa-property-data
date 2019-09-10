import urllib.request as req
import zipfile
import re
import csv
import random
import os
import subprocess
import datetime
from collections import OrderedDict

def run_command(cmd):
    result_text = subprocess.check_output(cmd, 
                                     shell=True, 
                                     stderr=subprocess.STDOUT)
    log_it(result_text)
    
def log_it(msg):
    print("%s: %s" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                      , msg))

def download_and_unzip_file(file_url):
    log_it("downloading file %s" % file_url)
    req.urlretrieve(file_url, "download.zip")
    log_it("unzipping file %s" % file_url)
    with zipfile.ZipFile('download.zip', 'r') as zip_ref:
        zip_ref.extractall(path = 'data/')
    log_it("done %s" % file_url)

def run_sql(sql):
    log_it("running sql")
    cmd = "psql -U postgres -d work -c \"%s\"" % (re.sub('[\r\n\t ]+', ' ', sql))
    run_command(cmd)
    log_it("done running sql")

def load_data_from_file(table_name, file_name, delim=','):
    cmd = "cat %s | psql -U postgres -d work -c \"SET CLIENT_ENCODING='LATIN1'; COPY %s FROM STDIN WITH CSV DELIMITER '%s' QUOTE '\\\"'\"" % (file_name, table_name, delim)
    run_command(cmd)

def load_csv_to_postgres(table_name, file_name):
    cols = []
    log_it("start load_csv_to_postgres %s" % table_name)

    with open(file_name, newline='') as csvfile:
        rdr = csv.reader(csvfile, delimiter=',', quotechar='"')
        for line in rdr:
            for col in line:
                col_name = re.sub('([a-z])([A-Z])', '\\1_\\2', col).lower()
                cols.append(col_name)
            break
    table_name = "import.%s" % table_name
    log_it("create table: %s" % table_name)
    run_sql('DROP TABLE IF EXISTS %s; CREATE TABLE %s(%s)' % (table_name, 
                                                              table_name, 
                                                              ', '.join(['\\"' + d + '\\" TEXT' for d in cols])))
    log_it("load data")
    load_data_from_file(table_name, file_name)
    log_it("trim data")
    run_sql("UPDATE %s SET %s" % (table_name, ', '.join(['\\"%s\\"=trim(\\"%s\\")' % (d, d) for d in cols])))
    log_it("empty to null")
    run_sql("UPDATE %s SET %s" % (table_name, 
                                ', '.join(['\\"%s\\"=CASE WHEN \\"%s\\" = \'\' THEN NULL ELSE \\"%s\\" END' % (d, d, d) for d in cols])))
    log_it("done load_csv_to_postgres %s" % table_name)


def run_clean_script(table_name):
    log_it("run_clean_script %s" % table_name)
    sql = open("sql/clean_%s.sql" % table_name).read()
    run_sql(sql)
    log_it("run_clean_script complete")

