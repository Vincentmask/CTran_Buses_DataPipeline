import time
import psycopg2
import argparse
import re
import csv
import json
from datetime import datetime

DBname = "postgres"
DBuser = "postgres"
DBpwd = "aereal"
Table1 = 'Trip'
Table2 = 'BreadCrumb'
#get file name ready
now = datetime.now()
d = now.strftime("%m-%d-%Y") 
ifname = d + 'out.json'
fname = ifname + '.csv'

def bsrow2vals(row):
    for key in row:
        if not row[key]:
            row[key] = 0  # ENHANCE: handle the null vals
        row['EVENT_NO_TRIP'] = row['EVENT_NO_TRIP'].replace('\'','')  # TIDY: eliminate quotes within literals

    ret = f"""
       '{row["ACT_TIME"]}',          -- tstamp
       {row['GPS_LATITUDE']},        -- latitude
       {row['GPS_LONGITUDE']},       -- longitude
       {row['DIRECTION']},           -- direction
       {row['VELOCITY']},            -- speed
       {row['EVENT_NO_TRIP']}        -- trip_id
    """

    return ret

def tprow2vals(row):
    for key in row:
        if not row[key]:
            row[key] = 0  # ENHANCE: handle the null vals
        row['EVENT_NO_TRIP'] = row['EVENT_NO_TRIP'].replace('\'','')  # TIDY: eliminate quotes within literals

    ret = f"""
       {row['EVENT_NO_TRIP']},            -- trip_id
       {row['ROUTE']},                    -- rouute_id
       {row['VEHICLE_ID']},               -- vehicle_id
       '{row["DAYOFWEEK"]}',              -- service_key
       '{row["DIRE"]}'                    -- direction
    """

    return ret

def dbconnect():
    connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
            )
    #connection.autocommit = True
    return connection
def tpgetSQLcmnds(rowlist):
    cmdlist = []
    for row in rowlist:
        valstr = tprow2vals(row)
        cmd = f"INSERT INTO {Table1} VALUES ({valstr});"
        cmdlist.append(cmd)
    return cmdlist
def bsgetSQLcmnds(rowlist):
    cmdlist = []
    for row in rowlist:
        valstr = bsrow2vals(row)
        cmd = f"INSERT INTO {Table2} VALUES ({valstr});"
        cmdlist.append(cmd)
    return cmdlist
def tpreaddata(fname):
    print(f"readdata: reading from File: {fname}")
    flag = 0
    with open(fname, mode="r") as fil:
        dr = csv.DictReader(fil)
        
        rowlist = []
        for row in dr:
            if flag == 0:
                flag = int(row['EVENT_NO_TRIP'])
                rowlist.append(row)
            if flag != int(row['EVENT_NO_TRIP']):
                rowlist.append(row)
                flag = int(row['EVENT_NO_TRIP'])
    return rowlist
def bsreaddata(fname):
    print(f"readdata: reading from File: {fname}")
    flag = 0
    with open(fname, mode="r") as fil:
        dr = csv.DictReader(fil)
        
        rowlist = []
        for row in dr:
            rowlist.append(row)
    return rowlist
#convert json to csv because it's more easy to read from csv to db
def to_csv():
    #get data
    with open(ifname) as f:
        data = json.load(f)
    #set up out file
    outfile = open(fname, 'w')
    #get ready to write
    csv_writer = csv.writer(outfile)
    count = 0
    for obj in data:
        #write column name to file
        if count == 0:
            header = obj.keys()
            csv_writer.writerow(header)
            count += 1
        #write all values to file
        csv_writer.writerow(obj.values())
    outfile.close()
def load(conn, icmdlist):
    with conn.cursor() as cursor:
        print(f"Loading {len(icmdlist)} rows")
        start = time.perf_counter()
        for cmd in icmdlist:
            cursor.execute(cmd)
        cursor.execute(f"""
            COMMIT;
            """)
        elapsed = time.perf_counter() - start
        print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')
if __name__ == "__main__":
    to_csv()
    conn = dbconnect()
    tqrlis = tpreaddata(fname)
    cmdlist = tpgetSQLcmnds(tqrlis)
    load(conn, cmdlist)
    bsrils = bsreaddata(fname)
    cmdlist = bsgetSQLcmnds(bsrils)
    load(conn, cmdlist)
