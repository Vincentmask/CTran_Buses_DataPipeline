#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#r
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer, KafkaError
import json
import ccloud_lib
import tokenize
import random
import time
from datetime import date
from datetime import timedelta
from datetime import datetime
import pandas as pd

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'project1'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)

        # Subscribe to topic
    consumer.subscribe([topic])

    
    now = datetime.now()


    d = now.strftime("%m-%d-%Y")
    days_ago = now - timedelta(days = 573) 


    dow = days_ago.weekday()
    if(dow == 1):
        strdow = "Monday"
    elif(dow == 2):
        strdow = "Tuesday"
    elif(dow == 3):
        strdow = "Wednesday"
    elif(dow == 4):
        strdow = "Thursday"
    elif(dow == 5):
        strdow = "Friday"
    elif(dow == 6):
        strdow = "Saturday"
    else:
        strdow = "Sunday"

    fname = d + 'out.json'

    # Process messages
    fname = d + 'out.json'
    total_count = 0
    out = []
    done = 0; 
    sum_for_avg = 0; #to calculate average speed
    #previous vehicle to check meters increasing
    p_vcl = {}
    try:
        while True:
            msg = consumer.poll(1.0)
            #once the data is all consumed we start transformation
            if done == 5 and len(out) > 0:
                # Summary: There were hundreds of thousands of bus records 
                #           in a day but not millions.
                #data = pd.read_json(fname)
                data = pd.DataFrame(out)
                count_records = data.shape[0]
                print("Count of records: ", count_records)
                # Summary: Each Trip ID has bus records in the range of
                #           200 to 250.
                total_records = data.shape[0]
                temp = pd.unique(pd.Series(data['EVENT_NO_TRIP']))
                total_trips = temp.shape[0]
                print("Total records in today: ", total_records)
                print("Total trips in today: ", total_trips)
                average = total_records / total_trips
                print("Average of trip in today: ", average)

                
                obj = out

                #inter: Speed is around 15-35MPH
                average = sum_for_avg/total_count
                if average < 15:
                    print("The avg speed is too slow for this day")
                elif average > 35:
                    print("The avg speed is too fast for this day")


                #Limit: check if date is unique
                sr = data['OPD_DATE'].unique()
                if(len(sr) == 1):
                    print("Only one date in file")
                else:
                    #process to change all date back to moust occurence
                    print("More than one date in file")
                    date = data['OPD_DATE'].value_counts().idxmax()
                    print(date)
                    for entry in obj:
                        if(entry['OPD_DATE'] != date):
                            entry['OPD_DATE'] = date

                #transformation done, dump back to out.json
                with open(fname, mode = 'w', encoding = 'utf-8') as f:
                    json.dump(obj, f, indent = 4)


                #Clear data for the next day
                sum_for_avg = 0
                total_count = 0

                out = []

            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                #if there is wait then we are done for the day
                done += 1
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                done = 0
                # Check for Kafka message
                record_value = msg.value()
                value = json.loads(record_value)
                total_count+=1

                if not p_vcl:
                    p_vcl = value
                else:
                    #INTER meters go up with time
                    #if we are comparing the same vehicle
                    if p_vcl['VEHICLE_ID'] == value['VEHICLE_ID']:
                        if p_vcl['METERS'] < value['METERS'] and p_vcl['ACT_TIME'] > value['ACT_TIME']:
                            p_vcl['METERS'] = value['METERS']
                    #Existence: Each record has a vehicle_ID'
                    veh_id = value['VEHICLE_ID']
                    if veh_id == '':
                        value['VEHICLE_ID'] = p_vcl['VEHICLE_ID']
                    

                #Existence: Each record has an operation day.
                    oper_day = value['OPD_DATE']
                    if oper_day == '':
                        value['OPD_DATE'] = p_vcl['OPD_DATE']


                #ACT_TIME to tstamp
                #time of day
                if value['ACT_TIME'] != '':
                    sec = int(value['ACT_TIME'])
                    #format in HH:MM:SS
                    tod = timedelta(seconds=sec)
                    tod = str(tod) 
 
                    tstamp = value['OPD_DATE'] + ' ' + tod
                    value['ACT_TIME'] = tstamp 
                    value['ACT_TIME'] = value['ACT_TIME'].replace('1 day,','')
                

                # Intra: If there's a longitude then it has a latitude
                    longi = value['GPS_LONGITUDE']
                    lat = value['GPS_LATITUDE']
                    if longi == '':
                        value['GPS_LONGITUDE'] = p_vcl['GPS_LONGITUDE']
                    if lat == '':
                        value['GPS_LATITUDE'] = p_vcl['GPS_LATITUDE']
                    #vehicle changed
                    p_vcl = value

                #limit: direction transformation
                if(value['DIRECTION'] == '' or int(value['DIRECTION']) < 0 or int(value['DIRECTION']) > 360):
                    value['DIRECTION'] = 0
                #Limit: Velocity can't be negative
                if(value['VELOCITY'] == '' or int(value['VELOCITY']) < 0):
                    value['VELOCITY'] = 0

                #check if velocity has a value otherwisde 0
                if value['VELOCITY'] != '':
                    speed = int(value['VELOCITY'])
                else:
                    speed = 0
                #convert meters/second to MPH
                speed *= 2.23694
                value['VELOCITY'] = speed
                
                sum_for_avg += speed


                #entry is a json object
                #limit: direction transformation
                entry = {}
                #put each breadscmp into object
                entry = value
                entry.update(ROUTE = "", DAYOFWEEK = "", DIRE = "")

                entry['DAYOFWEEK'] = strdow
                entry['DIRE'] = "Out"
                #add to the out
                out.append(entry)
                
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
