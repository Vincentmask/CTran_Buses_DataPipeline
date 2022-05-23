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
    consumer_conf['group.id'] = 'project3'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)

        # Subscribe to topic
    consumer.subscribe([topic])

    
    now = datetime.now()


    d = now.strftime("%m-%d-%y")
    
    fname = d + 'stop_out.json'

    # Process messages
    total_count = 0
    out = []
    done = 0;
    #previous vehicle to check meters increasing
    p_vcl = {}
    try:
        while True:
            msg = consumer.poll(1.0)
            #once the data is all consumed we start transformation
            if done == 1:

                obj = out
                print("total records for today: ", total_count)
                #transformation done, dump back to out.json
                with open(fname, mode = 'w', encoding = 'utf-8') as f:
                    json.dump(obj, f, indent = 4)

                #Clear data for the next day
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

                entry = {}
                entry = value
                # Transform direction to Out or Back for the use of data base
                if(str(entry['direction']) == ' 1'):
                    entry['direction'] = 'Out'   
                # 1 means out and all other will be backbound
                else:
                    entry['direction'] = 'Back'
                out.append(entry)

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
