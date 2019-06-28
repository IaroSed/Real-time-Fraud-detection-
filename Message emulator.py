# -*- coding: utf-8 -*-
"""
Created on Sat Jun 15 05:48:05 2019

@author: iasedric
"""

#import sys
import logging
#import datetime
import time
#import os
import pandas as pd
import numpy as np
#import json

from azure.eventhub import EventHubClient, EventData

# Importing the dataset
dataset = pd.read_csv('creditcard.csv')
X = dataset.iloc[:, 0:30]

type(X['V1'].iloc[2])

X['Hour'] = round(X['Time'].astype('float64')/3600,0) % 24

logger = logging.getLogger("azure")

# Address can be in either of these formats:
# "amqps://<URL-encoded-SAS-policy>:<URL-encoded-SAS-key>@<mynamespace>.servicebus.windows.net/myeventhub"
# "amqps://<mynamespace>.servicebus.windows.net/myeventhub"
# For example:
ADDRESS = "amqps://realtimeml.servicebus.windows.net/test"

#Endpoint=sb://realtimeml.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=R83QAByslZ/bZ7jI/AyK6EQSNvVQKX2NIL15X+Bdlp0=
# SAS policy and key are not required if they are encoded in the URL
USER =   "RootManageSharedAccessKey"
KEY = "R83QAByslZ/bZ7jI/AyK6EQSNvVQKX2NIL15X+Bdlp0="

try:
    if not ADDRESS:
        raise ValueError("No EventHubs URL supplied.")

    # Create Event Hubs client
    client = EventHubClient(ADDRESS, debug=False, username=USER, password=KEY)
    sender = client.add_sender(partition="0")
    client.run()
    try:
        start_time = time.time()
        #for i in range(100):
        i=0
        while True:
            print("Sending message: {}".format(str(i)))
            transaction =  X.iloc[i, :].to_json(orient='columns')
            #print("Sending message: {}".format(str(transaction)))
            sender.send(EventData(transaction))
            i+=1
    except:
        raise
    finally:
        end_time = time.time()
        client.stop()
        run_time = end_time - start_time
        logger.info("Runtime: {} seconds".format(run_time))

except KeyboardInterrupt:
    pass