import paho.mqtt.client as mqttClient
import time
import json
import sys
from hdbcli import dbapi
import os
import pandas as pd

def on_connect(client, userdata, flags, rc):

    if rc == 0:

        print("Connected to broker")

        global Connected                #Use global variable
        Connected = True                #Signal connection

    else:

        print("Connection failed")

timestr = time.strftime("%Y%m%d%H%M%S")
file_name = '/vol/vol_HDB/sysfiles/stream_data/gyro/gyro_256'+"_"+timestr+".csv"
open(file_name,'a+')
def Get_RotateFile(file_name):
    if int(os.path.getsize(file_name)) > 47216840 : 
             timestr = time.strftime("%Y%m%d%H%M%S")
             new_file_name = '/vol/vol_HDB/data/gyro_256'+"_"+timestr+".csv" 
    else:
            new_file_name=file_name
    return(new_file_name)

def on_message(client, userdata, message):
    global file_name
    y = json.loads(message.payload)
    v = (len(y['sec_data']))
    p = int(v)

    if int(os.path.getsize(file_name)) > 67108864 :
      timestr = time.strftime("%Y%m%d%H%M%S")
      file_name = '/vol/vol_HDB/sysfiles/stream_data/gyro/gyro_256'+"_"+timestr+".csv"

    if p >= 200:
         for i in range(len(y['sec_data'])): 
                d=(y["device_id"] + ';' + y["edge_history"][0]["name"] + ';' + y["edge_history"][0]["time"] + ';' + y["last_node"] + ';' +y["serial"] + ';' + y["MODEL"] + ';' + y["sec_data"][i]+ ';' + y["target"] + ';' + y["time"])
                with open(file_name,'a+') as f:
                        f.write(d + "\n")
Connected = False   #global variable for the state of the connection

broker_address= "34.87.36.235"  #Broker address
port = 1883                         #Broker port
user = ""                    #Connection username
password = ""            #Connection password

client = mqttClient.Client("PythonGyronewlongraw9729474")               #create new instance
client.username_pw_set(user, password=password)    #set username and password
client.on_connect= on_connect                      #attach function to callback
client.on_message= on_message                      #attach function to callback
client.connect(broker_address,port,100) #connect
client.subscribe("testmqtt/test1") #subscribe
client.loop_forever() #then keep listening forever
