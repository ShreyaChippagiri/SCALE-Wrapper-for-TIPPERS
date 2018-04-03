import paho.mqtt.client as mqtt  #import the client
import time
import ast
import json
from pymongo import MongoClient
import requests
from datetime import datetime
import collections
import logging

client = MongoClient('mongodb://localhost:27017/')
#db = client['sensordatabase']
#collection_name = 'scale'
scale_sensortypes = [] #Global lists that hold the registered SCALE sensortypes and sensors.
scale_sensors = []


def convert(data):  #Helper function to convert unicode json into readable json
    if isinstance(data, basestring):
        return str(data)
    elif isinstance(data, collections.Mapping):
        return dict(map(convert, data.iteritems()))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(convert, data))
    else:
        return data


def populate_sensortypes():  #When the wrapper is first brought up, queries TIPPERS to load all the registered SCALE sensor types
	get_url_sensortype = 'http://sensoria.ics.uci.edu:8001/sensortype/get'
	r_sensortype = requests.get(get_url_sensortype)

	for each_sensortype in convert(r_sensortype.json()):
		if 'SCALE' in each_sensortype['description']:
			sensortype = {}
			sensortype['id'] = each_sensortype['id']
			sensortype['description'] = each_sensortype['description']
			if not any(each_scale_sensortype['description'] == each_sensortype['description'] for each_scale_sensortype in scale_sensortypes):
			#if each_sensortype['description'] not in str(scale_sensortypes):
				scale_sensortypes.append(sensortype)


def populate_sensors():  #When the wrapper is first brought up, queries TIPPERS to load all the registered SCALE sensors
	get_url_sensor = 'http://sensoria.ics.uci.edu:8001/sensor/get'
	r_sensor = requests.get(get_url_sensor)

	for each_sensor in convert(r_sensor.json()):
		if 'scale' in each_sensor['id']:
			sensor = {}
			sensor['id'] = each_sensor['id']
			sensor['Type'] = each_sensor['Type']
			if not any(each_scale_sensor['id'] == each_sensor['id'] for each_scale_sensor in scale_sensors):
			#if  each_sensor['id'] not in str(scale_sensors):
				scale_sensors.append(sensor)


def print_existing_sensortypes_and_sensors():
	for each_sensortype in scale_sensortypes:
		print each_sensortype['description']

	for each_sensor in scale_sensors:
		print each_sensor['id']


def on_connect(client1, userdata, flags, rc):
	client1.subscribe("iot-1/d/#")
	m="Connected flags"+str(flags)+"result code "+str(rc)+"client1_id  "+str(client1)
	print(m)
	logging.info(m)

def on_disconnect(client1, userdata,rc=0):
	logging.debug("DisConnected result code "+str(rc))


def on_message(client1, userdata, message):
	print('*************************')
	sensordata = {}
	incomingdata = str(message.payload.decode("utf-8"))
	deviceID = str(message.topic.decode("utf-8"))
	sensordata["sensor_id"] = deviceID
	sensordata["timestamp"] = message.timestamp
	payload_data = json.loads(ast.literal_eval(repr(incomingdata)))
	sensordata["payload"] = payload_data
	storeIncomingData(sensordata)	

def storeIncomingData(jsonMessage): #Stores each new incoming sensor data observation to TIPPERS

	#collection = db[collection_name]
	#result = collection.insert_one(jsonMessage)
	print("Sensor data received")
	logging.info("Sensor data received")


#def store_tippers(db):
	#for each_item in db.scale.find():
		#observation = convert(each_item)
		#sensortype = observation["payload"]["d"]["event"]
	sensortype_id = ''
	
	sensortype = jsonMessage["payload"]["d"]["event"]
	post_url_sensortype = 'http://sensoria.ics.uci.edu:8001/sensortype/add'
	headers = {'content-type': 'application/json'}
	

	#if "SCALE "+ sensortype + " Sensor" not in str(scale_sensortypes):
	#Registering a new sensortype if it does not exist in TIPPERS

	if not any(each_scale_sensortype['description'] == "SCALE "+ sensortype + " Sensor" for each_scale_sensortype in scale_sensortypes):
		print "Registering new SCALE sensortype: SCALE " + sensortype + " Sensor"
		logging.info("Registering new SCALE sensortype: SCALE " + sensortype + " Sensor")
		body = [{'name':"SCALE "+ sensortype + " Sensor",'description':"SCALE "+ sensortype + " Sensor",'mobility':'fixed'}]
		try:
			response = requests.post(post_url_sensortype, data=json.dumps(body), headers=headers)
		except:
			if response.status_code != 200:
				print "Sensor type register request unsuccessful"
				logging.debug("Sensor type register request unsuccessful. HTTP response status code "+str(response.status_code))

		populate_sensortypes()

	for each_sensortype in scale_sensortypes:
		if "SCALE "+ sensortype + " Sensor" == each_sensortype['description']:
			sensortype_id = each_sensortype['id']


	post_url_sensor = 'http://sensoria.ics.uci.edu:8001/sensor/add'
	sensor = 'scale_' + jsonMessage["sensor_id"][8:20] + '_' + sensortype
	

	#if sensor not in str(scale_sensors):
	#Registering a new sensor if it does not already exist in TIPPERS

	if not any(each_scale_sensor['id'] == sensor for each_scale_sensor in scale_sensors):
		print "Registering new SCALE Sensor: "+ sensor
		logging.info("Registering new SCALE Sensor: "+ sensor)
		body = [{ "id": sensor, "name": sensor, "description":"SCALE " + sensortype + " Sensor", "user_id": "185", "ip": "", "port": "","platform": "1","type_id": sensortype_id,"location":{"x":"1", "y":"2", "z":"3"},"coverage_room_ids": []}]
		try:
			response_sensor = requests.post(post_url_sensor, data=json.dumps(body), headers=headers)
		except:
			if response_sensor.status_code != 200:
				print "Sensor register request unsuccessful"
				logging.debug("Sensor register request unsuccessful. HTTP response status code " + str(response_sensor.status_code))

		sensor = {}
		sensor['id'] = each_sensor['id']
		sensor['Type'] = each_sensor['Type']
		scale_sensors.append(sensor)

	#Storing the new observation to TIPPERS

	post_url_observation = 'http://sensoria.ics.uci.edu:8001/observation/add'
	timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	#timestamp = datetime.utcfromtimestamp(observation['timestamp'].strftime('%Y-%m-%d %H:%M:%S'))
	print "Observation recorded to TIPPERS at" + timestamp
	logging.info("Observation recorded to TIPPERS at" + timestamp)
	body = [{"sensor_id": sensor,"type": sensortype_id,"timestamp": timestamp,"payload": jsonMessage["payload"]}]
	try:
		response_observation = requests.post(post_url_observation, data=json.dumps(body), headers=headers)
	except:
		if response_observation.status_code != 200:
			print "Posting observation to TIPPERS failed"
			logging.debug("Posting observation to TIPPERS failed. HTTP response status code: "+str(response_observation.status_code))

		
populate_sensortypes()
populate_sensors()
print_existing_sensortypes_and_sensors()

broker_address="iqueue.ics.uci.edu"
client1 = mqtt.Client("P1")    #create new instance
client1.on_connect= on_connect        #attach function to callback
client1.on_message=on_message        #attach function to callback
time.sleep(1)
client1.connect(broker_address)      #connect to broker

client1.loop_forever()   #loop forever

#time.sleep(10)
#client1.loop_stop()
client1.sleep(5)
client1.on_disconnect = on_disconnect
client1.disconnect()
#store_tippers(db)