import requests
import json
import grpc
import time
import uuid 
import sys
from timeit import default_timer as timer
from datetime import timedelta
from zeebe_grpc import gateway_pb2, gateway_pb2_grpc
from confluent_kafka import Consumer, KafkaError
from elasticsearch import Elasticsearch

def startProcessInstance(stub, payload):
	return stub.CreateProcessInstance.future(gateway_pb2.CreateProcessInstanceRequest(
			bpmnProcessId='ping-pong',
			version=-1,
			variables=payload.replace('RANDOM', str(uuid.uuid1()))))


def startProcessInstances(numberOfInstances, payload):
	print( "## Start process instances ")
	start = timer()

	file = open('payloads/payload-'+payload+'.json', 'r')
	payload = file.read()
	with grpc.insecure_channel("localhost:26500") as channel:
		stub = gateway_pb2_grpc.GatewayStub(channel)

		createdInstances = 0
		while createdInstances < numberOfInstances:
			batch = min(numberOfInstances - createdInstances, 100)
			futures = [startProcessInstance(stub, payload) for i in range(0, batch)]
			for future in futures:
				try:
					future.result()
					createdInstances += 1
				except grpc.RpcError as e:
					if e.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
					# retry creation in next iteration as it was probably rejected by back pressure
						pass
					else:
						raise e


	print("Started process instances: " + str(timedelta(seconds=timer()-start)))

def startKafkaConnectSource():
	contents = open('source.json', 'rb').read()
	headers = {'Content-type': 'application/json'}
	response = requests.post('http://localhost:8083/connectors', data=contents, headers=headers)
	print( "## Started Kafka Connect Source with response: " + str( response ))

def deleteKafkaConnectSource():
	response = requests.delete('http://localhost:8083/connectors/ping')
	print( "## Deleted Kafka Connect Source with response: " + str( response ))

def startKafkaConnectSink():
	contents = open('sink.json', 'rb').read()
	headers = {'Content-type': 'application/json'}
	response = requests.post('http://localhost:8083/connectors', data=contents, headers=headers)
	print( "## Started Kafka Connect Sink with response: " + str( response ))

def deleteKafkaConnectSink():
	response = requests.delete('http://localhost:8083/connectors/pong')
	print( "## Deleted Kafka Connect Sink with response: " + str( response ))

def errorCallback(kafkaError):
	print (kafkaError)

def waitForRecordsToArrive(numberOfEpectedMessages):
	print( "## Start Kafka Consumer to Check for Messages" )

	start = timer()
	amount = 0
	settings = {
		'bootstrap.servers': 'localhost:9092',
		'group.id': 'mygroup',
		'client.id': 'client-1',
		'enable.auto.commit': True,
		'session.timeout.ms': 6000,
		'default.topic.config': {'auto.offset.reset': 'smallest'},
		'error_cb': errorCallback
	}
	c = Consumer(settings)
	c.subscribe(['pong'])

	try:
		topicNotEmpty = True
		while (amount<numberOfEpectedMessages or topicNotEmpty):
			msg = c.poll(0.1)
			if msg is None:
				topicNotEmpty = False
				continue
			elif not msg.error():
				amount += 1
				topicNotEmpty = True

			elif msg.error().code() == KafkaError._PARTITION_EOF:
				print('End of partition reached {0}/{1}'
					  .format(msg.topic(), msg.partition()))
			else:
				print('Error occured: {0}'.format(msg.error().str()))

	except KeyboardInterrupt:
		pass

	finally:
		c.close()
		print("Received "+ str(amount) + " records on Kafka: " + str(timedelta(seconds=timer()-start)))


def getMetricValue(metricName):
	json = requests.get("http://localhost:9090/api/v1/query?query=sum(" + metricName + ")").json()
	try:
		return int(json["data"]["result"][0]["value"][1])
	except:
		# return 0 if metric does not yet exist
		return 0

def waitForWorkflowsToBeFinished():
	print( "## Wait for workflows to be finished" )
	start = timer()
	numberOfWorkflowsRunning = 1;
	while (numberOfWorkflowsRunning > 0):
		numberOfWorkflowsRunning = getMetricValue("zeebe_executed_instances_total");
	print("Workflows finished: " + str(timedelta(seconds=timer()-start)))

def waitForJobsToBeCreated():
	print( "## Wait for some jobs in Zeebe to be created" )
	numberOfJobsPending = 0;
	while (numberOfJobsPending == 0):
		numberOfJobsPending = getMetricValue("zeebe_job_events_total");

def waitForJobsToBeFinished():
	print( "## Wait for all jobs in Zeebe to be processed" )
	start = timer()	
	numberOfJobsPending = 1;
	while (numberOfJobsPending > 0):
		numberOfJobsPending = getMetricValue("zeebe_job_events_total");
	print("Jobs Finished: " + str(timedelta(seconds=timer()-start)))




if (len(sys.argv)==3):
	number = int(sys.argv[1])
	payload = str(sys.argv[2])
else:
	number = 1
	payload = "1"

print( "####### Starting with number of instances: " + str(number) + ", payload: " + payload)
print( "####### Keep in mind that Prometheus scraping interval is 1 second, so precision of measurements is rounded up to seconds.")

# Cleanup (to make sure it is not running)
deleteKafkaConnectSource()
deleteKafkaConnectSink()

# Run test scenario
startProcessInstances(number, payload)

# skipt the waiting for the moment, even if run time get more unreliable then
#waitForJobsToBeCreated() # make sure we have the jobs also available in Prometheus - otherwise the scrape interval might lead to a situation where we pass on too quickly because no jobs are vsisible

startKafkaConnectSource()
#waitForJobsToBeFinished()
##waitForRecordsToArrive(number)
#deleteKafkaConnectSource()

startKafkaConnectSink()
#waitForWorkflowsToBeFinished()
#deleteKafkaConnectSink()
