# The kafka-connect-zeebe Benchmark

# OUTDATED/UNMAINTAINED: This project uses Zeebe 0.x, and Zeebe is already on version > 1.0 with some API changes. In order to leverage this project you need to upgrade all components. 

This project runs a simple benchmark on https://github.com/zeebe-io/kafka-connect-zeebe to check thorughput for different test scenarios.

The workflow sample is based https://github.com/zeebe-io/kafka-connect-zeebe/tree/master/examples/ping-pong


<a href="http://www.youtube.com/watch?feature=player_embedded&v=j019EYpxpPE" target="_blank"><img src="http://img.youtube.com/vi/j019EYpxpPE/0.jpg" alt="Walkthrough" width="240" height="180" border="10" /></a>


# Setup

## Run locally via Docker Compose

```shell
cd docker
docker-compose up
```

I personally run this on a Google Compute Engine (*n1-standard-8 (8 vCPUs, 30 GB memory)* and then connect via SSH with port forwarding:

```
ssh -L 9200:localhost:9200 -L 8080:localhost:8080 -L 9021:localhost:9021 -L 8083:localhost:8083 -i ~/gke_key bernd_ruecker_camunda_com@34.89.200.201
```

Docker compose will start:

- [Zeebe](https://zeebe.io), on port `26500` for the client, and port `9600` for monitoring.
    - To check if your Zeebe instance is ready, you can check [http://localhost:9600/ready](http://localhost:9600/ready), 
      which will return a `204` response if ready.
- [Kafka](https://kafka.apache.org/), on port `9092`.
    - [Zookeeper](https://zookeeper.apache.org/), on port `2081`.
- [Kafka Connect](https://docs.confluent.io/current/connect/index.html), on port `8083`.
- Monitoring tools
    - Operate on port `8080`.
        - Operate has an external dependency on [Elasticsearch](https://www.elastic.co/), which we'll also run on port `9200`.
    - [Confluent Control Center](https://www.confluent.io/confluent-control-center/), on port `9021`. This will be our tool to monitor the Kafka cluster, create connectors, visualize Kafka topics, etc.
    - Grafana: http://localhost:3000 (admin/admin)

This Docker Compose file is based on the examples provided by Zeebe and Confluent:

- [Zeebe Docker Compose](https://github.com/zeebe-io/zeebe-docker-compose)
- [CP Docker Images](https://github.com/zeebe-io/zeebe-docker-compose)

## Run on Kubernetes / GCP

See [kubernetes/README.md](kubernetes/README.md)

# Run the benchmark


## Deploy workflow 

```shell
zbctl --insecure deploy process.bpmn
```

## Run test script

This python script does the whole test driving (parameters saying 1 instance with payload type 1):

```
python run-test.py 1 1
```

In order to run it you need to install python and:

```
pip install requests
pip install zeebe-grpc
pip install confluent-kafka
pip install elasticsearch
pip install prometheus_client
```

The test script

* Start workflow instances
* Start Kafka Connect Source so that records are written to Kafka, and then stop it again
* Start Kafka Connect Sink so that records are correlated back to Zeebe
* Wait for all workflows to be completly finished

It will print the durations of the different tasks.


## Test cases & Payloads

* *15k instances, 100 byte message size*
* 25k instances, 100 byte message size
* 1M instances, 100 byte message size

* 500 instances, 5kb message size
* *15k instances, 5kb message size*
* 20k instances, 5kb message size

* 500 instances, 50kb message
* 2k instances, 50kb message size
