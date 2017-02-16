#!/usr/bin/env python

from confluent_kafka import Producer, KafkaError, KafkaException
import pytest
import subprocess

@pytest.fixture(autouse=True)
def resource_setup(request):
    subprocess.call(['bash','-c', "maprcli stream topic create -path /stream -topic topic1"])
    print("resource_setup")
    def resource_teardown():
        subprocess.check_call(['bash','-c', "maprcli stream topic delete -path /stream -topic topic1"])
        print("resource_teardown")
    request.addfinalizer(resource_teardown)


def test_basic_api():
    try:
        p = Producer()
    except TypeError as e:
        assert str(e) == "expected configuration dict"


    def error_cb (err):
        print('error_cb', err)

    p = Producer({'socket.timeout.ms':10,
                  'error_cb': error_cb,
                  'default.topic.config': {'message.timeout.ms': 10, 'auto.offset.reset': 'earliest'}})

    p.produce('/stream:topic1')
    p.produce('/stream:topic1', value='somedata', key='a key')
    try:
        p.produce(None)
    except TypeError as e:
        assert str(e) == "argument 1 must be string, not None"


    def on_delivery(err,msg):
        print('delivery', str)

    p.produce(topic='/stream:topic1', value='testing', partition=9,
              callback=on_delivery)

    p.poll(0.001)

    p.flush()


