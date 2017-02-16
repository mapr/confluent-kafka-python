#!/usr/bin/env python

from confluent_kafka import Producer, Consumer,  KafkaError, KafkaException, TopicPartition
import mock
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


def test_producer_on_delivery():
    p = Producer({'socket.timeout.ms':10,
                  'default.topic.config': {'message.timeout.ms': 10, 'auto.offset.reset': 'earliest'}})
    on_delivery = mock.Mock()
    p.produce(topic='/stream:topic1', value='testing', partition=0,
              callback=on_delivery)
    p.flush()
    assert on_delivery.called


def test_producer_partition():
    p = Producer({'socket.timeout.ms':10,
                  'default.topic.config': {'message.timeout.ms': 10, 'auto.offset.reset': 'earliest'}})
    p.produce(topic='/stream:topic3', value='testing', partition=0)
    p.poll(1)
    kc = Consumer({'group.id':'test', 'socket.timeout.ms':'100','enable.auto.commit': False,
                   'session.timeout.ms': 1000, 'default.topic.config':{'auto.offset.reset': 'earliest'}})
    kc.assign([TopicPartition("/stream:topic3", 0)])
    msg = kc.poll()
    assert  msg.value() == "testing"
    kc.close()