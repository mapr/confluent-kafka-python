#!/usr/bin/env python
from confluent_kafka import Consumer, Producer, TopicPartition, KafkaError, KafkaException
import subprocess
import pytest
import mock

@pytest.fixture(autouse=True)
def resource_setup(request):
    subprocess.call(['bash','-c', "maprcli stream topic create -path /test_stream -topic topic1"])
    conf = {'default.topic.config':{'produce.offset.report': True, 'auto.offset.reset': 'latest'}}
    # Create producer
    p = Producer(**conf)
    p.produce('/test_stream:topic1', 'Hello Python!')
    p.flush()
    print("resource_setup")
    def resource_teardown():
        subprocess.check_call(['bash','-c', "maprcli stream topic delete -path /test_stream -topic topic1"])
        print("resource_teardown")
    request.addfinalizer(resource_teardown)


def test_consumer_subscribe():
    kc = Consumer({'group.id':'test', 'socket.timeout.ms':'100',
                   'session.timeout.ms': 1000, 'default.topic.config':{'auto.offset.reset': 'earliest'}}) # Avoid close() blocking too long
    kc.subscribe(["/test_stream:topic1"])
    msg = kc.poll()
    assert  msg.value() == "Hello Python!"
    kc.close()


def test_consumer_on_commit():
    on_commit_cb = mock.Mock()
    kc = Consumer({'group.id':'test', 'socket.timeout.ms':'100','enable.auto.commit': False,
                   'session.timeout.ms': 1000, # Avoid close() blocking too long
                   'on_commit': on_commit_cb, 'default.topic.config':{'auto.offset.reset': 'earliest'}})
    kc.subscribe(["/test_stream:topic1"])
    msg = kc.poll()
    kc.commit(msg)
    kc.close()
    assert on_commit_cb.called
