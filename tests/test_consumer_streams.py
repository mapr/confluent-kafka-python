#!/usr/bin/env python
from confluent_kafka import Consumer, Producer, TopicPartition, KafkaError, KafkaException
import subprocess
import pytest
import mock
import utils as u

@pytest.fixture(scope="module", autouse=True)
def create_stream(request):
    u.new_stream('/test_stream', checked=True)
    print("stream created")
    def delete_stream():
        u.delete_stream('/test_stream', checked=True)
        print("stream deleted")
    request.addfinalizer(delete_stream)
    
    
@pytest.fixture(autouse=True)
def resource_setup(request):
    u.create_topic('/test_stream', 'topic1', checked=True)
    conf = {'default.topic.config':{'produce.offset.report': True, 'auto.offset.reset': 'latest'}}
    # Create producer
    p = Producer(**conf)
    p.produce('/test_stream:topic1', 'Hello Python!')
    p.flush()
    print("resource_setup")
    def resource_teardown():
        u.delete_topic('/test_stream', 'topic1')
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


def test_consumer_fail_commited():
    kc = Consumer({'group.id':'test', 'socket.timeout.ms':'100','enable.auto.commit': False,
                   'session.timeout.ms': 1000, 'default.topic.config':{'auto.offset.reset': 'earliest'}})
    partitions = list(map(lambda p: TopicPartition("/test_stream:topic1", p), range(0,1)))
    kc.subscribe(["/test_stream:topic1"])
    msg = kc.poll()
    kc.commit(msg, async=False)
    kc.close()
    c = Consumer({'group.id':'test', 'socket.timeout.ms':'100','enable.auto.commit': False,
                  'session.timeout.ms': 1000, 'default.topic.config':{'auto.offset.reset': 'earliest'}})
    try:
        offsets_last = c.committed(partitions)
    except KafkaException as e:
        print e
        assert e.args[0].code() == KafkaError._TIMED_OUT
    c.close()


def test_consumer_commit():
    kc = Consumer({'group.id':'test', 'socket.timeout.ms':'100','enable.auto.commit': False,
                   'session.timeout.ms': 1000, 'default.topic.config':{'auto.offset.reset': 'earliest'}})
    partitions = list(map(lambda p: TopicPartition("/test_stream:topic1", p), range(0,1)))
    kc.assign(partitions)
    msg = kc.poll()
    kc.commit(msg, async=False)
    offsets_base = kc.position(partitions)
    kc.close()
    c = Consumer({'group.id':'test', 'socket.timeout.ms':'100','enable.auto.commit': False,
                  'session.timeout.ms': 1000, # Avoid close() blocking too long
                  'default.topic.config':{'auto.offset.reset': 'earliest'}})
    c.assign(partitions)
    offsets_last = c.committed(partitions)
    assert offsets_base == offsets_last
    c.close()


def test_consumer_default_stream():
    kc = Consumer({'group.id':'test', 'socket.timeout.ms':'100','enable.auto.commit': False,
                   'session.timeout.ms': 1000, 'default.topic.config':{'auto.offset.reset': 'earliest'}, 'streams.consumer.default.stream': '/test_stream'})
    kc.subscribe(['topic1'])
    msg = kc.poll()
    assert msg.value() == "Hello Python!"
    kc.close()


def test_consumer_poll_zero():
    kc = Consumer({'group.id':'test', 'socket.timeout.ms':'100',
                   'session.timeout.ms': 1000, 'default.topic.config':{'auto.offset.reset': 'earliest'}}) # Avoid close() blocking too long
    kc.subscribe(["/test_stream:topic1"])
    msg = kc.poll(0)
    assert  msg  == None
    kc.close()