from mapr_streams_python import Consumer, Producer, TopicPartition, KafkaError, KafkaException
import pytest
import utils as u

@pytest.fixture(autouse=True)
def create_stream(request):
    u.new_stream('/test_stream', checked=True)
    print("stream created")

    def delete_stream():
        u.delete_stream('/test_stream', checked=True)
        print("stream deleted")

    request.addfinalizer(delete_stream)


def create_test_producer():
    def error_cb(err):
        print('error_cb', err)

    p = Producer({'socket.timeout.ms': 10,
                  'error_cb': error_cb,
                  'default.topic.config': {
                      'message.timeout.ms': 10,
                      'auto.offset.reset': 'earliest'}
                  })
    return p


def produce(p, topics, messages):
    for ((topic, partition), message) in zip(topics, messages):
        p.produce(topic, message)
    p.flush()


def consume(consumers, expected, handler=None):
    received = []
    running = True
    print consumers
    while running:
        for c in consumers:
            if len(received) == expected:
                running = False
                break
                
            msg = c.poll()
            if msg == None :
                break
            if not msg.error():
                print('Received message: %s' % msg.value().decode('utf-8'))
                received.append(msg.value().decode('utf-8'))
                if handler is not None:
                    handler(c, msg)
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                running = False
    return received


def test_consumer_reads_all_partitions():
    # Create consumer and assign it to topics with multiple partitions.
    # Check that all subscribed partitions are read by consumer

    p = create_test_producer()

    def dummy_commit_cb(err, partitions):
        pass

    c = Consumer({'group.id': 'test', 'socket.timeout.ms': '100',
                  'session.timeout.ms': 1000,  'default.topic.config': { 'auto.offset.reset': 'earliest'},
                  'on_commit': dummy_commit_cb})

    c.subscribe(["/test_stream:topic1", "/test_stream:topic2"])

    topics = [('/test_stream:topic1', 0), ('/test_stream:topic1', 1), ('/test_stream:topic1', 2),
              ('/test_stream:topic2', 0), ('/test_stream:topic2', 1), ('/test_stream:topic2', 2)]
    messages = ['topic1v0', 'topic1v1', 'topic1v2', 'topic2v0', 'topic2v1', 'topic2v2']

    produce(p, topics, messages)

    received = consume([c], len(messages), lambda c, msg: None)

    assert set(messages) == set(received)

    c.close()



def test_consumer_group_partitions():
    # Create consumer and assign it to topics with multiple partitions.
    # Check that all subscribed partitions are read by consumer

    p = create_test_producer()

    def dummy_commit_cb(err, partitions):
        pass

    c1 = Consumer({'group.id': 'test', 'socket.timeout.ms': '1000', "streams.consumer.default.stream":"/test_stream",
                   'session.timeout.ms': 100,"client.id": "c1", 'default.topic.config': { 'auto.offset.reset': 'earliest'},
                   'on_commit': dummy_commit_cb})

    c2 = Consumer({'group.id': 'test', 'socket.timeout.ms': '1000', "streams.consumer.default.stream" : "/test_stream",
                   'session.timeout.ms': 100,"client.id":"c2", 'default.topic.config': { 'auto.offset.reset': 'earliest'},
                   'on_commit': dummy_commit_cb})

    c3 = Consumer({'group.id': 'test', 'socket.timeout.ms': '1000', "streams.consumer.default.stream" : "/test_stream",
                   'session.timeout.ms': 100, "client.id":"c3",'default.topic.config': { 'auto.offset.reset': 'earliest'},
                   'on_commit': dummy_commit_cb})

    c1.subscribe(["topic1","topic2"])
    c2.subscribe(["topic1","topic2"])
    c3.subscribe(["topic1","topic2"])

    topics = [('/test_stream:topic1', 0), ('/test_stream:topic1', 1), ('/test_stream:topic1', 2),
              ('/test_stream:topic2', 0), ('/test_stream:topic2', 1), ('/test_stream:topic2', 2)]

    messages = ['topic1v0', 'topic1v1', 'topic1v2', 'topic2v0', 'topic2v1', 'topic2v2']

    produce(p, topics, messages)
    received = []
    msg_cont = 0
    while msg_cont < len(messages):
        msg1 = c1.poll(1)
        if msg1 != None:
            received.append(msg1.value())
            msg_cont = msg_cont + 1
        msg2 = c2.poll(1)
        if msg2 != None:
            msg_cont = msg_cont + 1
            received.append(msg2.value())
        msg3 = c3.poll(1)
        if msg3 != None:
            msg_cont = msg_cont + 1
            received.append(msg3.value())

    assert set(messages) == set(received)

    c1.close()
    c2.close()
    c3.close()