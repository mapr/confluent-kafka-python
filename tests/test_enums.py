#!/usr/bin/env python

import mapr_streams_python

def test_enums():
    """ Make sure librdkafka error enums are reachable directly from the
        KafkaError class without an instantiated object. """
    print(mapr_streams_python.KafkaError._NO_OFFSET)
    print(mapr_streams_python.KafkaError.REBALANCE_IN_PROGRESS)
