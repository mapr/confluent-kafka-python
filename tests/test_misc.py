#!/usr/bin/env python

import mapr_streams_python


def test_version():
    print('Using confluent_kafka module version %s (0x%x)' % mapr_streams_python.version())
    sver, iver = mapr_streams_python.version()
    assert len(sver) > 0
    assert iver > 0

    print('Using librdkafka version %s (0x%x)' % mapr_streams_python.libversion())
    sver, iver = mapr_streams_python.libversion()
    assert len(sver) > 0
    assert iver > 0

