#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2023 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import json

import pytest

from confluent_kafka.schema_registry import SchemaRegistryClient, \
    Schema
from confluent_kafka.schema_registry.avro import AvroSerializer, \
    AvroDeserializer
from confluent_kafka.schema_registry.rules.cel.cel_executor import CelExecutor
from confluent_kafka.schema_registry.rules.cel.cel_field_executor import \
    CelFieldExecutor
from confluent_kafka.schema_registry.rules.encryption.awskms.aws_driver import \
    AwsKmsDriver
from confluent_kafka.schema_registry.rules.encryption.azurekms.azure_driver import \
    AzureKmsDriver
from confluent_kafka.schema_registry.rules.encryption.encrypt_executor import \
    FieldEncryptionExecutor
from confluent_kafka.schema_registry.rules.encryption.gcpkms.gcp_driver import \
    GcpKmsDriver
from confluent_kafka.schema_registry.rules.encryption.hcvault.hcvault_driver import \
    HcVaultKmsDriver
from confluent_kafka.schema_registry.rules.encryption.localkms.local_driver import \
    LocalKmsDriver
from confluent_kafka.schema_registry.schema_registry_client import RuleSet, \
    Rule, RuleKind, RuleMode, SchemaReference
from confluent_kafka.serialization import SerializationContext, MessageField

CelExecutor.register()
CelFieldExecutor.register()
FieldEncryptionExecutor.register()
AwsKmsDriver.register()
AzureKmsDriver.register()
GcpKmsDriver.register()
HcVaultKmsDriver.register()
LocalKmsDriver.register()

_BASE_URL = "mock://"
_TOPIC = "topic1"
_SUBJECT = _TOPIC + "-value"


def test_avro_basic_serialization():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True}
    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string'},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes'},
        ]
    }
    ser = AvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    bytes = ser(obj, ser_ctx)

    deser = AvroDeserializer(client)
    obj2 = deser(bytes, ser_ctx)
    assert obj == obj2


def test_avro_serialize_nested():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True}
    nested = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    obj = {
        'nested': nested
    }
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'nested', 'type': {
                'type': 'record',
                'name': 'nested',
                'fields': [
                    {'name': 'intField', 'type': 'int'},
                    {'name': 'doubleField', 'type': 'double'},
                    {'name': 'stringField', 'type': 'string'},
                    {'name': 'booleanField', 'type': 'boolean'},
                    {'name': 'bytesField', 'type': 'bytes'},
                ]
            }},
        ]
    }
    ser = AvroSerializer(client, schema_str=json.dumps(schema), conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    bytes = ser(obj, ser_ctx)

    deser = AvroDeserializer(client)
    obj2 = deser(bytes, ser_ctx)
    assert obj == obj2


def test_avro_serialize_references():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}

    referenced = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    obj = {
        'refField': referenced
    }
    ref_schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string'},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes'},
        ]
    }
    client.register_schema('ref', Schema(json.dumps(ref_schema)))
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'refField', 'type': 'ref'},
        ]
    }
    refs = [SchemaReference('ref', 'ref', 1)]
    client.register_schema(_SUBJECT, Schema(json.dumps(schema), 'AVRO', refs))

    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    bytes = ser(obj, ser_ctx)

    deser = AvroDeserializer(client)
    obj2 = deser(bytes, ser_ctx)
    assert obj == obj2


def test_avro_schema_evolution():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}

    evolution1 = {
        "name": "SchemaEvolution",
        "type": "record",
        "fields": [
            {
                "name": "fieldToDelete",
                "type": "string"
            }
        ]
    }
    evolution2 = {
        "name": "SchemaEvolution",
        "type": "record",
        "fields": [
            {
                "name": "newOptionalField",
                "type": ["string", "null"],
                "default": "optional"
            }
        ]
    }
    obj = {
        'fieldToDelete': 'bye',
    }

    client.register_schema(_SUBJECT, Schema(json.dumps(evolution1)))

    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    bytes = ser(obj, ser_ctx)

    client.register_schema(_SUBJECT, Schema(json.dumps(evolution2)))

    client.clear_latest_caches()
    deser = AvroDeserializer(client, conf={ 'use.latest.version': True })
    obj2 = deser(bytes, ser_ctx)
    assert obj2.get('fieldToDelete') is None
    assert obj2.get('newOptionalField') == 'optional'


def test_avro_cel_condition():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}
    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': b'foobar',
    }
    schema = {
        'type': 'record',
        'name': 'test',
        'fields': [
            {'name': 'intField', 'type': 'int'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'stringField', 'type': 'string'},
            {'name': 'booleanField', 'type': 'boolean'},
            {'name': 'bytesField', 'type': 'bytes'},
        ]
    }

    rule = Rule(
        "test-cel",
        "",
        RuleKind.CONDITION,
        RuleMode.WRITE,
        "CEL",
        None,
        None,
        "message.stringField == 'hi'",
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        json.dumps(schema),
        "AVRO",
        [],
        None,
        RuleSet(None, [rule])
    ))

    ser = AvroSerializer(client, schema_str=None, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    bytes = ser(obj, ser_ctx)

    deser = AvroDeserializer(client)
    obj2 = deser(bytes, ser_ctx)
    assert obj == obj2


