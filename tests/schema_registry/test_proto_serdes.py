#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2024 Confluent Inc.
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
import time

import pytest

from confluent_kafka.schema_registry import SchemaRegistryClient, \
    Schema, Metadata, MetadataProperties
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer, \
    ProtobufDeserializer, _schema_to_str
from confluent_kafka.schema_registry.rules.cel.cel_executor import CelExecutor
from confluent_kafka.schema_registry.rules.cel.cel_field_executor import \
    CelFieldExecutor
from confluent_kafka.schema_registry.rules.encryption.awskms.aws_driver import \
    AwsKmsDriver
from confluent_kafka.schema_registry.rules.encryption.azurekms.azure_driver import \
    AzureKmsDriver
from confluent_kafka.schema_registry.rules.encryption.dek_registry.dek_registry_client import \
    DekRegistryClient
from confluent_kafka.schema_registry.rules.encryption.encrypt_executor import \
    FieldEncryptionExecutor, Clock
from confluent_kafka.schema_registry.rules.encryption.gcpkms.gcp_driver import \
    GcpKmsDriver
from confluent_kafka.schema_registry.rules.encryption.hcvault.hcvault_driver import \
    HcVaultKmsDriver
from confluent_kafka.schema_registry.rules.encryption.localkms.local_driver import \
    LocalKmsDriver
from confluent_kafka.schema_registry.rules.jsonata.jsonata_executor import \
    JsonataExecutor
from confluent_kafka.schema_registry.schema_registry_client import RuleSet, \
    Rule, RuleKind, RuleMode, SchemaReference, RuleParams
from confluent_kafka.schema_registry.serde import RuleConditionError
from confluent_kafka.serialization import SerializationContext, MessageField
from .data.proto import example_pb2, nested_pb2, test_pb2, dep_pb2, cycle_pb2


class FakeClock(Clock):

    def __init__(self):
        self.fixed_now = int(round(time.time() * 1000))

    def now(self) -> int:
        return self.fixed_now


CelExecutor.register()
CelFieldExecutor.register()
AwsKmsDriver.register()
AzureKmsDriver.register()
GcpKmsDriver.register()
HcVaultKmsDriver.register()
JsonataExecutor.register()
LocalKmsDriver.register()

_BASE_URL = "mock://"
_TOPIC = "topic1"
_SUBJECT = _TOPIC + "-value"


def test_proto_basic_serialization():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': True,
        'use.deprecated.format': False
    }
    obj = example_pb2.Author(
        name='Kafka',
        id=123,
        picture=b'foobar',
        works=['The Castle ', 'TheTrial']
    )
    ser = ProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser_conf = {
        'use.deprecated.format': False
    }
    deser = ProtobufDeserializer(example_pb2.Author, deser_conf, client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_proto_second_message():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': True,
        'use.deprecated.format': False
    }
    obj = example_pb2.Pizza(
        size="large",
        toppings=["cheese", "pepperoni"],
    )
    ser = ProtobufSerializer(example_pb2.Pizza, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser_conf = {
        'use.deprecated.format': False
    }
    deser = ProtobufDeserializer(example_pb2.Pizza, deser_conf, client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_proto_nested_message():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': True,
        'use.deprecated.format': False
    }
    obj = nested_pb2.NestedMessage.InnerMessage(
        id="inner",
    )
    ser = ProtobufSerializer(nested_pb2.NestedMessage.InnerMessage, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser_conf = {
        'use.deprecated.format': False
    }
    deser = ProtobufDeserializer(nested_pb2.NestedMessage.InnerMessage, deser_conf, client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_proto_reference():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': True,
        'use.deprecated.format': False
    }
    msg = test_pb2.TestMessage(
        test_string="hi",
        test_bool=True,
        test_bytes=b'foobar',
        test_double=1.23,
        test_float=3.45,
        test_fixed32=67,
        test_fixed64=89,
        test_int32=100,
        test_int64=200,
        test_sfixed32=300,
        test_sfixed64=400,
        test_sint32=500,
        test_sint64=600,
        test_uint32=700,
        test_uint64=800,
    )
    obj = dep_pb2.DependencyMessage(
        is_active=True,
        test_message=msg
    )

    ser = ProtobufSerializer(dep_pb2.DependencyMessage, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser_conf = {
        'use.deprecated.format': False
    }
    deser = ProtobufDeserializer(dep_pb2.DependencyMessage, deser_conf, client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_proto_cycle():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': True,
        'use.deprecated.format': False
    }
    inner = cycle_pb2.LinkedList(
        value=100
    )
    obj = cycle_pb2.LinkedList(
        value=200,
        next=inner
    )

    ser = ProtobufSerializer(cycle_pb2.LinkedList, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser_conf = {
        'use.deprecated.format': False
    }
    deser = ProtobufDeserializer(cycle_pb2.LinkedList, deser_conf, client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_proto_cel_condition():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': False,
        'use.latest.version': True,
        'use.deprecated.format': False
    }
    rule = Rule(
        "test-cel",
        "",
        RuleKind.CONDITION,
        RuleMode.WRITE,
        "CEL",
        None,
        None,
        "message.name == 'Kafka'",
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        _schema_to_str(example_pb2.Author.DESCRIPTOR.file),
        "PROTOBUF",
        [],
        None,
        RuleSet(None, [rule])
    ))
    obj = example_pb2.Author(
        name='Kafka',
        id=123,
        picture=b'foobar',
        works=['The Castle ', 'TheTrial']
    )
    ser = ProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser_conf = {
        'use.deprecated.format': False
    }
    deser = ProtobufDeserializer(example_pb2.Author, deser_conf, client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_proto_cel_condition_fail():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': False,
        'use.latest.version': True,
        'use.deprecated.format': False
    }
    rule = Rule(
        "test-cel",
        "",
        RuleKind.CONDITION,
        RuleMode.WRITE,
        "CEL",
        None,
        None,
        "message.name != 'Kafka'",
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        _schema_to_str(example_pb2.Author.DESCRIPTOR.file),
        "PROTOBUF",
        [],
        None,
        RuleSet(None, [rule])
    ))
    obj = example_pb2.Author(
        name='Kafka',
        id=123,
        picture=b'foobar',
        works=['The Castle ', 'TheTrial']
    )
    ser = ProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    try:
        obj_bytes = ser(obj, ser_ctx)
    except Exception as e:
        assert isinstance(e.__cause__, RuleConditionError)


def test_proto_cel_field_transform():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {
        'auto.register.schemas': False,
        'use.latest.version': True,
        'use.deprecated.format': False
    }
    rule = Rule(
        "test-cel",
        "",
        RuleKind.TRANSFORM,
        RuleMode.WRITE,
        "CEL_FIELD",
        None,
        None,
        "name == 'name' ; value + '-suffix'",
        None,
        None,
        False
    )
    client.register_schema(_SUBJECT, Schema(
        _schema_to_str(example_pb2.Author.DESCRIPTOR.file),
        "PROTOBUF",
        [],
        None,
        RuleSet(None, [rule])
    ))
    obj = example_pb2.Author(
        name='Kafka',
        id=123,
        picture=b'foobar',
        works=['The Castle ', 'TheTrial']
    )
    ser = ProtobufSerializer(example_pb2.Author, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    obj2 = example_pb2.Author(
        name='Kafka-suffix',
        id=123,
        picture=b'foobar',
        works=['The Castle ', 'TheTrial']
    )
    deser_conf = {
        'use.deprecated.format': False
    }
    deser = ProtobufDeserializer(example_pb2.Author, deser_conf, client)
    newobj = deser(obj_bytes, ser_ctx)
    assert obj2 == newobj


