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
import json
import time
from datetime import datetime, timedelta

import pytest
from fastavro._logical_readers import UUID

from confluent_kafka.schema_registry import SchemaRegistryClient, \
    Schema, Metadata, MetadataProperties
from confluent_kafka.schema_registry.avro import AvroSerializer, \
    AvroDeserializer
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer, \
    ProtobufDeserializer
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
from tests.schema_registry.data.proto.example_pb2 import Author
from .data.proto import example_pb2

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


