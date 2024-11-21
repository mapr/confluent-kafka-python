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
import base64
import json
import time
from datetime import datetime, timedelta

import pytest
from fastavro._logical_readers import UUID

from confluent_kafka.schema_registry import SchemaRegistryClient, \
    Schema, Metadata, MetadataProperties
from confluent_kafka.schema_registry.json_schema import JSONSerializer, \
    JSONDeserializer
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



def test_json_basic_serialization():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True}
    obj = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    schema = {
        "type": "object",
        "properties": {
            "intField": { "type": "integer" },
            "doubleField": { "type": "number" },
            "stringField": {
                "type": "string",
                "confluent:tags": [ "PII" ]
            },
            "boolField": { "type": "boolean" },
            "bytesField": {
                "type": "string",
                "contentEncoding": "base64",
                "confluent:tags": [ "PII" ]
            }
        }
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = JSONDeserializer(None, schema_registry_client=client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_json_serialize_nested():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': True}
    nested = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    obj = {
        'nested': nested
    }
    schema = {
        "type" : "object",
        "properties" : {
            "otherField" : {
                "type" : "object",
                "properties" : {
                    "intField" : {
                        "type" : "integer"
                    },
                    "doubleField" : {
                        "type" : "number"
                    },
                    "stringField" : {
                        "type" : "string"
                    },
                    "boolField" : {
                        "type" : "boolean"
                    },
                    "bytesField" : {
                        "type" : "string"
                    }
                }
            }
        }
    }
    ser = JSONSerializer(json.dumps(schema), client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = JSONDeserializer(None, schema_registry_client=client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2


def test_json_serialize_references():
    conf = {'url': _BASE_URL}
    client = SchemaRegistryClient.new_client(conf)
    ser_conf = {'auto.register.schemas': False, 'use.latest.version': True}

    referenced = {
        'intField': 123,
        'doubleField': 45.67,
        'stringField': 'hi',
        'booleanField': True,
        'bytesField': base64.b64encode(b'foobar').decode('utf-8'),
    }
    obj = {
        'refField': referenced
    }
    ref_schema = {
        "type": "object",
        "properties": {
            "intField": { "type": "integer" },
            "doubleField": { "type": "number" },
            "stringField": {
                "type": "string",
                "confluent:tags": [ "PII" ]
            },
            "boolField": { "type": "boolean" },
            "bytesField": {
                "type": "string",
                "contentEncoding": "base64",
                "confluent:tags": [ "PII" ]
            }
        }
    }
    client.register_schema('ref', Schema(json.dumps(ref_schema)), 'JSON')
    schema = {
        "type": "object",
        "properties": {
            "otherField": { "$ref": "ref" }
        }
    }
    refs = [SchemaReference('ref', 'ref', 1)]
    client.register_schema(_SUBJECT, Schema(json.dumps(schema), 'JSON', refs))

    ser = JSONSerializer(None, client, conf=ser_conf)
    ser_ctx = SerializationContext(_TOPIC, MessageField.VALUE)
    obj_bytes = ser(obj, ser_ctx)

    deser = JSONDeserializer(None, schema_registry_client=client)
    obj2 = deser(obj_bytes, ser_ctx)
    assert obj == obj2
