# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: nested.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0cnested.proto\x12\x04test\x1a\x1fgoogle/protobuf/timestamp.proto\"l\n\x06UserId\x12\x17\n\rkafka_user_id\x18\x01 \x01(\tH\x00\x12\x17\n\rother_user_id\x18\x02 \x01(\x05H\x00\x12%\n\nanother_id\x18\x03 \x01(\x0b\x32\x0f.test.MessageIdH\x00\x42\t\n\x07user_id\"\x17\n\tMessageId\x12\n\n\x02id\x18\x01 \x01(\t\"R\n\x0b\x43omplexType\x12\x10\n\x06one_id\x18\x01 \x01(\tH\x00\x12\x12\n\x08other_id\x18\x02 \x01(\x05H\x00\x12\x11\n\tis_active\x18\x03 \x01(\x08\x42\n\n\x08some_val\"\xe0\x03\n\rNestedMessage\x12\x1d\n\x07user_id\x18\x01 \x01(\x0b\x32\x0c.test.UserId\x12\x11\n\tis_active\x18\x02 \x01(\x08\x12\x1a\n\x12\x65xperiments_active\x18\x03 \x03(\t\x12.\n\nupdated_at\x18\x04 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x1c\n\x06status\x18\x05 \x01(\x0e\x32\x0c.test.Status\x12\'\n\x0c\x63omplex_type\x18\x06 \x01(\x0b\x32\x11.test.ComplexType\x12\x32\n\x08map_type\x18\x07 \x03(\x0b\x32 .test.NestedMessage.MapTypeEntry\x12/\n\x05inner\x18\x08 \x01(\x0b\x32 .test.NestedMessage.InnerMessage\x1a.\n\x0cMapTypeEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x1a/\n\x0cInnerMessage\x12\x0e\n\x02id\x18\x01 \x01(\tR\x02id\x12\x0f\n\x03ids\x18\x02 \x03(\x05\x42\x02\x10\x01\"(\n\tInnerEnum\x12\x08\n\x04ZERO\x10\x00\x12\r\n\tALSO_ZERO\x10\x00\x1a\x02\x10\x01J\x04\x08\x0e\x10\x0fJ\x04\x08\x0f\x10\x10J\x04\x08\t\x10\x0cR\x03\x66ooR\x03\x62\x61r*\"\n\x06Status\x12\n\n\x06\x41\x43TIVE\x10\x00\x12\x0c\n\x08INACTIVE\x10\x01\x42\tZ\x07../testb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'nested_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'Z\007../test'
  _NESTEDMESSAGE_MAPTYPEENTRY._options = None
  _NESTEDMESSAGE_MAPTYPEENTRY._serialized_options = b'8\001'
  _NESTEDMESSAGE_INNERMESSAGE.fields_by_name['ids']._options = None
  _NESTEDMESSAGE_INNERMESSAGE.fields_by_name['ids']._serialized_options = b'\020\001'
  _NESTEDMESSAGE_INNERENUM._options = None
  _NESTEDMESSAGE_INNERENUM._serialized_options = b'\020\001'
  _STATUS._serialized_start=757
  _STATUS._serialized_end=791
  _USERID._serialized_start=55
  _USERID._serialized_end=163
  _MESSAGEID._serialized_start=165
  _MESSAGEID._serialized_end=188
  _COMPLEXTYPE._serialized_start=190
  _COMPLEXTYPE._serialized_end=272
  _NESTEDMESSAGE._serialized_start=275
  _NESTEDMESSAGE._serialized_end=755
  _NESTEDMESSAGE_MAPTYPEENTRY._serialized_start=590
  _NESTEDMESSAGE_MAPTYPEENTRY._serialized_end=636
  _NESTEDMESSAGE_INNERMESSAGE._serialized_start=638
  _NESTEDMESSAGE_INNERMESSAGE._serialized_end=685
  _NESTEDMESSAGE_INNERENUM._serialized_start=687
  _NESTEDMESSAGE_INNERENUM._serialized_end=727
# @@protoc_insertion_point(module_scope)