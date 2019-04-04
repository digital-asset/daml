#!/usr/bin/env python
import sys
import os
# Import protoc generated {de,}serializers (generated from test_proto{,_import}.proto)
from test_proto_pb2 import *
import test_proto_import_pb2       as test_proto_import
import test_proto_oneof_pb2        as test_proto_oneof
import test_proto_oneof_import_pb2 as test_proto_oneof_import

def write_proto(msg):
    out = msg.SerializeToString()
    print len(out)
    sys.stdout.write(out)

# Test case 1: Trivial message
write_proto(Trivial(trivialField = 0x7BADBEEF))

# Test case 2: Multiple fields
write_proto(
    MultipleFields(
        multiFieldDouble = 1.125,
        multiFieldFloat = 1e9,
        multiFieldInt32 = 0x1135,
        multiFieldInt64 = 0x7FFAFABADDEAFFA0,
        multiFieldString = "Goodnight moon",
        multiFieldBool = False))

# Test case: SignedInts
write_proto(SignedInts(signed32 = 0, signed64 = 0))
write_proto(SignedInts(signed32 = 42, signed64 = 84))
write_proto(SignedInts(signed32 = (-42), signed64 = (-84)))
write_proto(SignedInts(signed32 = -(2**31), signed64 = -(2**63)))
write_proto(SignedInts(signed32 = (2**32 - 1) / 2, signed64 = (2**64 - 1) / 2))

# Test case 3: Nested enumeration
write_proto(WithEnum(enumField = WithEnum.ENUM1))
write_proto(WithEnum(enumField = WithEnum.ENUM2))
write_proto(WithEnum(enumField = WithEnum.ENUM3))
write_proto(WithEnum(enumField = 0xBEEF))

# Test case 4: Nested messages
write_proto(
    WithNesting(nestedMessage=
                WithNesting.Nested(nestedField1   = "testCase4 nestedField1",
                                   nestedField2   = 0xABCD,
                                   nestedPacked   = [],
                                   nestedUnpacked = [])))
write_proto(WithNesting())

# Test case 5: Nested repeated message
write_proto(WithNestingRepeated(nestedMessages =
    [ WithNestingRepeated.Nested(nestedField1 = "testCase5 nestedField1",
                                 nestedField2 = 0xDCBA,
                                 nestedPacked = [1, 1, 2, 3, 5],
                                 nestedUnpacked = [0xB, 0xABCD, 0xBADBEEF, 0x10203040]),
      WithNestingRepeated.Nested(nestedField1 = "Hello world",
                                 nestedField2 = 0x7FFFFFFF,
                                 nestedPacked = [0, 0, 0],
                                 nestedUnpacked = []),
      WithNestingRepeated.Nested(nestedField1 = "", nestedField2 = 0,
                                 nestedPacked = [], nestedUnpacked = []) ]))

write_proto(WithNestingRepeated())

# Test case 6: Nested repeated int message
write_proto(WithNestingRepeatedInts(nestedInts=[NestedInts(nestedInt1 = 636513, nestedInt2 = 619021)]))
write_proto(WithNestingRepeatedInts(nestedInts=[]))
write_proto(WithNestingRepeatedInts(nestedInts=
    [ NestedInts(nestedInt1 = 636513, nestedInt2 = 619021),
      NestedInts(nestedInt1 = 423549, nestedInt2 = 687069),
      NestedInts(nestedInt1 = 545506, nestedInt2 = 143731),
      NestedInts(nestedInt1 = 193605, nestedInt2 = 385360) ]))

# Test case 7: Repeated int32 field
write_proto(WithRepetition())
write_proto(WithRepetition(repeatedField1 = range(1, 10001)))

# Test case 8: Fixed-width integer types
write_proto(WithFixed(fixed1 = 0, fixed2 = 0, fixed3 = 0, fixed4 = 0))
write_proto(WithFixed(fixed1 = 2**32 - 1,
                      fixed2 = (2**32 - 1) / 2,
                      fixed3 = 2**64 - 1,
                      fixed4 = (2**64 - 1) / 2))
write_proto(WithFixed(fixed1 = 0,
                      fixed2 = -(2**31),
                      fixed3 = 0,
                      fixed4 = -(2**63)))

# Test case 9: bytes fields
write_proto(WithBytes(bytes1 = "\x00\x00\x00\x01\x02\x03\xFF\xFF\x00\x01",
                      bytes2 = ["", "\x01", "\xAB\xBAhello", "\xBB"]))
write_proto(WithBytes(bytes1 = "Hello world", bytes2 = []))
write_proto(WithBytes(bytes1 = "", bytes2 = ["Hello", "\x00world", "\x00\x00"]))
write_proto(WithBytes(bytes1 = "", bytes2 = []))

# Test case 10: packed v unpacked repeated types
write_proto(WithPacking(packing1 = [], packing2 = []))
write_proto(WithPacking(packing1 = [100, 2000, 300, 4000, 500, 60000, 7000],
                        packing2 = []))
write_proto(WithPacking(packing1 = [],
                        packing2 = [100, 2000, 300, 4000, 500, 60000, 7000]))
write_proto(WithPacking(packing1 = [1, 2, 3, 4, 5],
                        packing2 = [5, 4, 3, 2, 1]))

# Test case 11: All possible packed types
write_proto(AllPackedTypes(packedWord32 = [],
                           packedWord64 = [],
                           packedInt32 = [],
                           packedInt64 = [],
                           packedFixed32 = [],
                           packedFixed64 = [],
                           packedFloat = [],
                           packedDouble = [],
                           packedSFixed32 = [],
                           packedSFixed64 = [],
                           packedBool = [],
                           packedEnum = [],
                           unpackedEnum = []))
write_proto(AllPackedTypes(packedWord32 = [1],
                           packedWord64 = [2],
                           packedInt32 = [3],
                           packedInt64 = [4],
                           packedFixed32 = [5],
                           packedFixed64 = [6],
                           packedFloat = [7],
                           packedDouble = [8],
                           packedSFixed32 = [9],
                           packedSFixed64 = [10],
                           packedBool = [False],
                           packedEnum = [FLD0],
                           unpackedEnum = [FLD0]))
write_proto(AllPackedTypes(packedWord32 = [1],
                           packedWord64 = [2],
                           packedInt32 = [-3],
                           packedInt64 = [-4],
                           packedFixed32 = [5],
                           packedFixed64 = [6],
                           packedFloat = [-7],
                           packedDouble = [-8],
                           packedSFixed32 = [-9],
                           packedSFixed64 = [-10],
                           packedBool = [True],
                           packedEnum = [FLD1],
                           unpackedEnum = [FLD1]))
expected_fp = [x / 8.0 for x in range(8, 80001)]
write_proto(AllPackedTypes(packedWord32 = range(1, 10001),
                           packedWord64 = range(1, 10001),
                           packedInt32 = range(1, 10001),
                           packedInt64 = range(1, 10001),
                           packedFixed32 = range(1, 10001),
                           packedFixed64 = range(1, 10001),
                           packedFloat = expected_fp,
                           packedDouble = expected_fp,
                           packedSFixed32 = range(1, 10001),
                           packedSFixed64 = range(1, 10001),
                           packedBool = [False,True],
                           packedEnum = [FLD0,FLD1],
                           unpackedEnum = [FLD0,FLD1]))

# Test case 12: message with out of order field numbers
write_proto(OutOfOrderFields(field1 = [], field2 = "", field3 = 2 ** 63 - 1, field4 = []))
write_proto(OutOfOrderFields(field1 = range(1, 101, 6), field2 = "This is a test", field3 = -(2 ** 63), field4 = ["This", "is", "a", "test"]))

# Test case 13: Nested mesage with the same name as another package-level message
write_proto(ShadowedMessage(name = "name", value = 0x7DADBEEF))
write_proto(MessageShadower(shadowed_message = MessageShadower.ShadowedMessage(name = "name", value = "string value"),
                            name = "another name"))
write_proto(MessageShadower.ShadowedMessage(name = "another name", value = "another string"))

# Test case 14: Qualified name resolution
write_proto(WithQualifiedName(qname1 = ShadowedMessage(name="int value", value=42),
                              qname2 = MessageShadower.ShadowedMessage(name="string value", value="hello world")))

# Test case 15: Imported message resolution
write_proto(test_proto_import.WithNesting(nestedMessage1 = test_proto_import.WithNesting.Nested(nestedField1 = 1, nestedField2 = 2)))

# Test case 16: Proper resolution of shadowed message names
write_proto(UsingImported(importedNesting = test_proto_import.WithNesting(nestedMessage1 = test_proto_import.WithNesting.Nested(nestedField1 = 1, nestedField2 = 2),
                                                                          nestedMessage2 = test_proto_import.WithNesting.Nested(nestedField1 = 3, nestedField2 = 4)),
                          localNesting = WithNesting(nestedMessage = WithNesting.Nested(nestedField1 = "field", nestedField2 = 0xBEEF, nestedPacked = [], nestedUnpacked = []))))

# Test case 17: Oneof

# Send default values for oneof subfields
write_proto(test_proto_oneof.Something(value=1, another=2, name=""))
write_proto(test_proto_oneof.Something(value=3, another=4, someid=0))
write_proto(test_proto_oneof.Something(value=5, another=6, dummyMsg1=test_proto_oneof.DummyMsg(dummy=0)))
write_proto(test_proto_oneof.Something(value=7, another=8, dummyMsg2=test_proto_oneof.DummyMsg(dummy=0)))
write_proto(test_proto_oneof.Something(value=9, another=10, dummyEnum=test_proto_oneof.DUMMY0))

# Send non-default values for oneof subfields
write_proto(test_proto_oneof.Something(value=1, another=2, name="hello world"))
write_proto(test_proto_oneof.Something(value=3, another=4, someid=42))
write_proto(test_proto_oneof.Something(value=5, another=6, dummyMsg1=test_proto_oneof.DummyMsg(dummy=66)))
write_proto(test_proto_oneof.Something(value=7, another=8, dummyMsg2=test_proto_oneof.DummyMsg(dummy=67)))
write_proto(test_proto_oneof.Something(value=9, another=10, dummyEnum=test_proto_oneof.DUMMY1))

# Send with oneof not set
write_proto(test_proto_oneof.Something(value=11, another=12))

# Test case 18: Imported Oneof
write_proto(test_proto_oneof.WithImported(dummyMsg1=test_proto_oneof.DummyMsg(dummy=0)))
write_proto(test_proto_oneof.WithImported(dummyMsg1=test_proto_oneof.DummyMsg(dummy=68)))
write_proto(test_proto_oneof.WithImported(withOneof=test_proto_oneof_import.WithOneof()))
write_proto(test_proto_oneof.WithImported(withOneof=test_proto_oneof_import.WithOneof(a="")))
write_proto(test_proto_oneof.WithImported(withOneof=test_proto_oneof_import.WithOneof(b=0)))
write_proto(test_proto_oneof.WithImported(withOneof=test_proto_oneof_import.WithOneof(a="foo")))
write_proto(test_proto_oneof.WithImported(withOneof=test_proto_oneof_import.WithOneof(b=19)))
write_proto(test_proto_oneof.WithImported())

# Send the special 'done' message
write_proto(MultipleFields(multiFieldString = "All tests complete"))
