# Generates the test-files/*.bin files used in the unit tests.
import test_proto_pb2

def serialize_to_file(msg, fp):
    with open(fp, 'wb') as f:
        f.write(msg.SerializeToString())

def main():
    triv = test_proto_pb2.Trivial()
    triv.trivialField = 123
    serialize_to_file(triv, 'trivial.bin')

    multipleFields = test_proto_pb2.MultipleFields()
    multipleFields.multiFieldDouble = 1.23
    multipleFields.multiFieldFloat = -0.5
    multipleFields.multiFieldInt32 = 123
    multipleFields.multiFieldInt64 = 1234567890
    multipleFields.multiFieldString = "Hello, world!"
    multipleFields.multiFieldBool = True
    serialize_to_file(multipleFields, 'multiple_fields.bin')

    signedints= test_proto_pb2.SignedInts()
    signedints.signed32 = -42
    signedints.signed64 = -84
    serialize_to_file(signedints, 'signedints.bin')

    withEnum = test_proto_pb2.WithEnum()
    withEnum.enumField = test_proto_pb2.WithEnum.ENUM1
    serialize_to_file(withEnum, 'with_enum0.bin')

    withEnum = test_proto_pb2.WithEnum()
    withEnum.enumField = test_proto_pb2.WithEnum.ENUM2
    serialize_to_file(withEnum, 'with_enum1.bin')

    withNesting = test_proto_pb2.WithNesting()
    withNesting.nestedMessage.nestedField1 = "123abc"
    withNesting.nestedMessage.nestedField2 = 123456
    serialize_to_file(withNesting, 'with_nesting.bin')

    withNestingRepeated = test_proto_pb2.WithNestingRepeated()
    msg1 = withNestingRepeated.nestedMessages.add()
    msg1.nestedField1 = "123abc"
    msg1.nestedField2 = 123456
    msg1.nestedPacked.extend([1,2,3,4])
    msg1.nestedUnpacked.extend([5,6,7,8])
    msg2 = withNestingRepeated.nestedMessages.add()
    msg2.nestedField1 = "abc123"
    msg2.nestedField2 = 654321
    msg2.nestedPacked.extend([0,9,8,7])
    msg2.nestedUnpacked.extend([6,5,4,3])
    serialize_to_file(withNestingRepeated, "with_nesting_repeated.bin")

    nestingRepeatedMissingFields = test_proto_pb2.WithNestingRepeatedInts()
    msg1 = nestingRepeatedMissingFields.nestedInts.add()
    msg1.nestedInt1 = 0
    msg1.nestedInt2 = 2
    msg2 = nestingRepeatedMissingFields.nestedInts.add()
    msg2.nestedInt1 = 2
    msg2.nestedInt2 = 0
    serialize_to_file(nestingRepeatedMissingFields, "with_nesting_ints.bin")

    withRepetition = test_proto_pb2.WithRepetition()
    withRepetition.repeatedField1.extend([1,2,3,4,5])
    serialize_to_file(withRepetition, 'with_repetition.bin')

    trivNeg = test_proto_pb2.Trivial()
    trivNeg.trivialField = -1
    serialize_to_file(trivNeg, 'trivial_negative.bin')

    withFixed = test_proto_pb2.WithFixed()
    withFixed.fixed1 = 16
    withFixed.fixed2 = -123
    withFixed.fixed3 = 4096
    withFixed.fixed4 = -4096
    serialize_to_file(withFixed, 'with_fixed.bin')

    withBytes = test_proto_pb2.WithBytes()
    withBytes.bytes1 = "abc"
    withBytes.bytes2.extend(["abc", "123"])
    serialize_to_file(withBytes, "with_bytes.bin")

    withPacking = test_proto_pb2.WithPacking()
    withPacking.packing1.extend([1,2,3])
    withPacking.packing2.extend([1,2,3])
    serialize_to_file(withPacking, "with_packing.bin")

    allPackedTypes = test_proto_pb2.AllPackedTypes()
    allPackedTypes.packedWord32.extend([1,2,3])
    allPackedTypes.packedWord64.extend([1,2,3])
    allPackedTypes.packedInt32.extend([-1,-2,-3])
    allPackedTypes.packedInt64.extend([-1,-2,-3])
    allPackedTypes.packedFixed32.extend([1,2,3])
    allPackedTypes.packedFixed64.extend([1,2,3])
    allPackedTypes.packedFloat.extend([1.0,2.0])
    allPackedTypes.packedDouble.extend([1.0,-1.0])
    allPackedTypes.packedSFixed32.extend([1,2,3])
    allPackedTypes.packedSFixed64.extend([1,2,3])
    allPackedTypes.packedBool.extend([False,True])
    allPackedTypes.packedEnum.extend([test_proto_pb2.FLD0,test_proto_pb2.FLD1])
    allPackedTypes.unpackedEnum.extend([test_proto_pb2.FLD0,test_proto_pb2.FLD1])
    serialize_to_file(allPackedTypes, "all_packed_types.bin")

if __name__ == '__main__':
    main()
