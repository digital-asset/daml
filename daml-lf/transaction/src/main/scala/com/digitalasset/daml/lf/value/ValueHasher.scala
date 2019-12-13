// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value

import java.nio.ByteBuffer
import java.security.MessageDigest

import com.digitalasset.daml.lf.data.{Numeric, Ref, Utf8}
import com.digitalasset.daml.lf.value.Value._

abstract class ValueHasher {

  private def encode(a: Array[Byte]) =
    Ref.LedgerString.assertFromString(
      a.foldLeft(StringBuilder.newBuilder)((acc, b) => acc.append("%02x".format(b))).toString()
    )

  def hashValue(value: Value[AbsoluteContractId]): Array[Byte]

  final def hashValueString(value: Value[AbsoluteContractId]): Ref.LedgerString =
    encode(hashValue(value))

  def hashContract(value: Value[AbsoluteContractId], identifier: Ref.Identifier): Array[Byte]

  final def hashContractString(
      identifier: Ref.Identifier,
      value: Value[AbsoluteContractId]
  ): Ref.LedgerString =
    encode(hashContract(value, identifier))

  def hashContractKey(identifier: Ref.Identifier, value: Value[AbsoluteContractId]): Array[Byte]

  final def hashContractKeyString(
      identifier: Ref.Identifier,
      value: Value[AbsoluteContractId]
  ): Ref.LedgerString =
    encode(hashContractKey(identifier, value))

}

object ValueHasher extends ValueHasher {

  // tag are used to avoid hash collisions due to equal encoding for different objects
  private val tagUnit: Byte = 1
  private val tagTrue: Byte = 2
  private val tagFalse: Byte = 3
  private val tagInt64: Byte = 4
  private val tagNumeric: Byte = 5
  private val tagDate: Byte = 6
  private val tagTimeStamp: Byte = 7
  private val tagText: Byte = 8
  private val tagParty: Byte = 9
  private val tagContractId: Byte = 10
  private val tagNone: Byte = 11
  private val tagSome: Byte = 12
  private val tagList: Byte = 13
  private val tagTextMap: Byte = 14
  private val tagGenMap: Byte = 15
  private val tagRecord: Byte = 16
  private val tagVariant: Byte = 17
  private val tagEnum: Byte = 18
  private val tagContract: Byte = 19
  private val tagContractKey: Byte = 20

  // used for tagging end of variable length data structure
  private val tagEnd: Byte = -1 // Does not appear in any valid UTF8 string

  private implicit class MessageDigestOp(val digest: MessageDigest) extends AnyVal {

    def mixByte(v: Byte): MessageDigest = {
      digest.update(v); digest
    }

    def mixInt(v: Int): MessageDigest = {
      digest.update(ByteBuffer.allocate(4).putInt(v))
      digest
    }

    def mixLong(v: Long): MessageDigest = {
      digest.update(ByteBuffer.allocate(8).putLong(v))
      digest
    }

    def mixBytes(v: Array[Byte]): MessageDigest = {
      digest.update(v)
      digest mixByte tagEnd
    }

    def mixString(v: String): MessageDigest =
      digest mixBytes Utf8.getBytes(v)

    def mixIdentifier(id: Ref.Identifier): MessageDigest =
      digest mixString id.packageId mixString id.qualifiedName.toString

    def mixValue(value: Value[AbsoluteContractId]): MessageDigest = value match {
      case ValueUnit =>
        digest mixByte tagUnit
      case Value.ValueBool(true) =>
        digest mixByte tagTrue
      case Value.ValueBool(false) =>
        digest mixByte tagFalse
      case ValueInt64(v) =>
        digest mixByte tagInt64 mixLong v
      case ValueNumeric(v) =>
        digest mixByte tagNumeric mixString Numeric.toString(v)
      case ValueTimestamp(v) =>
        digest mixByte tagTimeStamp mixLong v.micros
      case ValueDate(v) =>
        digest mixByte tagDate mixInt v.days
      case ValueParty(v) =>
        digest mixByte tagParty mixString v
      case ValueText(v) =>
        digest mixByte tagText mixString v
      case ValueContractId(v) =>
        digest mixByte tagContractId mixString v.coid
      case ValueRecord(_, fs) =>
        fs.foldLeft(digest mixByte tagRecord)(_ mixValue _._2) mixByte tagEnd
      case ValueVariant(_, variant, v) =>
        digest mixByte tagVariant mixString variant mixValue v
      case ValueEnum(_, v) =>
        digest mixByte tagEnum mixString v
      case ValueOptional(None) =>
        digest mixByte tagNone
      case ValueOptional(Some(v)) =>
        digest mixByte tagSome mixValue v
      case ValueList(xs) =>
        xs.iterator.foldLeft(digest mixByte tagList)(_ mixValue _) mixByte tagEnd
      case ValueTextMap(xs) =>
        xs.toImmArray.foldLeft(digest mixByte tagTextMap) {
          case (acc, (k, v)) => acc mixString k mixValue v
        } mixByte tagEnd
      case ValueGenMap(entries) =>
        entries.foldLeft(digest mixByte tagGenMap) {
          case (acc, (k, v)) => acc mixValue k mixValue v
        } mixByte tagEnd
      // Struct: should never be encountered
      case ValueStruct(_) =>
        sys.error("Hashing of struct values is not supported")
    }

    def mixContract(identifier: Ref.Identifier, value: Value[AbsoluteContractId]): MessageDigest =
      digest mixByte tagContract mixIdentifier identifier mixValue value

    def mixContractKey(
        identifier: Ref.Identifier,
        value: Value[AbsoluteContractId]): MessageDigest =
      digest mixByte tagContractKey mixIdentifier identifier mixValue value
  }

  private def newDigest: MessageDigest = MessageDigest.getInstance("SHA-256")

  override final def hashValue(value: Value[AbsoluteContractId]): Array[Byte] =
    (newDigest mixValue value).digest()

  override final def hashContract(
      value: Value[AbsoluteContractId],
      identifier: Ref.Identifier
  ): Array[Byte] =
    newDigest.mixContract(identifier, value).digest()

  override final def hashContractKey(
      identifier: Ref.Identifier,
      value: Value[AbsoluteContractId]
  ): Array[Byte] =
    newDigest.mixContractKey(identifier, value).digest()

}
