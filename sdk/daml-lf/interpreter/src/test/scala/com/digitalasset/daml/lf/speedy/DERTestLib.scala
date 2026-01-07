// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data.{Bytes, Ref}
import org.bouncycastle.asn1.{
  ASN1Encodable,
  ASN1Integer,
  ASN1ObjectIdentifier,
  ASN1Primitive,
  DERBitString,
  DEROctetString,
  DLSequence,
}
import org.scalacheck.{Arbitrary, Gen}

import java.math.BigInteger

object DERTestLib {
  object DERUtil {
    def encode(data: ASN1Primitive): Bytes = {
      Bytes.fromByteArray(data.getEncoded())
    }

    def decode(data: Bytes): ASN1Primitive = {
      ASN1Primitive.fromByteArray(data.toByteArray)
    }
  }

  def derBitStringGen: Gen[DERBitString] =
    for {
      bytes <- Gen.frequency(
        1 -> Gen.listOf(Arbitrary.arbitrary[Byte]),
        99 -> Gen.listOfN(32, Arbitrary.arbitrary[Byte]),
      )
    } yield new DERBitString(new DEROctetString(bytes.toArray))

  def asn1ObjectIdentifierGen: Gen[ASN1ObjectIdentifier] = {
    for {
      label <- Gen.frequency(
        1 -> (for {
          prefix <- Gen.oneOf(0, 1, 2)
          suffix <- Gen.nonEmptyListOf(Gen.posNum[Int])
        } yield (prefix +: suffix).mkString(".")),
        99 -> Gen.oneOf("1.2.840.10045.2.1", "1.3.132.0.10"),
      )
    } yield new ASN1ObjectIdentifier(label)
  }

  def asn1ObjectIdentifierSequenceGen: Gen[DLSequence] = {
    for {
      objIds <- Gen.frequency(
        1 -> Gen.listOf(asn1ObjectIdentifierGen),
        99 -> Gen.listOfN(2, asn1ObjectIdentifierGen),
      )
    } yield new DLSequence(objIds.toArray[ASN1Encodable])
  }

  def derPublicKeyGen: Gen[DLSequence] =
    for {
      objId <- asn1ObjectIdentifierSequenceGen
      bitStr <- derBitStringGen
    } yield new DLSequence(Array[ASN1Encodable](objId, bitStr))

  def derPublicKeyHexStringGen: Gen[Ref.HexString] =
    Gen.frequency(
      1 -> Gen
        .listOf(Arbitrary.arbitrary[Byte])
        .map(bytes => Bytes.fromByteArray(bytes.toArray).toHexString),
      99 -> derPublicKeyGen.map(derObj => DERUtil.encode(derObj).toHexString),
    )

  def bigIntegerGen: Gen[BigInteger] =
    for {
      size <- Gen.oneOf(77, 76, 78)
      num <- Gen.listOfN(size, Gen.oneOf("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"))
    } yield new BigInteger(num.mkString)

  def asn1IntegerGen: Gen[ASN1Integer] =
    for {
      n <- bigIntegerGen
    } yield new ASN1Integer(n)

  def derSignatureGen: Gen[DLSequence] = {
    for {
      coords <- Gen.frequency(
        1 -> Gen.listOf(asn1IntegerGen),
        99 -> Gen.listOfN(2, asn1IntegerGen),
      )
    } yield new DLSequence(coords.toArray[ASN1Encodable])
  }

  def derSignatureHexStringGen: Gen[Ref.HexString] =
    Gen.frequency(
      1 -> Gen
        .listOf(Arbitrary.arbitrary[Byte])
        .map(bytes => Bytes.fromByteArray(bytes.toArray).toHexString),
      99 -> derSignatureGen.map(derObj => DERUtil.encode(derObj).toHexString),
    )
}
