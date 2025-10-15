// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data.support.crypto.MessageSignatureUtil
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.language.{Ast, LanguageMajorVersion}
import com.digitalasset.daml.lf.speedy.SBuiltinFun.SBSECP256K1WithEcdsaBool
import com.digitalasset.daml.lf.speedy.SValue.{SBool, SText}
import com.digitalasset.daml.lf.speedy.Speedy.Control
import org.bouncycastle.asn1.{
  ASN1Encodable,
  ASN1Integer,
  ASN1ObjectIdentifier,
  ASN1Primitive,
  DERBitString,
  DEROctetString,
  DLSequence,
}
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.math.BigInteger
import java.security.{KeyPairGenerator, Security}
import java.security.spec.InvalidKeySpecException
import scala.collection.immutable.ArraySeq

class SBSECP256K1BoolTestV2 extends SBSECP256K1BoolTest(LanguageMajorVersion.V2)

class SBSECP256K1BoolTest(majorLanguageVersion: LanguageMajorVersion)
    extends AnyFreeSpec
    with Matchers
    with ScalaCheckPropertyChecks {
  import SBSECP256K1BoolTest._
  import SpeedyTestLib.loggingContext

  Security.addProvider(new BouncyCastleProvider)

  private val compilerConfig = Compiler.Config.Default(majorLanguageVersion)
  private val dummyMachine = Speedy.Machine.fromPureExpr(
    PureCompiledPackages.Empty(compilerConfig),
    Ast.EBuiltinCon(Ast.BCUnit),
  )

  "PublicKeys are correctly built from hex encoded public key strings" in {
    val actualPublicKey = MessageSignatureUtil.generateKeyPair.getPublic
    val hexEncodedPublicKey = Bytes.fromByteArray(actualPublicKey.getEncoded).toHexString

    SBSECP256K1WithEcdsaBool.extractPublicKey(hexEncodedPublicKey) shouldBe actualPublicKey
  }

  "PublicKeys fail to be built from invalid hex encoded public key strings" in {
    val invalidHexEncodedPublicKey = Ref.HexString.assertFromString("deadbeef")

    assertThrows[InvalidKeySpecException](
      SBSECP256K1WithEcdsaBool.extractPublicKey(invalidHexEncodedPublicKey)
    )
  }

  "PublicKeys fail to be built from incorrectly formatted hex encoded public key strings" in {
    val keyPairGen = KeyPairGenerator.getInstance("RSA")
    keyPairGen.initialize(1024)
    val badFormatPublicKey = keyPairGen.generateKeyPair().getPublic
    val invalidHexEncodedPublicKey =
      Ref.HexString.encode(Bytes.fromByteArray(badFormatPublicKey.getEncoded))

    assertThrows[InvalidKeySpecException](
      SBSECP256K1WithEcdsaBool.extractPublicKey(invalidHexEncodedPublicKey)
    )
  }

  "correctly identify invalid DER encoded public keys during decoding" in {
    val keyPair = MessageSignatureUtil.generateKeyPair
    val privateKey = keyPair.getPrivate
    val message = Ref.HexString.assertFromString("deadbeef")
    val signature = MessageSignatureUtil.sign(message, privateKey)

    forAll(derPublicKeyHexStringGen) { (invalidPublicKey: Ref.HexString) =>
      SBSECP256K1WithEcdsaBool.execute(
        ArraySeq(SText(signature), SText(message), SText(invalidPublicKey)),
        dummyMachine,
      ) match {
        case Control.Error(IE.Crypto(IE.Crypto.MalformedByteEncoding(`invalidPublicKey`, _))) =>
          succeed
        case Control.Error(IE.Crypto(IE.Crypto.MalformedKey(`invalidPublicKey`, _))) =>
          succeed
        case _ =>
          fail()
      }
    }
  }

  "correctly identify invalid DER encoded signatures" in {
    val keyPair = MessageSignatureUtil.generateKeyPair
    val publicKey = Bytes.fromByteArray(keyPair.getPublic.getEncoded).toHexString
    val message = Ref.HexString.assertFromString("deadbeef")

    forAll(derSignatureHexStringGen) { (invalidSignature: Ref.HexString) =>
      SBSECP256K1WithEcdsaBool.execute(
        ArraySeq(SText(invalidSignature), SText(message), SText(publicKey)),
        dummyMachine,
      ) match {
        case Control.Value(SBool(false)) =>
          // May have a valid DER format that otherwise will fail verification
          succeed
        case Control.Error(IE.Crypto(IE.Crypto.MalformedSignature(`invalidSignature`, _))) =>
          succeed
        case _ =>
          fail()
      }
    }
  }
}

object SBSECP256K1BoolTest {
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
