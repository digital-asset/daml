// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data.support.crypto.MessageSignatureUtil
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.speedy.SBuiltinFun.SBSECP256K1WithEcdsaBool
import com.digitalasset.daml.lf.speedy.SValue.{SBool, SText}
import com.digitalasset.daml.lf.speedy.Speedy.Control
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.security.{KeyPairGenerator, Security}
import java.security.spec.InvalidKeySpecException
import scala.collection.immutable.ArraySeq

class SBSECP256K1BoolTest extends AnyFreeSpec with Matchers with ScalaCheckPropertyChecks {
  import DERTestLib._
  import SpeedyTestLib.loggingContext

  Security.addProvider(new BouncyCastleProvider)

  private val compilerConfig = Compiler.Config.Default
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
