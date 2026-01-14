// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.data.support.crypto.MessageSignatureUtil
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.speedy.SValue.{SBool, SText}
import com.digitalasset.daml.lf.speedy.Speedy.Control
import com.digitalasset.daml.lf.speedy.DERTestLib.derPublicKeyHexStringGen
import com.digitalasset.daml.lf.speedy.SBuiltinFun.SBSECP256K1ValidateKey
import org.bouncycastle.jce.provider.BouncyCastleProvider

import java.security.interfaces.ECPublicKey
import java.security.spec.ECGenParameterSpec
import java.security.{KeyPairGenerator, Security}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.immutable.ArraySeq

class SBSECP256K1ValidateKeyTest extends AnyFreeSpec with Matchers with ScalaCheckPropertyChecks {
  import SpeedyTestLib.loggingContext

  Security.addProvider(new BouncyCastleProvider)

  private val compilerConfig = Compiler.Config.Default
  private val dummyMachine = Speedy.Machine.fromPureExpr(
    PureCompiledPackages.Empty(compilerConfig),
    Ast.EBuiltinCon(Ast.BCUnit),
  )

  "correctly validate ECDSA keys" in {
    val keyPair = MessageSignatureUtil.generateKeyPair
    val publicKey = Bytes.fromByteArray(keyPair.getPublic.getEncoded).toHexString

    SBSECP256K1ValidateKey.execute(
      ArraySeq(SText(publicKey)),
      dummyMachine,
    ) match {
      case Control.Value(SBool(true)) =>
        succeed
      case _ =>
        fail()
    }
  }

  "correctly identify non-ECDSA public keys" in {
    val invalidKeyPairGen = KeyPairGenerator.getInstance("RSA")
    invalidKeyPairGen.initialize(1024)
    val invalidPublicKey =
      Bytes.fromByteArray(invalidKeyPairGen.generateKeyPair().getPublic.getEncoded).toHexString

    SBSECP256K1ValidateKey.execute(ArraySeq(SText(invalidPublicKey)), dummyMachine) match {
      case Control.Error(IE.Crypto(IE.Crypto.MalformedKey(`invalidPublicKey`, _))) =>
        succeed
      case _ =>
        fail()
    }
  }

  "correctly identify off curve ECDSA public keys" in {
    val keyPairGen = KeyPairGenerator.getInstance("EC", "BC")
    // Use a different EC curve to generate a ECDSA public key that is not on the secp256k1 curve
    // - we do this as raw ECPoints are checked as being on the secp256k1 curve during key generation!
    keyPairGen.initialize(new ECGenParameterSpec("secp256r1"))
    val fakePublicKey = keyPairGen.generateKeyPair().getPublic.asInstanceOf[ECPublicKey]

    // Ensure we do not have POINT_INFINITY
    fakePublicKey.getW.getAffineX should not be null
    fakePublicKey.getW.getAffineY should not be null

    SBSECP256K1ValidateKey.execute(
      ArraySeq(SText(Bytes.fromByteArray(fakePublicKey.getEncoded).toHexString)),
      dummyMachine,
    ) match {
      case Control.Value(SBool(false)) =>
        succeed
      case _ =>
        fail()
    }
  }

  "correctly identify invalid DER encoded public keys during decoding" in {
    forAll(derPublicKeyHexStringGen) { (invalidPublicKey: Ref.HexString) =>
      SBSECP256K1ValidateKey.execute(
        ArraySeq(SText(invalidPublicKey)),
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
}
