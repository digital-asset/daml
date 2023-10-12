// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import com.google.protobuf.ByteString
import org.bouncycastle.asn1.x509.AlgorithmIdentifier
import org.scalatest.wordspec.AsyncWordSpec

import java.security.PublicKey as JPublicKey
import scala.concurrent.Future

trait JavaPublicKeyConverterTest extends BaseTest with CryptoTestHelper { this: AsyncWordSpec =>

  private def javaSigningKeyConvertTest(
      name: String,
      newCrypto: => Future[Crypto],
      newPublicKey: Crypto => Future[SigningPublicKey],
      fromPublicKey: Crypto => (
          JPublicKey,
          AlgorithmIdentifier,
          Fingerprint,
      ) => Either[JavaKeyConversionError, SigningPublicKey],
      convertToName: String,
  ): Unit = {

    s"Convert $name signing public key to Java and back to $convertToName" in {
      for {
        crypto <- newCrypto
        publicKey <- newPublicKey(crypto)
        hash = TestHash.digest("foobar")
        sig <- crypto.privateCrypto
          .sign(hash, publicKey.fingerprint)
          .valueOrFail("signing")
        (algoId, javaPublicKey) = crypto.javaKeyConverter
          .toJava(publicKey)
          .valueOrFail("convert to java")
        publicKey2 = fromPublicKey(crypto)(javaPublicKey, algoId, publicKey.fingerprint)
          .valueOrFail("convert from java")
        _ = crypto.pureCrypto
          .verifySignature(hash, publicKey2, sig)
          .valueOrFail("verify signature with converted key")
      } yield {
        publicKey.fingerprint shouldEqual publicKey2.fingerprint
      }
    }
  }

  private def javaEncryptionKeyConvertTest(
      name: String,
      newCrypto: => Future[Crypto],
      newPublicKey: Crypto => Future[EncryptionPublicKey],
      fromPublicKey: Crypto => (
          JPublicKey,
          AlgorithmIdentifier,
          Fingerprint,
      ) => Either[JavaKeyConversionError, EncryptionPublicKey],
      convertToName: String,
  ): Unit = {

    s"Convert $name encryption public key to Java and back to $convertToName" in {
      for {
        crypto <- newCrypto
        publicKey <- newPublicKey(crypto)
        (algoId, javaPublicKey) = crypto.javaKeyConverter
          .toJava(publicKey)
          .valueOrFail("convert to java")
        publicKey2 = fromPublicKey(crypto)(javaPublicKey, algoId, publicKey.fingerprint)
          .valueOrFail("convert from java")
        message = CryptoTestHelper.TestMessage(ByteString.copyFromUtf8("test"))
        ciphertext = crypto.pureCrypto
          .encryptWith(message, publicKey2, testedProtocolVersion)
          .valueOrFail("encrypt with converted key")
        message2 <- crypto.privateCrypto
          .decrypt(ciphertext)(CryptoTestHelper.TestMessage.fromByteString)
          .valueOrFail("decrypt")
      } yield {
        publicKey.fingerprint shouldEqual publicKey2.fingerprint
        message shouldEqual message2
      }
    }
  }

  def javaPublicKeyConverterProvider(
      supportedSigningKeySchemes: Set[SigningKeyScheme],
      supportedEncryptionKeySchemes: Set[EncryptionKeyScheme],
      newCrypto: => Future[Crypto],
      convertName: String,
  ): Unit = {
    forAll(supportedSigningKeySchemes) { signingKeyScheme =>
      javaSigningKeyConvertTest(
        signingKeyScheme.toString,
        newCrypto,
        crypto => getSigningPublicKey(crypto, signingKeyScheme),
        crypto => crypto.javaKeyConverter.fromJavaSigningKey,
        convertName,
      )
    }

    forAll(supportedEncryptionKeySchemes) { encryptionKeyScheme =>
      javaEncryptionKeyConvertTest(
        encryptionKeyScheme.toString,
        newCrypto,
        crypto => getEncryptionPublicKey(crypto, encryptionKeyScheme),
        crypto => crypto.javaKeyConverter.fromJavaEncryptionKey,
        convertName,
      )
    }
  }

  def javaPublicKeyConverterProviderOther(
      supportedSigningKeySchemes: Set[SigningKeyScheme],
      supportedEncryptionKeySchemes: Set[EncryptionKeyScheme],
      newCrypto: => Future[Crypto],
      otherConvertName: String,
      otherKeyConverter: JavaKeyConverter,
  ): Unit = {
    forAll(supportedSigningKeySchemes) { signingKeyScheme =>
      javaSigningKeyConvertTest(
        signingKeyScheme.toString,
        newCrypto,
        crypto => getSigningPublicKey(crypto, signingKeyScheme),
        _ => otherKeyConverter.fromJavaSigningKey,
        otherConvertName,
      )
    }

    forAll(supportedEncryptionKeySchemes) { encryptionKeyScheme =>
      javaEncryptionKeyConvertTest(
        encryptionKeyScheme.toString,
        newCrypto,
        crypto => getEncryptionPublicKey(crypto, encryptionKeyScheme),
        _ => otherKeyConverter.fromJavaEncryptionKey,
        otherConvertName,
      )
    }
  }
}
