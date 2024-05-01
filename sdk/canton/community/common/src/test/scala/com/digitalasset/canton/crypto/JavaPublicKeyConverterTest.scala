// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

  /** Test conversion of keys from the providers' format to Java and back.
    *
    * @param newSigningPublicKey either generate a new signing public key using `getSigningPublicKey`
    *                                       or use an alternative custom key generation function. This is
    *                                       used to specify a custom function that generates a different
    *                                       signing public key; for example a Tink key with TINK output
    *                                       prefix.
    * @param newEncryptionPublicKey either generate a new encryption public key using `getEncryptionPublicKey`
    *                                          or use an alternative custom key generation function. This is
    *                                          used to specify a custom function that generates a different
    *                                          encryption public key; for example a Tink key with TINK output
    *                                          prefix.
    */
  def javaPublicKeyConverterProvider(
      supportedSigningKeySchemes: Set[SigningKeyScheme],
      supportedEncryptionKeySchemes: Set[EncryptionKeyScheme],
      newCrypto: => Future[Crypto],
      convertName: String,
      newSigningPublicKey: (Crypto, SigningKeyScheme) => Future[SigningPublicKey] =
        getSigningPublicKey,
      newEncryptionPublicKey: (
          Crypto,
          EncryptionKeyScheme,
      ) => Future[EncryptionPublicKey] = getEncryptionPublicKey,
      testNameSuffix: String = "",
  ): Unit =
    s"Convert public keys to java keys with own provider $testNameSuffix" should {
      forAll(supportedSigningKeySchemes) { signingKeyScheme =>
        javaSigningKeyConvertTest(
          signingKeyScheme.toString,
          newCrypto,
          crypto => newSigningPublicKey(crypto, signingKeyScheme),
          crypto => crypto.javaKeyConverter.fromJavaSigningKey,
          convertName,
        )
      }

      forAll(supportedEncryptionKeySchemes) { encryptionKeyScheme =>
        javaEncryptionKeyConvertTest(
          encryptionKeyScheme.toString,
          newCrypto,
          crypto => newEncryptionPublicKey(crypto, encryptionKeyScheme),
          crypto => crypto.javaKeyConverter.fromJavaEncryptionKey,
          convertName,
        )
      }
    }

  /** Test conversion of keys from another providers' format to Java and back.
    *
    * @param newSigningPublicKey either generate a new signing public key using `getSigningPublicKey`
    *                                       or use an alternative custom key generation function. This is
    *                                       used to specify a custom function that generates a different
    *                                       signing public key; for example a Tink key with TINK output
    *                                       prefix.
    * @param newEncryptionPublicKey either generate a new encryption public key using `getEncryptionPublicKey`
    *                                          or use an alternative custom key generation function. This is
    *                                          used to specify a custom function that generates a different
    *                                          encryption public key; for example a Tink key with TINK output
    *                                          prefix.
    */
  def javaPublicKeyConverterProviderOther(
      supportedSigningKeySchemes: Set[SigningKeyScheme],
      supportedEncryptionKeySchemes: Set[EncryptionKeyScheme],
      newCrypto: => Future[Crypto],
      otherConvertName: String,
      otherKeyConverter: JavaKeyConverter,
      newSigningPublicKey: (Crypto, SigningKeyScheme) => Future[SigningPublicKey] =
        getSigningPublicKey,
      newEncryptionPublicKey: (
          Crypto,
          EncryptionKeyScheme,
      ) => Future[EncryptionPublicKey] = getEncryptionPublicKey,
      testNameSuffix: String = "",
  ): Unit =
    s"Convert public keys to java keys with different provider $testNameSuffix" should {
      forAll(supportedSigningKeySchemes) { signingKeyScheme =>
        javaSigningKeyConvertTest(
          signingKeyScheme.toString,
          newCrypto,
          crypto => newSigningPublicKey(crypto, signingKeyScheme),
          _ => otherKeyConverter.fromJavaSigningKey,
          otherConvertName,
        )
      }

      forAll(supportedEncryptionKeySchemes) { encryptionKeyScheme =>
        javaEncryptionKeyConvertTest(
          encryptionKeyScheme.toString,
          newCrypto,
          crypto => newEncryptionPublicKey(crypto, encryptionKeyScheme),
          _ => otherKeyConverter.fromJavaEncryptionKey,
          otherConvertName,
        )
      }
    }

}
