// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.symbolic

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.memory.{
  InMemoryCryptoPrivateStore,
  InMemoryCryptoPublicStore,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.version.ReleaseProtocolVersion
import com.google.protobuf.ByteString
import org.bouncycastle.asn1.x509.AlgorithmIdentifier

import java.security.PublicKey as JPublicKey
import scala.concurrent.{ExecutionContext, Future}

object SymbolicCrypto {

  private val keyData: ByteString = ByteString.copyFromUtf8("symbolic_crypto_key_data")

  // Note: The scheme is ignored for symbolic keys
  def signingPublicKey(keyId: String): SigningPublicKey = signingPublicKey(
    Fingerprint.tryCreate(keyId)
  )

  def signingPublicKey(keyId: Fingerprint): SigningPublicKey =
    new SigningPublicKey(keyId, CryptoKeyFormat.Symbolic, keyData, SigningKeyScheme.Ed25519)

  // Randomly generated private key with the given fingerprint
  def signingPrivateKey(keyId: Fingerprint): SigningPrivateKey =
    new SigningPrivateKey(
      keyId,
      CryptoKeyFormat.Symbolic,
      keyData,
      SigningKeyScheme.Ed25519,
    )

  def signingPrivateKey(keyId: String): SigningPrivateKey = signingPrivateKey(
    Fingerprint.tryCreate(keyId)
  )

  def encryptionPublicKey(keyId: Fingerprint): EncryptionPublicKey =
    new EncryptionPublicKey(
      keyId,
      CryptoKeyFormat.Symbolic,
      keyData,
      EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm,
    )

  def encryptionPublicKey(keyId: String): EncryptionPublicKey = encryptionPublicKey(
    Fingerprint.tryCreate(keyId)
  )

  // Randomly generated private key with the given fingerprint
  def encryptionPrivateKey(keyId: Fingerprint): EncryptionPrivateKey =
    new EncryptionPrivateKey(
      keyId,
      CryptoKeyFormat.Symbolic,
      keyData,
      EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm,
    )

  def encryptionPrivateKey(keyId: String): EncryptionPrivateKey =
    encryptionPrivateKey(Fingerprint.tryCreate(keyId))

  def signature(signature: ByteString, signedBy: Fingerprint): Signature =
    SymbolicPureCrypto.createSignature(signature, signedBy, 0xffffffff)

  def emptySignature: Signature =
    signature(ByteString.EMPTY, Fingerprint.create(ByteString.EMPTY))

  def create(
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  ): Crypto = {
    implicit val ec: ExecutionContext =
      DirectExecutionContext(loggerFactory.getLogger(this.getClass))

    val pureCrypto = new SymbolicPureCrypto()
    val cryptoPublicStore = new InMemoryCryptoPublicStore
    val cryptoPrivateStore = new InMemoryCryptoPrivateStore(releaseProtocolVersion, loggerFactory)
    val privateCrypto =
      new SymbolicPrivateCrypto(pureCrypto, cryptoPrivateStore, timeouts, loggerFactory)

    // Conversion to java keys is not supported by symbolic crypto
    val javaKeyConverter = new JavaKeyConverter {
      override def toJava(
          publicKey: PublicKey
      ): Either[JavaKeyConversionError, (AlgorithmIdentifier, JPublicKey)] =
        throw new UnsupportedOperationException(
          "Symbolic crypto does not support conversion to java keys"
        )

      override def fromJavaSigningKey(
          publicKey: JPublicKey,
          algorithmIdentifier: AlgorithmIdentifier,
          fingerprint: Fingerprint,
      ): Either[JavaKeyConversionError, SigningPublicKey] =
        throw new UnsupportedOperationException(
          "Symbolic crypto does not support conversion to java keys"
        )

      override def fromJavaEncryptionKey(
          publicKey: JPublicKey,
          algorithmIdentifier: AlgorithmIdentifier,
          fingerprint: Fingerprint,
      ): Either[JavaKeyConversionError, EncryptionPublicKey] =
        throw new UnsupportedOperationException(
          "Symbolic crypto does not support conversion to java keys"
        )
    }

    new Crypto(
      pureCrypto,
      privateCrypto,
      cryptoPrivateStore,
      cryptoPublicStore,
      javaKeyConverter,
      timeouts,
      loggerFactory,
    )
  }

  /** Create symbolic crypto and pre-populate with keys using the given fingerprint suffixes, which will be prepended with the type of key (sigK, encK), and the fingerprints used for signing keys. */
  def tryCreate(
      signingFingerprints: Seq[Fingerprint],
      fingerprintSuffixes: Seq[String],
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  ): Crypto = {
    import com.digitalasset.canton.tracing.TraceContext.Implicits.Empty.*
    implicit val loggingContext: ErrorLoggingContext =
      ErrorLoggingContext.fromTracedLogger(loggerFactory.getTracedLogger(this.getClass))

    val crypto =
      SymbolicCrypto.create(releaseProtocolVersion, timeouts, loggerFactory)
    val cryptoPrivateStore = crypto.cryptoPrivateStore.toExtended
      .getOrElse(
        throw new RuntimeException(s"Crypto private store does not implement all necessary methods")
      )
    def runStorage[A](op: EitherT[Future, _, A], description: String): A =
      timeouts.io
        .await(s"storing $description")(op.value)
        .valueOr(err => throw new RuntimeException(s"Failed to store $description: $err"))

    // Create a keypair for each signing fingerprint
    signingFingerprints.foreach { k =>
      val sigPrivKey = SymbolicCrypto.signingPrivateKey(k)
      val sigPubKey = SymbolicCrypto.signingPublicKey(k)
      runStorage(
        cryptoPrivateStore.storeSigningKey(sigPrivKey, None),
        s"private signing key for $k",
      )
      runStorage(
        crypto.cryptoPublicStore.storeSigningKey(sigPubKey),
        s"public signing key for $k",
      )
    }

    // For the fingerprint suffixes, create both encryption and signing keys with a `encK-` or `sigK-` prefix.
    fingerprintSuffixes.foreach { k =>
      val sigKeyId = s"sigK-$k"
      val sigPrivKey = SymbolicCrypto.signingPrivateKey(sigKeyId)
      val sigPubKey = SymbolicCrypto.signingPublicKey(sigKeyId)

      val encKeyId = s"encK-$k"
      val encPrivKey = SymbolicCrypto.encryptionPrivateKey(encKeyId)
      val encPubKey = SymbolicCrypto.encryptionPublicKey(encKeyId)

      runStorage(
        cryptoPrivateStore.storeSigningKey(sigPrivKey, None),
        s"private signing key for $k",
      )
      runStorage(
        crypto.cryptoPublicStore.storeSigningKey(sigPubKey),
        s"public signign key for $k",
      )
      runStorage(
        cryptoPrivateStore.storeDecryptionKey(encPrivKey, None),
        s"decryption key for $k",
      )
      runStorage(
        crypto.cryptoPublicStore.storeEncryptionKey(encPubKey),
        s"encryption key for $k",
      )
    }

    crypto
  }

}
