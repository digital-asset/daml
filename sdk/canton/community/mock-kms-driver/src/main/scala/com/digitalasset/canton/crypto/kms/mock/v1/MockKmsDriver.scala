// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.mock.v1

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.crypto.kms.driver.api.v1.*
import com.digitalasset.canton.crypto.kms.driver.api.v1.KmsDriverHealth.Ok
import com.digitalasset.canton.crypto.kms.driver.v1.KmsDriverSpecsConverter
import com.digitalasset.canton.crypto.kms.mock.audit.MockKmsRequestResponseLogger
import com.digitalasset.canton.crypto.{
  AsymmetricEncrypted,
  Crypto,
  Encrypted,
  EncryptionPublicKey,
  Fingerprint,
  KeyName,
  SigningKeyUsage,
  SigningPublicKey,
  SymmetricKey,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{DelayUtil, EitherTUtil}
import com.google.protobuf.ByteString
import io.opentelemetry.context.Context

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** Mock KMS Driver that wraps software-based cryptography using JCE.
  *
  * NOTE: This is a mock implementation and should not be used in production. The driver does not
  * provide persistence of keys.
  */
class MockKmsDriver(
    config: MockKmsDriverConfig,
    crypto: Crypto,
    override val supportedSigningKeySpecs: Set[SigningKeySpec],
    override val supportedSigningAlgoSpecs: Set[SigningAlgoSpec],
    override val supportedEncryptionKeySpecs: Set[EncryptionKeySpec],
    override val supportedEncryptionAlgoSpecs: Set[EncryptionAlgoSpec],
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends KmsDriver
    with NamedLogging {

  private lazy val loggerKms =
    new MockKmsRequestResponseLogger(config.auditLogging, NamedLoggerFactory.root)

  // The canton crypto private store does not store symmetric keys, therefore we store them in this map
  private lazy val symmetricKeys: TrieMap[String, SymmetricKey] = TrieMap.empty

  private def getSymmetricKey(keyId: String): EitherT[FutureUnlessShutdown, String, SymmetricKey] =
    symmetricKeys
      .get(keyId)
      .toRight(s"Symmetric key not found: $keyId")
      .toEitherT[FutureUnlessShutdown]

  private def mapErr[A](operation: String)(
      result: => EitherT[FutureUnlessShutdown, String, A]
  )(implicit traceContext: TraceContext): Future[A] =
    EitherTUtil.toFuture {
      loggerKms
        .withLogging(
          s"KMS operation: $operation",
          (a: A) => s"KMS operation `$operation` succeeded with result: $a",
        )(result)
        .failOnShutdownToAbortException(operation)
        .leftMap { err =>
          KmsDriverException(
            new RuntimeException(s"KMS operation `$operation` failed: $err"),
            // JCE crypto provider does not fail due to transient errors like network issues in cloud KMSs
            retryable = false,
          )
        }
    }

  private def applySigningLatency(
      fingerprint: Fingerprint
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = for {
    // Get the metadata for the public key
    publicKey <- crypto.cryptoPublicStore
      .signingKeyWithName(fingerprint)
      .toRight(s"Signing key not found: $fingerprint")

    // Find the latency based on the key's name
    latencyO = publicKey.name.flatMap(config.signingLatencies.get)

    _ = latencyO.foreach(latency =>
      logger.debug(s"Applying signing latency $latency for key $fingerprint")
    )

    // Apply the latency by returning a delayed future
    _ <- latencyO
      .map { latency =>
        EitherT.right[String] {
          FutureUnlessShutdown.outcomeF {
            DelayUtil.delay(latency.underlying)
          }
        }
      }
      .getOrElse(EitherTUtil.unitUS[String])
  } yield ()

  override def health: Future[KmsDriverHealth] = Future.successful(Ok)

  override def generateSigningKeyPair(signingKeySpec: SigningKeySpec, keyName: Option[String])(
      traceContext: Context
  ): Future[String] =
    TraceContext.withOpenTelemetryContext(traceContext) { implicit tc: TraceContext =>
      mapErr("generate signing keypair") {
        for {
          _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
            supportedSigningKeySpecs.contains(signingKeySpec),
            s"Unsupported signing key spec: $signingKeySpec",
          )
          keySpec = KmsDriverSpecsConverter.convertToCryptoSigningKeySpec(signingKeySpec)
          name <- keyName.traverse(KeyName.create).toEitherT[FutureUnlessShutdown]
          // We do not have the concept of key usage in the KMS layer, so we use `All` by default for generation and signing
          publicKey <- crypto
            .generateSigningKey(keySpec, SigningKeyUsage.All, name)
            .leftMap(err => s"Generate signing key failed: $err")
        } yield publicKey.id.unwrap
      }
    }

  override def generateEncryptionKeyPair(
      encryptionKeySpec: EncryptionKeySpec,
      keyName: Option[String],
  )(traceContext: Context): Future[String] =
    TraceContext.withOpenTelemetryContext(traceContext) { implicit tc: TraceContext =>
      mapErr("generate encryption keypair") {
        for {
          _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
            supportedEncryptionKeySpecs.contains(encryptionKeySpec),
            s"Unsupported encryption key spec: $encryptionKeySpec",
          )
          keySpec = KmsDriverSpecsConverter.convertToCryptoEncryptionKeySpec(encryptionKeySpec)
          name <- keyName.traverse(KeyName.create).toEitherT[FutureUnlessShutdown]
          publicKey <- crypto
            .generateEncryptionKey(keySpec, name)
            .leftMap(err => s"Generate signing key failed: $err")
        } yield publicKey.id.unwrap
      }
    }

  override def generateSymmetricKey(keyName: Option[String])(
      traceContext: Context
  ): Future[String] =
    TraceContext.withOpenTelemetryContext(traceContext) { implicit tc: TraceContext =>
      mapErr("generate symmetric key") {
        for {
          symmetricKey <- crypto.pureCrypto
            .generateSymmetricKey()
            .leftMap(err => s"Generate symmetric key failed: $err")
            .toEitherT[FutureUnlessShutdown]
          // We compute a hash of the symmetric key as the key id
          keyId = Fingerprint.create(symmetricKey.key).unwrap
          _ = symmetricKeys.put(keyId, symmetricKey)
        } yield keyId
      }
    }

  override def sign(data: Array[Byte], keyId: String, algoSpec: SigningAlgoSpec)(
      traceContext: Context
  ): Future[Array[Byte]] =
    TraceContext.withOpenTelemetryContext(traceContext) { implicit tc: TraceContext =>
      mapErr("sign") {
        for {
          _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
            supportedSigningAlgoSpecs.contains(algoSpec),
            s"Unsupported signing algorithm spec: $algoSpec",
          )
          algo = KmsDriverSpecsConverter.convertToCryptoSigningAlgoSpec(algoSpec)
          fingerprint <- Fingerprint.fromString(keyId).toEitherT[FutureUnlessShutdown]
          // For testing, we may apply a signing latency before actually signing with the key
          _ <- applySigningLatency(fingerprint)
          signature <- crypto.privateCrypto
            .signBytes(ByteString.copyFrom(data), fingerprint, SigningKeyUsage.All, algo)
            .leftMap(err => s"Failed to sign with key $fingerprint: $err")
        } yield signature.unwrap.toByteArray
      }
    }

  override def decryptAsymmetric(
      ciphertext: Array[Byte],
      keyId: String,
      algoSpec: EncryptionAlgoSpec,
  )(traceContext: Context): Future[Array[Byte]] =
    TraceContext.withOpenTelemetryContext(traceContext) { implicit tc: TraceContext =>
      mapErr("decrypt asymmetric") {
        for {
          _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
            supportedEncryptionAlgoSpecs.contains(algoSpec),
            s"Unsupported encryption algorithm spec: $algoSpec",
          )
          algo = KmsDriverSpecsConverter.convertToCryptoEncryptionAlgoSpec(algoSpec)
          fingerprint <- Fingerprint.fromString(keyId).toEitherT[FutureUnlessShutdown]
          encrypted = AsymmetricEncrypted(ByteString.copyFrom(ciphertext), algo, fingerprint)
          decrypted <- crypto.privateCrypto
            .decrypt(encrypted)(Right(_))
            .leftMap(err => s"Failed to decrypt with key $fingerprint: $err")
        } yield decrypted.toByteArray
      }
    }

  override def encryptSymmetric(data: Array[Byte], keyId: String)(
      traceContext: Context
  ): Future[Array[Byte]] =
    TraceContext.withOpenTelemetryContext(traceContext) { implicit tc: TraceContext =>
      mapErr("encrypt symmetric") {
        for {
          symmetricKey <- getSymmetricKey(keyId)
          ciphertext <- crypto.pureCrypto
            .encryptSymmetricWith(
              ByteString.copyFrom(data),
              symmetricKey,
            )
            .leftMap(err => s"Failed to encrypt with symmetric key $keyId: $err")
            .toEitherT[FutureUnlessShutdown]
        } yield ciphertext.toByteArray
      }
    }

  override def decryptSymmetric(ciphertext: Array[Byte], keyId: String)(
      traceContext: Context
  ): Future[Array[Byte]] =
    TraceContext.withOpenTelemetryContext(traceContext) { implicit tc: TraceContext =>
      mapErr("decrypt symmetric") {
        for {
          symmetricKey <- getSymmetricKey(keyId)
          encrypted = Encrypted(ByteString.copyFrom(ciphertext))
          plaintext <- crypto.pureCrypto
            .decryptWith(encrypted, symmetricKey)(Right(_))
            .leftMap(err => s"Failed to decrypt with symmetric key $keyId: $err")
            .toEitherT[FutureUnlessShutdown]
        } yield plaintext.toByteArray
      }
    }

  override def getPublicKey(keyId: String)(traceContext: Context): Future[PublicKey] =
    TraceContext.withOpenTelemetryContext(traceContext) { implicit tc: TraceContext =>
      mapErr(loggerKms.getPublicKeyRequestMsg(keyId)) {
        for {
          fingerprint <- Fingerprint.fromString(keyId).toEitherT[FutureUnlessShutdown]
          publicKey <- crypto.cryptoPublicStore
            .publicKey(fingerprint)
            .toRight(s"Key not found: $keyId")
          keySpec <- publicKey match {
            case EncryptionPublicKey(_, _, keySpec) =>
              KmsDriverSpecsConverter
                .convertToDriverEncryptionKeySpec(keySpec)
                .toEitherT[FutureUnlessShutdown]
            case SigningPublicKey(_, _, keySpec, _, _) =>
              KmsDriverSpecsConverter
                .convertToDriverSigningKeySpec(keySpec)
                .toEitherT[FutureUnlessShutdown]
            case _ =>
              EitherT.leftT[FutureUnlessShutdown, KeySpec](
                s"Unsupported public key type: $publicKey"
              )
          }
        } yield PublicKey(publicKey.key.toByteArray, keySpec)
      }
    }

  override def keyExistsAndIsActive(keyId: String)(traceContext: Context): Future[Unit] =
    if (symmetricKeys.contains(keyId))
      Future.unit
    else
      getPublicKey(keyId)(traceContext).map(_ => ())

  override def deleteKey(keyId: String)(traceContext: Context): Future[Unit] =
    if (symmetricKeys.contains(keyId))
      Future.successful(symmetricKeys.remove(keyId).discard)
    else
      TraceContext.withOpenTelemetryContext(traceContext) { implicit tc: TraceContext =>
        mapErr("delete key") {
          for {
            fingerprint <- Fingerprint.fromString(keyId).toEitherT[FutureUnlessShutdown]
            _ <- crypto.cryptoPrivateStore
              .removePrivateKey(fingerprint)
              .leftMap(err => s"Failed to delete key: $err")
            _ <- EitherT.right(crypto.cryptoPublicStore.deleteKey(fingerprint))
          } yield ()
        }
      }

  override def close(): Unit = crypto.close()
}
