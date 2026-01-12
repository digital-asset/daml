// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.health.ComponentHealthState
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

/** Wraps the [[CryptoPrivateApi]] to include static synchronizer parameters, ensuring that during
  * asymmetric decryption, the static synchronizer parameters are explicitly checked. This is
  * crucial because a malicious counter participant could potentially use a downgraded scheme. For
  * other methods, such as key generation, or signing by this (honest) participant, we rely on the
  * synchronizer handshake to ensure that only supported schemes within the synchronizer are used.
  */
final class SynchronizerCryptoPrivateApi(
    override val staticSynchronizerParameters: StaticSynchronizerParameters,
    privateCrypto: CryptoPrivateApi,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends CryptoPrivateApi
    with SynchronizerCryptoValidation
    with NamedLogging {

  override private[crypto] def getInitialHealthState: ComponentHealthState =
    privateCrypto.getInitialHealthState

  override def decrypt[M](
      encrypted: AsymmetricEncrypted[M]
  )(
      deserialize: ByteString => Either[DeserializationError, M]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, DecryptionError, M] =
    for {
      _ <- checkDecryption(
        keyFormatO = None,
        keySpecO = None,
        encrypted.encryptionAlgorithmSpec,
      )
        .toEitherT[FutureUnlessShutdown]
      res <- privateCrypto.decrypt(encrypted)(deserialize)
    } yield res

  override def encryptionSchemes: EncryptionCryptoSchemes = privateCrypto.encryptionSchemes

  override def generateEncryptionKey(
      keySpec: EncryptionKeySpec,
      name: Option[KeyName],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, EncryptionKeyGenerationError, EncryptionPublicKey] =
    privateCrypto.generateEncryptionKey(keySpec)

  override def name: String = privateCrypto.name

  override protected def initialHealthState: ComponentHealthState = getInitialHealthState

  override def signingSchemes: SigningCryptoSchemes = privateCrypto.signingSchemes

  override def signBytes(
      bytes: ByteString,
      signingKeyId: Fingerprint,
      usage: NonEmpty[Set[SigningKeyUsage]],
      signingAlgorithmSpec: SigningAlgorithmSpec,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, SigningError, Signature] =
    privateCrypto.signBytes(bytes, signingKeyId, usage, signingAlgorithmSpec)

  override def generateSigningKey(
      keySpec: SigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]],
      name: Option[KeyName],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SigningKeyGenerationError, SigningPublicKey] =
    privateCrypto.generateSigningKey(keySpec, usage, name)
}
