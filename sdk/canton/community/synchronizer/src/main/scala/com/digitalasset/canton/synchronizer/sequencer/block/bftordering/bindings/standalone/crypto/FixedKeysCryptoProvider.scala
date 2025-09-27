// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.standalone.crypto

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.crypto.{
  CryptoPureApi,
  Hash,
  Signature,
  SignatureCheckError,
  SigningPrivateKey,
  SigningPublicKey,
  SyncCryptoError,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider.{
  BftOrderingSigningKeyUsage,
  hashForMessage,
  timeCrypto,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  SignedMessage,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

class FixedKeysCryptoProvider(
    privKey: SigningPrivateKey,
    pubKeys: Map[BftNodeId, SigningPublicKey],
    cryptoApi: CryptoPureApi,
    metrics: BftOrderingMetrics,
)(implicit executionContext: ExecutionContext)
    extends CryptoProvider[PekkoEnv] {

  override def signHash(hash: Hash, operationId: String)(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): PekkoModuleSystem.PekkoFutureUnlessShutdown[Either[SyncCryptoError, Signature]] =
    PekkoFutureUnlessShutdown(
      "sign",
      () =>
        timeCrypto(
          metrics,
          signHash(hash),
          operationId,
        ),
    )

  override def signMessage[MessageT <: ProtocolVersionedMemoizedEvidence & MessageFrom](
      message: MessageT,
      authenticatedMessageType: CryptoProvider.AuthenticatedMessageType,
  )(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): PekkoModuleSystem.PekkoFutureUnlessShutdown[Either[SyncCryptoError, SignedMessage[MessageT]]] =
    PekkoFutureUnlessShutdown(
      "signMessage",
      () =>
        for {
          hash <-
            timeCrypto(
              metrics,
              FutureUnlessShutdown.outcomeF(
                Future(hashForMessage(message, message.from, authenticatedMessageType))
              ),
              operationId = s"hash-$authenticatedMessageType",
            )
          signature <-
            timeCrypto(
              metrics,
              signHash(hash),
              operationId = s"sign-$authenticatedMessageType",
            )
        } yield signature.map(SignedMessage(message, _)),
    )

  override def verifySignature(
      hash: Hash,
      member: BftNodeId,
      signature: Signature,
      operationId: String,
  )(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): PekkoModuleSystem.PekkoFutureUnlessShutdown[Either[SignatureCheckError, Unit]] =
    PekkoFutureUnlessShutdown(
      "verifying signature",
      () =>
        timeCrypto(
          metrics,
          FutureUnlessShutdown.outcomeF(
            Future(
              cryptoApi
                .verifySignature(hash, pubKeys(member), signature, BftOrderingSigningKeyUsage)
            )
          ),
          operationId,
        ),
    )

  private def signHash(
      hash: Hash
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[SyncCryptoError.SyncCryptoSigningError, Signature]] =
    FutureUnlessShutdown
      .outcomeF(Future(cryptoApi.sign(hash, privKey, BftOrderingSigningKeyUsage)))
      .map {
        case Right(sig) => Right(sig)
        case Left(err) => Left(SyncCryptoError.SyncCryptoSigningError(err))
      }
}
