// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver

import cats.data.EitherT
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.CantonCryptoProvider.BftOrderingSigningKeyUsage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  SignedMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

class CantonCryptoProvider(
    cryptoApi: SynchronizerSnapshotSyncCryptoApi,
    metrics: BftOrderingMetrics,
)(implicit ec: ExecutionContext)
    extends CryptoProvider[PekkoEnv] {

  override def signHash(
      hash: Hash,
      operationId: String,
  )(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): PekkoFutureUnlessShutdown[Either[SyncCryptoError, Signature]] =
    PekkoFutureUnlessShutdown(
      "sign",
      () => timeCrypto(cryptoApi.sign(hash, BftOrderingSigningKeyUsage).value, operationId),
    )

  override def signMessage[MessageT <: ProtocolVersionedMemoizedEvidence & MessageFrom](
      message: MessageT,
      authenticatedMessageType: CryptoProvider.AuthenticatedMessageType,
  )(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): PekkoFutureUnlessShutdown[Either[SyncCryptoError, SignedMessage[MessageT]]] =
    PekkoFutureUnlessShutdown(
      "signMessage",
      () =>
        (
          for {
            signature <-
              EitherT[FutureUnlessShutdown, SyncCryptoError, Signature](
                timeCrypto(
                  cryptoApi
                    .sign(
                      CryptoProvider
                        .hashForMessage(message, message.from, authenticatedMessageType),
                      BftOrderingSigningKeyUsage,
                    )
                    .value,
                  operationId = s"sign-$authenticatedMessageType",
                )
              )
          } yield SignedMessage(message, signature)
        ).value,
    )

  override def verifySignature(
      hash: Hash,
      node: BftNodeId,
      signature: Signature,
      operationId: String,
  )(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): PekkoFutureUnlessShutdown[Either[SignatureCheckError, Unit]] =
    PekkoFutureUnlessShutdown(
      "verifying signature",
      () =>
        SequencerNodeId.fromBftNodeId(node) match {
          case Left(error) =>
            FutureUnlessShutdown.pure(Left(SignatureCheckError.SignerHasNoValidKeys(error.message)))
          case Right(sequencerNodeId) =>
            timeCrypto(
              cryptoApi
                .verifySignature(hash, sequencerNodeId, signature, BftOrderingSigningKeyUsage)
                .value,
              operationId,
            )
        },
    )

  private def timeCrypto[T](call: => FutureUnlessShutdown[T], operationId: String)(implicit
      metricsContext: MetricsContext
  ) =
    FutureUnlessShutdown(
      metrics.performance.orderingStageLatency.timer
        .timeFuture(call.unwrap)(
          metricsContext.withExtraLabels(
            metrics.performance.orderingStageLatency.labels.stages.Key -> operationId
          )
        )
    )
}

object CantonCryptoProvider {

  private[driver] val BftOrderingSigningKeyUsage: NonEmpty[Set[SigningKeyUsage]] =
    SigningKeyUsage.ProtocolOnly
}
