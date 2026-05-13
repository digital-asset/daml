// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.crypto

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.topology.SequencerNodeId
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
      () => timeCrypto(metrics, cryptoApi.sign(hash, BftOrderingSigningKeyUsage).value, operationId),
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
              cryptoApi.sign(hash, BftOrderingSigningKeyUsage).value,
              operationId = s"sign-$authenticatedMessageType",
            )
        } yield signature.map(SignedMessage(message, _)),
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
              metrics,
              cryptoApi
                .verifySignature(hash, sequencerNodeId, signature, BftOrderingSigningKeyUsage)
                .value,
              operationId,
            )
        },
    )
}
