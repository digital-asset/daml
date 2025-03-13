// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
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

class CantonCryptoProvider(cryptoApi: SynchronizerSnapshotSyncCryptoApi)(implicit
    ec: ExecutionContext
) extends CryptoProvider[PekkoEnv] {

  override def signHash(
      hash: Hash
  )(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Either[SyncCryptoError, Signature]] =
    PekkoFutureUnlessShutdown(
      "sign",
      () => cryptoApi.sign(hash, SigningKeyUsage.ProtocolOnly).value,
    )

  override def signMessage[MessageT <: ProtocolVersionedMemoizedEvidence & MessageFrom](
      message: MessageT,
      autnenticatedMessageType: CryptoProvider.AuthenticatedMessageType,
  )(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Either[SyncCryptoError, SignedMessage[MessageT]]] =
    PekkoFutureUnlessShutdown(
      "signMessage",
      () =>
        (
          for {
            signature <- cryptoApi.sign(
              CryptoProvider.hashForMessage(message, message.from, autnenticatedMessageType),
              BftOrderingSigningKeyUsage,
            )
          } yield SignedMessage(message, signature)
        ).value,
    )

  override def verifySignature(
      hash: Hash,
      node: BftNodeId,
      signature: Signature,
  )(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Either[SignatureCheckError, Unit]] =
    PekkoFutureUnlessShutdown(
      "verifying signature",
      () =>
        SequencerNodeId.fromBftNodeId(node) match {
          case Left(error) =>
            FutureUnlessShutdown.pure(Left(SignatureCheckError.SignerHasNoValidKeys(error.message)))
          case Right(sequencerNodeId) =>
            cryptoApi
              .verifySignature(hash, sequencerNodeId, signature, BftOrderingSigningKeyUsage)
              .value
        },
    )
}

object CantonCryptoProvider {

  private[driver] val BftOrderingSigningKeyUsage: NonEmpty[Set[SigningKeyUsage]] =
    SigningKeyUsage.ProtocolOnly
}
