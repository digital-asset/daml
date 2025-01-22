// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{
  Hash,
  HashPurpose,
  Signature,
  SignatureCheckError,
  SigningKeyUsage,
  SyncCryptoError,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  SignedMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

class CantonCryptoProvider(cryptoApi: SynchronizerSnapshotSyncCryptoApi)(implicit
    ec: ExecutionContext
) extends CryptoProvider[PekkoEnv] {
  override def sign(
      hash: Hash,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Either[SyncCryptoError, Signature]] =
    PekkoFutureUnlessShutdown("sign", cryptoApi.sign(hash, usage).value)

  override def signMessage[MessageT <: ProtocolVersionedMemoizedEvidence & MessageFrom](
      message: MessageT,
      hashPurpose: HashPurpose,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Either[SyncCryptoError, SignedMessage[MessageT]]] =
    PekkoFutureUnlessShutdown(
      "signMessage",
      (
        for {
          signature <- cryptoApi.sign(
            CryptoProvider.hashForMessage(message, message.from, hashPurpose),
            usage,
          )
        } yield SignedMessage(message, signature)
      ).value,
    )

  override def verifySignature(hash: Hash, member: SequencerId, signature: Signature)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Either[SignatureCheckError, Unit]] = PekkoFutureUnlessShutdown(
    "verifying signature",
    cryptoApi.verifySignature(hash, member, signature).value,
  )
}
