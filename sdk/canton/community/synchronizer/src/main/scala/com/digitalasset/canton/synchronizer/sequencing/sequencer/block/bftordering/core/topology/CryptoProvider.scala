// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.topology

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.{
  Hash,
  HashPurpose,
  Signature,
  SignatureCheckError,
  SigningKeyUsage,
  SyncCryptoError,
}
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  SignedMessage,
}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext

import CryptoProvider.hashForMessage

trait CryptoProvider[E <: Env[E]] {
  def sign(
      hash: Hash,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Either[SyncCryptoError, Signature]]

  def signMessage[MessageT <: ProtocolVersionedMemoizedEvidence & MessageFrom](
      message: MessageT,
      hashPurpose: HashPurpose,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Either[SyncCryptoError, SignedMessage[MessageT]]]

  def verifySignature(hash: Hash, member: SequencerId, signature: Signature)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Either[SignatureCheckError, Unit]]

  def verifySignedMessage[MessageT <: ProtocolVersionedMemoizedEvidence & MessageFrom](
      signedMessage: SignedMessage[MessageT],
      hashPurpose: HashPurpose,
  )(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Either[SignatureCheckError, Unit]] =
    verifySignature(
      hashForMessage(signedMessage.message, signedMessage.from, hashPurpose),
      signedMessage.from,
      signedMessage.signature,
    )
}

object CryptoProvider {
  def hashForMessage[MessageT <: ProtocolVersionedMemoizedEvidence](
      messageT: MessageT,
      from: SequencerId,
      hashPurpose: HashPurpose,
  ): Hash =
    Hash
      .build(hashPurpose, Sha256)
      .add(from.toString)
      .add(messageT.getCryptographicEvidence)
      .finish()
}
