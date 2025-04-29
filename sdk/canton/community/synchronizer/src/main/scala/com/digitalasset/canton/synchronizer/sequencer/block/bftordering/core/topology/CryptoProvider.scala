// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology

import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider.{
  AuthenticatedMessageType,
  hashForMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  SignedMessage,
}
import com.digitalasset.canton.tracing.TraceContext

trait CryptoProvider[E <: Env[E]] {

  def signHash(
      hash: Hash
  )(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Either[SyncCryptoError, Signature]]

  def signMessage[MessageT <: ProtocolVersionedMemoizedEvidence & MessageFrom](
      message: MessageT,
      authenticatedMessageType: AuthenticatedMessageType,
  )(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Either[SyncCryptoError, SignedMessage[MessageT]]]

  def verifySignature(
      hash: Hash,
      member: BftNodeId,
      signature: Signature,
  )(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Either[SignatureCheckError, Unit]]

  def verifySignedMessage[MessageT <: ProtocolVersionedMemoizedEvidence & MessageFrom](
      signedMessage: SignedMessage[MessageT],
      authenticatedMessageType: AuthenticatedMessageType,
  )(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Either[SignatureCheckError, Unit]] =
    verifySignature(
      hashForMessage(signedMessage.message, signedMessage.from, authenticatedMessageType),
      signedMessage.from,
      signedMessage.signature,
    )
}

object CryptoProvider {

  sealed trait AuthenticatedMessageType extends Product
  object AuthenticatedMessageType {
    case object BftOrderingPbftBlock extends AuthenticatedMessageType
    case object BftAvailabilityAck extends AuthenticatedMessageType
    case object BftBatchId extends AuthenticatedMessageType
    case object BftSignedAvailabilityMessage extends AuthenticatedMessageType
    case object BftSignedConsensusMessage extends AuthenticatedMessageType
    case object BftSignedStateTransferMessage extends AuthenticatedMessageType
    case object BftSignedRetransmissionMessage extends AuthenticatedMessageType
  }

  def hashForMessage[MessageT <: ProtocolVersionedMemoizedEvidence](
      messageT: MessageT,
      from: BftNodeId,
      authenticatedMessageType: AuthenticatedMessageType,
  ): Hash =
    Hash
      .build(toHashPurpose(authenticatedMessageType), Sha256)
      .add(from)
      .add(messageT.getCryptographicEvidence)
      .finish()

  private def toHashPurpose(
      authenticatedMessageType: AuthenticatedMessageType
  ): HashPurpose =
    authenticatedMessageType match {
      case AuthenticatedMessageType.BftOrderingPbftBlock => HashPurpose.BftOrderingPbftBlock
      case AuthenticatedMessageType.BftAvailabilityAck => HashPurpose.BftAvailabilityAck
      case AuthenticatedMessageType.BftBatchId => HashPurpose.BftBatchId
      case AuthenticatedMessageType.BftSignedAvailabilityMessage =>
        HashPurpose.BftSignedAvailabilityMessage
      case AuthenticatedMessageType.BftSignedConsensusMessage =>
        HashPurpose.BftSignedConsensusMessage
      case AuthenticatedMessageType.BftSignedStateTransferMessage =>
        HashPurpose.BftSignedStateTransferMessage
      case AuthenticatedMessageType.BftSignedRetransmissionMessage =>
        HashPurpose.BftSignedRetransmissionMessage
    }
}

final case class DelegationCryptoProvider[E <: Env[E]](
    signer: CryptoProvider[E],
    verifier: CryptoProvider[E],
) extends CryptoProvider[E] {
  override def signHash(hash: Hash)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Either[SyncCryptoError, Signature]] =
    signer.signHash(hash)

  override def signMessage[MessageT <: ProtocolVersionedMemoizedEvidence & MessageFrom](
      message: MessageT,
      authenticatedMessageType: AuthenticatedMessageType,
  )(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Either[SyncCryptoError, SignedMessage[MessageT]]] =
    signer.signMessage(message, authenticatedMessageType)

  override def verifySignature(hash: Hash, member: BftNodeId, signature: Signature)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Either[SignatureCheckError, Unit]] =
    verifier.verifySignature(hash, member, signature)
}
