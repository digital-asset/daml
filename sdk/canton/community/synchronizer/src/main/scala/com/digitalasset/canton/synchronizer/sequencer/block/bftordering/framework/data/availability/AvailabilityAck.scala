// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability

import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose, Signature, v30}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.FingerprintKeyId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.AvailabilityAck.ValidationError
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.AvailabilityAck as ProtoAvailabilityAck

final case class AvailabilityAck(from: BftNodeId, signature: Signature) {

  def validateIn(currentOrderingTopology: OrderingTopology): Either[ValidationError, Unit] =
    for {
      _ <- Either.cond(
        currentOrderingTopology.contains(from),
        (),
        ValidationError.NodeNotInTopology,
      )
      keyId = FingerprintKeyId.toBftKeyId(signature.signedBy)
      _ <- Either.cond(
        currentOrderingTopology.nodesTopologyInfo
          .get(from)
          .map(_.keyIds)
          .getOrElse(Set.empty)
          .contains(keyId),
        (),
        ValidationError.KeyNotInTopology,
      )
    } yield ()
}

object AvailabilityAck {

  sealed trait ValidationError extends Product with Serializable
  object ValidationError {
    case object NodeNotInTopology extends ValidationError
    case object KeyNotInTopology extends ValidationError
  }

  def fromProto(
      ack: ProtoAvailabilityAck,
      signature: v30.Signature,
  ): ParsingResult[AvailabilityAck] =
    for {
      sig <- Signature.fromProtoV30(signature)
      originatingNode = BftNodeId(ack.from)
    } yield AvailabilityAck(originatingNode, sig)

  def hashFor(batchId: BatchId, epoch: EpochNumber, from: BftNodeId): Hash = Hash
    .build(
      HashPurpose.BftAvailabilityAck,
      HashAlgorithm.Sha256,
    )
    .add(batchId.getCryptographicEvidence)
    .add(epoch)
    .add(from)
    .finish()
}
