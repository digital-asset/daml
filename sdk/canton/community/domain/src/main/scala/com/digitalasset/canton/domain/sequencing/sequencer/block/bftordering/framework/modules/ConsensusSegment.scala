// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules

import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1.{
  Commit as ProtoCommit,
  ConsensusCertificate as ProtoConsensusCertificate,
  ConsensusMessage as ProtoConsensusMessage,
  NewView as ProtoNewView,
  PrePrepare as ProtoPrePrepare,
  Prepare as ProtoPrepare,
  ViewChange as ProtoViewChange,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.availability.{
  OrderingBlock,
  ProofOfAvailability,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  ConsensusCertificate,
  OrderedBlock,
  PrepareCertificate,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PbftNetworkMessage.headerFromProto
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{SequencerId, UniqueIdentifier}
import com.digitalasset.canton.version.ProtocolVersion

object ConsensusSegment {

  sealed trait Message extends Product with Serializable

  final case object Start extends Message

  /** Messages only sent to itself via pipe-to-self
    */
  sealed trait Internal extends Message
  object Internal {
    final case class OrderedBlockStored(
        block: OrderedBlock,
        commits: Seq[SignedMessage[ConsensusSegment.ConsensusMessage.Commit]],
        viewNumber: ViewNumber,
    ) extends Message

    final case class AsyncException(e: Throwable) extends Message
  }

  sealed trait ConsensusMessage extends Message
  object ConsensusMessage {
    final case class BlockProposal(orderingBlock: OrderingBlock, epochNumber: EpochNumber)
        extends ConsensusMessage

    sealed trait PbftEvent extends ConsensusMessage {
      def blockMetadata: BlockMetadata
      def viewNumber: ViewNumber
    }

    sealed trait PbftTimeout extends PbftEvent
    final case class PbftNormalTimeout(blockMetadata: BlockMetadata, viewNumber: ViewNumber)
        extends PbftTimeout
    final case class PbftNestedViewChangeTimeout(
        blockMetadata: BlockMetadata,
        viewNumber: ViewNumber,
    ) extends PbftTimeout

    /** Pbft consensus messages coming from the network
      * The order of messages below correspond to the protocol steps
      */
    sealed trait PbftNetworkMessage {
      def blockMetadata: BlockMetadata
      def viewNumber: ViewNumber
      def localTimestamp: CantonTimestamp
      def from: SequencerId
      def toProto: ProtoConsensusMessage
      // TODO(i18194) add signatures
    }

    /** A Signed Pbft consensus message that is an actual event that the consensus module
      * will act upon
      */
    final case class PbftSignedNetworkMessage(message: SignedMessage[PbftNetworkMessage])
        extends PbftEvent {
      override def blockMetadata: BlockMetadata = message.message.blockMetadata

      override def viewNumber: ViewNumber = message.message.viewNumber
    }

    sealed trait PbftNormalCaseMessage extends PbftNetworkMessage {
      def hash: Hash
    }
    sealed trait PbftViewChangeMessage extends PbftNetworkMessage

    object PbftNetworkMessage {
      final case class Header(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          from: SequencerId,
          timestamp: CantonTimestamp,
      )
      def headerFromProto(
          message: ProtoConsensusMessage
      ): ParsingResult[Header] =
        for {
          protoBlockMetadata <- ProtoConverter
            .required("blockMetadata", message.blockMetadata)
          blockMetadata <- BlockMetadata.fromProto(protoBlockMetadata)
          viewNumber = ViewNumber(message.viewNumber)
          from <- UniqueIdentifier
            .fromProtoPrimitive(message.fromSequencerUid, "from_sequencer_uid")
            .map(SequencerId(_))
          timestamp <- CantonTimestamp
            .fromProtoPrimitive(message.localTimestamp)
        } yield Header(blockMetadata, viewNumber, from, timestamp)
    }

    final case class PrePrepare private (
        blockMetadata: BlockMetadata,
        viewNumber: ViewNumber,
        localTimestamp: CantonTimestamp,
        block: OrderingBlock,
        canonicalCommitSet: CanonicalCommitSet,
        from: SequencerId,
    ) extends PbftNormalCaseMessage {

      lazy val hash: Hash = {
        val builder = Hash
          .build(HashPurpose.BftOrderingPbftBlock, HashAlgorithm.Sha256)
          .add(blockMetadata.blockNumber)
          .add(blockMetadata.epochNumber)
          .add(localTimestamp.toMicros)
          .add(from.toString)
          .add(block.proofs.size)
          .add(canonicalCommitSet.sortedCommits.size)

        block.proofs
          .foreach { proof =>
            builder.add(proof.batchId.hash.getCryptographicEvidence)
            builder.add(proof.acks.size)
            proof.acks.foreach { ack =>
              builder.add(ack.from.toString)
              // TODO(#17337): We should probably not rely on Protobuf when calculating hashes (due to a potential non-determinism).
              builder.add(ack.signature.toByteString(ProtocolVersion.dev))
            }
          }

        canonicalCommitSet.sortedCommits.foreach { commit =>
          builder.add(commit.message.blockMetadata.blockNumber)
          builder.add(commit.message.blockMetadata.epochNumber)
          builder.add(commit.message.hash.getCryptographicEvidence)
          builder.add(commit.message.localTimestamp.toMicros)
        }

        builder.finish()
      }

      override def toProto: ProtoConsensusMessage =
        ProtoConsensusMessage.of(
          Some(blockMetadata.toProto),
          viewNumber,
          from.uid.toProtoPrimitive,
          localTimestamp.toMicros,
          ProtoConsensusMessage.Message.PrePrepare(
            ProtoPrePrepare.of(
              Some(block.toProto),
              Some(canonicalCommitSet.toProto),
            )
          ),
        )
    }

    object PrePrepare {
      def create(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          localTimestamp: CantonTimestamp,
          block: OrderingBlock,
          canonicalCommitSet: CanonicalCommitSet,
          from: SequencerId,
      ): PrePrepare =
        PrePrepare(blockMetadata, viewNumber, localTimestamp, block, canonicalCommitSet, from)

      def fromProtoConsensusMessage(
          value: ProtoConsensusMessage
      ): ParsingResult[PrePrepare] =
        for {
          header <- headerFromProto(value)
          protoPrePrepare <- value.message.prePrepare.toRight(
            ProtoDeserializationError.OtherError("Not a PrePrepare message")
          )
          prePrepare <- ConsensusSegment.ConsensusMessage.PrePrepare.fromProto(
            header.blockMetadata,
            header.viewNumber,
            header.timestamp,
            protoPrePrepare,
            header.from,
          )
        } yield prePrepare

      def fromProto(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          timestamp: CantonTimestamp,
          prePrepare: ProtoPrePrepare,
          from: SequencerId,
      ): ParsingResult[PrePrepare] =
        for {
          protoCanonicalCommitSet <- ProtoConverter
            .required("bftTimeCanonicalCommitSet", prePrepare.bftTimeCanonicalCommitSet)
          canonicalCommitSet <- CanonicalCommitSet.fromProto(protoCanonicalCommitSet)
          proofs <- prePrepare.block match {
            case Some(block) => block.proofs.traverse(ProofOfAvailability.fromProto)
            case None =>
              Left(ProtoDeserializationError.OtherError("Pre-prepare with no ordering block"))
          }
        } yield ConsensusSegment.ConsensusMessage.PrePrepare(
          blockMetadata,
          viewNumber,
          timestamp,
          OrderingBlock(proofs),
          canonicalCommitSet,
          from,
        )
    }

    final case class Prepare private (
        blockMetadata: BlockMetadata,
        viewNumber: ViewNumber,
        hash: Hash,
        localTimestamp: CantonTimestamp,
        from: SequencerId,
    ) extends PbftNormalCaseMessage {
      override def toProto: ProtoConsensusMessage =
        ProtoConsensusMessage.of(
          Some(blockMetadata.toProto),
          viewNumber,
          from.uid.toProtoPrimitive,
          localTimestamp.toMicros,
          ProtoConsensusMessage.Message.Prepare(
            ProtoPrepare.of(
              hash.getCryptographicEvidence
            )
          ),
        )
    }

    object Prepare {
      implicit val ordering: Ordering[Prepare] =
        Ordering.by(prepare => (prepare.from, prepare.localTimestamp))

      def create(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          hash: Hash,
          localTimestamp: CantonTimestamp,
          from: SequencerId,
      ): Prepare =
        Prepare(blockMetadata, viewNumber, hash, localTimestamp, from)

      def fromProtoConsensusMessage(
          value: ProtoConsensusMessage
      ): ParsingResult[Prepare] =
        for {
          header <- headerFromProto(value)
          protoPrepare <- value.message.prepare.toRight(
            ProtoDeserializationError.OtherError("Not a Prepare message")
          )
          prepare <- Prepare.fromProto(
            header.blockMetadata,
            header.viewNumber,
            header.timestamp,
            protoPrepare,
            header.from,
          )
        } yield prepare

      def fromProto(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          timestamp: CantonTimestamp,
          prepare: ProtoPrepare,
          from: SequencerId,
      ): ParsingResult[Prepare] =
        for {
          hash <- Hash.fromProtoPrimitive(prepare.blockHash)
        } yield ConsensusSegment.ConsensusMessage.Prepare(
          blockMetadata,
          viewNumber,
          hash,
          timestamp,
          from,
        )
    }

    final case class Commit private (
        blockMetadata: BlockMetadata,
        viewNumber: ViewNumber,
        hash: Hash,
        localTimestamp: CantonTimestamp,
        from: SequencerId,
    ) extends PbftNormalCaseMessage {
      override def toProto: ProtoConsensusMessage =
        ProtoConsensusMessage.of(
          Some(blockMetadata.toProto),
          viewNumber,
          from.uid.toProtoPrimitive,
          localTimestamp.toMicros,
          ProtoConsensusMessage.Message.Commit(
            ProtoCommit.of(hash.getCryptographicEvidence)
          ),
        )
    }

    object Commit {
      implicit val ordering: Ordering[Commit] =
        Ordering.by(commit => (commit.from, commit.localTimestamp))

      def create(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          hash: Hash,
          localTimestamp: CantonTimestamp,
          from: SequencerId,
      ): Commit = Commit(blockMetadata, viewNumber, hash, localTimestamp, from)

      def fromProtoConsensusMessage(
          value: ProtoConsensusMessage
      ): ParsingResult[Commit] =
        for {
          header <- headerFromProto(value)
          protoCommit <- value.message.commit.toRight(
            ProtoDeserializationError.OtherError("Not a Commit message")
          )
          commit <- Commit.fromProto(
            header.blockMetadata,
            header.viewNumber,
            header.timestamp,
            protoCommit,
            header.from,
          )
        } yield commit

      def fromProto(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          timestamp: CantonTimestamp,
          commit: ProtoCommit,
          from: SequencerId,
      ): ParsingResult[Commit] =
        for {
          hash <- Hash.fromProtoPrimitive(commit.blockHash)
        } yield ConsensusSegment.ConsensusMessage.Commit(
          blockMetadata,
          viewNumber,
          hash,
          timestamp,
          from,
        )
    }

    final case class ViewChange private (
        blockMetadata: BlockMetadata,
        segmentIndex: Int,
        viewNumber: ViewNumber,
        localTimestamp: CantonTimestamp,
        consensusCerts: Seq[ConsensusCertificate],
        from: SequencerId,
    ) extends PbftViewChangeMessage {
      override def toProto: ProtoConsensusMessage =
        ProtoConsensusMessage.of(
          Some(blockMetadata.toProto),
          viewNumber,
          from.uid.toProtoPrimitive,
          localTimestamp.toMicros,
          ProtoConsensusMessage.Message.ViewChange(
            ProtoViewChange.of(
              segmentIndex,
              consensusCerts.map(certificate =>
                ProtoConsensusCertificate.of(certificate match {
                  case pc: PrepareCertificate =>
                    ProtoConsensusCertificate.Certificate.PrepareCertificate(pc.toProto)
                  case cc: CommitCertificate =>
                    ProtoConsensusCertificate.Certificate.CommitCertificate(cc.toProto)
                })
              ),
            )
          ),
        )
    }

    object ViewChange {
      def create(
          blockMetadata: BlockMetadata,
          segmentIndex: Int,
          viewNumber: ViewNumber,
          localTimestamp: CantonTimestamp,
          consensusCerts: Seq[ConsensusCertificate],
          from: SequencerId,
      ): ViewChange =
        ViewChange(blockMetadata, segmentIndex, viewNumber, localTimestamp, consensusCerts, from)

      def fromProtoConsensusMessage(
          value: ProtoConsensusMessage
      ): ParsingResult[ViewChange] =
        for {
          header <- headerFromProto(value)
          protoViewChange <- value.message.viewChange.toRight(
            ProtoDeserializationError.OtherError("Not a ViewChange message")
          )
          viewChange <- ViewChange.fromProto(
            header.blockMetadata,
            header.viewNumber,
            header.timestamp,
            protoViewChange,
            header.from,
          )
        } yield viewChange

      def fromProto(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          timestamp: CantonTimestamp,
          viewChange: ProtoViewChange,
          from: SequencerId,
      ): ParsingResult[ViewChange] =
        for {
          certs <- viewChange.consensusCerts.traverse(ConsensusCertificate.fromProto)
        } yield ConsensusSegment.ConsensusMessage.ViewChange(
          blockMetadata,
          viewChange.segmentIndex,
          viewNumber,
          timestamp,
          certs,
          from,
        )
    }

    final case class NewView private (
        blockMetadata: BlockMetadata,
        segmentIndex: Int,
        viewNumber: ViewNumber,
        localTimestamp: CantonTimestamp,
        viewChanges: Seq[SignedMessage[ViewChange]],
        prePrepares: Seq[SignedMessage[PrePrepare]],
        from: SequencerId,
    ) extends PbftViewChangeMessage {
      import NewView.*

      private lazy val sortedViewChanges: Seq[SignedMessage[ViewChange]] = viewChanges.sorted

      lazy val computedCertificatePerBlock = computeCertificatePerBlock(viewChanges.map(_.message))

      override def toProto: ProtoConsensusMessage =
        ProtoConsensusMessage.of(
          Some(blockMetadata.toProto),
          viewNumber,
          from.uid.toProtoPrimitive,
          localTimestamp.toMicros,
          ProtoConsensusMessage.Message.NewView(
            ProtoNewView.of(
              segmentIndex,
              sortedViewChanges.map(_.toProto),
              prePrepares.map(_.toProto),
            )
          ),
        )
    }

    @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
    object NewView {
      def create(
          blockMetadata: BlockMetadata,
          segmentIndex: Int,
          viewNumber: ViewNumber,
          localTimestamp: CantonTimestamp,
          viewChanges: Seq[SignedMessage[ViewChange]],
          prePrepares: Seq[SignedMessage[PrePrepare]],
          from: SequencerId,
      ): NewView = NewView(
        blockMetadata,
        segmentIndex,
        viewNumber,
        localTimestamp,
        viewChanges,
        prePrepares,
        from,
      )
      implicit val ordering: Ordering[ViewChange] =
        Ordering.by(viewChange => (viewChange.from, viewChange.localTimestamp))

      def fromProto(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          timestamp: CantonTimestamp,
          newView: ProtoNewView,
          from: SequencerId,
      ): ParsingResult[NewView] =
        for {
          viewChanges <- newView.viewChanges.traverse(
            SignedMessage.fromProto(ViewChange.fromProtoConsensusMessage)
          )
          prePrepares <- newView.prePrepares.traverse(
            SignedMessage.fromProto(PrePrepare.fromProtoConsensusMessage)
          )
        } yield ConsensusSegment.ConsensusMessage.NewView(
          blockMetadata,
          newView.segmentIndex,
          viewNumber,
          timestamp,
          viewChanges,
          prePrepares,
          from,
        )

      /** For each block number across the view-change messages, we look for:
        *   - the highest view-numbered commit certificate if it exists
        *   - else the highest view-numbered prepare certificate if it exists
        *   - if none exists, the block entry won't be in this map.
        */
      def computeCertificatePerBlock(
          viewChanges: Seq[ViewChange]
      ): Map[BlockNumber, ConsensusCertificate] = viewChanges
        .flatMap(_.consensusCerts)
        .groupBy(_.prePrepare.message.blockMetadata.blockNumber)
        .fmap[ConsensusCertificate] { certs =>
          certs
            .collect { case cc: CommitCertificate => cc }
            .maxByOption(_.prePrepare.message.viewNumber)
            .getOrElse[ConsensusCertificate](
              certs
                .collect { case pc: PrepareCertificate => pc }
                .maxBy(_.prePrepare.message.viewNumber)
            )
        }
    }

    final case class BlockOrdered(metadata: BlockMetadata) extends ConsensusMessage

    final case class CompletedEpoch(epochNumber: EpochNumber) extends ConsensusMessage

  }

}
