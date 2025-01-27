// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules

import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
  FutureId,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  OrderingBlock,
  ProofOfAvailability,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  ConsensusCertificate,
  OrderedBlock,
  PrepareCertificate,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  SignedMessage,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v1
import com.digitalasset.canton.topology.{SequencerId, UniqueIdentifier}
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  HasRepresentativeProtocolVersion,
  OriginalByteString,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanionNoContextMemoization,
}
import com.google.protobuf.ByteString

import ConsensusSegment.ConsensusMessage.PbftNetworkMessage.headerFromProto

object ConsensusSegment {

  sealed trait Message extends Product with Serializable

  final case object Start extends Message

  final case object StartModuleClosingBehaviour extends Message

  /** Messages only sent to itself via pipe-to-self
    */
  sealed trait Internal extends Message
  object Internal {
    final case class OrderedBlockStored(
        commitCertificate: CommitCertificate,
        viewNumber: ViewNumber,
    ) extends Message {
      private val prePrepare = commitCertificate.prePrepare
      val orderedBlock = OrderedBlock(
        prePrepare.message.blockMetadata,
        prePrepare.message.block.proofs,
        prePrepare.message.canonicalCommitSet,
      )
    }

    final case class AsyncException(e: Throwable) extends Message

  }

  sealed trait RetransmissionsMessage extends Message
  object RetransmissionsMessage {
    final case class StatusRequest(segmentIndex: Int) extends RetransmissionsMessage
    final case class RetransmissionRequest(
        from: SequencerId,
        fromStatus: ConsensusStatus.SegmentStatus.Incomplete,
    ) extends RetransmissionsMessage
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

    sealed trait PbftMessagesStored extends PbftEvent {
      def blockMetadata: BlockMetadata
      def viewNumber: ViewNumber
    }
    final case class PrePrepareStored(blockMetadata: BlockMetadata, viewNumber: ViewNumber)
        extends PbftMessagesStored
    final case class PreparesStored(blockMetadata: BlockMetadata, viewNumber: ViewNumber)
        extends PbftMessagesStored
    final case class NewViewStored(blockMetadata: BlockMetadata, viewNumber: ViewNumber)
        extends PbftMessagesStored

    final case class RetransmittedCommitCertificate(
        from: SequencerId,
        commitCertificate: CommitCertificate,
    ) extends PbftEvent {
      override def blockMetadata: BlockMetadata = commitCertificate.prePrepare.message.blockMetadata
      override def viewNumber: ViewNumber =
        commitCertificate.commits.headOption.map(_.message.viewNumber).getOrElse(ViewNumber.First)
    }

    /** Pbft consensus messages coming from the network
      * The order of messages below correspond to the protocol steps
      */
    sealed trait PbftNetworkMessage
        extends HasRepresentativeProtocolVersion
        with ProtocolVersionedMemoizedEvidence
        with MessageFrom {
      def blockMetadata: BlockMetadata
      def viewNumber: ViewNumber
      def localTimestamp: CantonTimestamp
      def toProto: v1.ConsensusMessage
    }

    /** A Signed Pbft consensus message that is an actual event that the consensus module
      * will act upon
      */
    final case class PbftSignedNetworkMessage(message: SignedMessage[PbftNetworkMessage])
        extends PbftEvent {
      override def blockMetadata: BlockMetadata = message.message.blockMetadata

      override def viewNumber: ViewNumber = message.message.viewNumber
    }

    final case class MessageFromPipeToSelf(
        event: Option[Message],
        futureId: FutureId,
    ) extends Message

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
          message: v1.ConsensusMessage
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
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[PrePrepare.type],
        override val deserializedFrom: Option[ByteString],
    ) extends PbftNormalCaseMessage
        with HasProtocolVersionedWrapper[PrePrepare] {

      lazy val stored = PrePrepareStored(blockMetadata, viewNumber)

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

      override def toProto: v1.ConsensusMessage =
        v1.ConsensusMessage.of(
          Some(blockMetadata.toProto),
          viewNumber,
          from.uid.toProtoPrimitive,
          localTimestamp.toMicros,
          v1.ConsensusMessage.Message.PrePrepare(
            v1.PrePrepare.of(
              Some(block.toProto),
              Some(canonicalCommitSet.toProto),
            )
          ),
        )

      override protected val companionObj: PrePrepare.type = PrePrepare

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object PrePrepare extends VersioningCompanionNoContextMemoization[PrePrepare] {
      override def name: String = "PrePrepare"

      def create(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          localTimestamp: CantonTimestamp,
          block: OrderingBlock,
          canonicalCommitSet: CanonicalCommitSet,
          from: SequencerId,
      ): PrePrepare =
        PrePrepare(blockMetadata, viewNumber, localTimestamp, block, canonicalCommitSet, from)(
          protocolVersionRepresentativeFor(ProtocolVersion.minimum): RepresentativeProtocolVersion[
            PrePrepare.this.type
          ],
          None,
        )

      def fromProtoConsensusMessage(
          value: v1.ConsensusMessage
      )(originalByteString: OriginalByteString): ParsingResult[PrePrepare] =
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
          )(originalByteString)
        } yield prePrepare

      def fromProto(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          timestamp: CantonTimestamp,
          prePrepare: v1.PrePrepare,
          from: SequencerId,
      )(originalByteString: OriginalByteString): ParsingResult[PrePrepare] =
        for {
          protoCanonicalCommitSet <- ProtoConverter
            .required("bftTimeCanonicalCommitSet", prePrepare.bftTimeCanonicalCommitSet)
          canonicalCommitSet <- CanonicalCommitSet.fromProto(protoCanonicalCommitSet)
          proofs <- prePrepare.block match {
            case Some(block) => block.proofs.traverse(ProofOfAvailability.fromProto)
            case None =>
              Left(ProtoDeserializationError.OtherError("Pre-prepare with no ordering block"))
          }
          rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
        } yield ConsensusSegment.ConsensusMessage.PrePrepare(
          blockMetadata,
          viewNumber,
          timestamp,
          OrderingBlock(proofs),
          canonicalCommitSet,
          from,
        )(rpv, Some(originalByteString))

      override def versioningTable: VersioningTable = VersioningTable(
        ProtoVersion(30) ->
          VersionedProtoCodec(
            ProtocolVersion.v33
          )(v1.ConsensusMessage)(
            supportedProtoVersionMemoized(_)(
              PrePrepare.fromProtoConsensusMessage
            ),
            _.toProto,
          )
      )
    }

    final case class Prepare private (
        blockMetadata: BlockMetadata,
        viewNumber: ViewNumber,
        hash: Hash,
        localTimestamp: CantonTimestamp,
        from: SequencerId,
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[Prepare.type],
        override val deserializedFrom: Option[ByteString],
    ) extends PbftNormalCaseMessage
        with HasProtocolVersionedWrapper[Prepare] {
      override def toProto: v1.ConsensusMessage =
        v1.ConsensusMessage.of(
          Some(blockMetadata.toProto),
          viewNumber,
          from.uid.toProtoPrimitive,
          localTimestamp.toMicros,
          v1.ConsensusMessage.Message.Prepare(
            v1.Prepare.of(
              hash.getCryptographicEvidence
            )
          ),
        )

      override protected val companionObj: Prepare.type = Prepare

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object Prepare extends VersioningCompanionNoContextMemoization[Prepare] {
      override def name: String = "Prepare"
      implicit val ordering: Ordering[Prepare] =
        Ordering.by(prepare => (prepare.from, prepare.localTimestamp))

      def create(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          hash: Hash,
          localTimestamp: CantonTimestamp,
          from: SequencerId,
      ): Prepare =
        Prepare(blockMetadata, viewNumber, hash, localTimestamp, from)(
          protocolVersionRepresentativeFor(ProtocolVersion.minimum),
          None,
        )

      def fromProtoConsensusMessage(
          value: v1.ConsensusMessage
      )(originalByteString: OriginalByteString): ParsingResult[Prepare] =
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
          )(originalByteString)
        } yield prepare

      def fromProto(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          timestamp: CantonTimestamp,
          prepare: v1.Prepare,
          from: SequencerId,
      )(originalByteString: OriginalByteString): ParsingResult[Prepare] =
        for {
          hash <- Hash.fromProtoPrimitive(prepare.blockHash)
          rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
        } yield ConsensusSegment.ConsensusMessage.Prepare(
          blockMetadata,
          viewNumber,
          hash,
          timestamp,
          from,
        )(rpv, Some(originalByteString))

      override def versioningTable: VersioningTable = VersioningTable(
        ProtoVersion(30) ->
          VersionedProtoCodec(
            ProtocolVersion.v33
          )(v1.ConsensusMessage)(
            supportedProtoVersionMemoized(_)(
              Prepare.fromProtoConsensusMessage
            ),
            _.toProto,
          )
      )
    }

    final case class Commit private (
        blockMetadata: BlockMetadata,
        viewNumber: ViewNumber,
        hash: Hash,
        localTimestamp: CantonTimestamp,
        from: SequencerId,
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[Commit.type],
        override val deserializedFrom: Option[ByteString],
    ) extends PbftNormalCaseMessage
        with HasProtocolVersionedWrapper[Commit] {
      override def toProto: v1.ConsensusMessage =
        v1.ConsensusMessage.of(
          Some(blockMetadata.toProto),
          viewNumber,
          from.uid.toProtoPrimitive,
          localTimestamp.toMicros,
          v1.ConsensusMessage.Message.Commit(
            v1.Commit.of(hash.getCryptographicEvidence)
          ),
        )

      override protected val companionObj: Commit.type = Commit

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object Commit extends VersioningCompanionNoContextMemoization[Commit] {
      override def name: String = "Commit"
      implicit val ordering: Ordering[Commit] =
        Ordering.by(commit => (commit.from, commit.localTimestamp))

      def create(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          hash: Hash,
          localTimestamp: CantonTimestamp,
          from: SequencerId,
      ): Commit = Commit(blockMetadata, viewNumber, hash, localTimestamp, from)(
        protocolVersionRepresentativeFor(ProtocolVersion.minimum),
        None,
      )

      def fromProtoConsensusMessage(
          value: v1.ConsensusMessage
      )(originalByteString: OriginalByteString): ParsingResult[Commit] =
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
          )(originalByteString)
        } yield commit

      def fromProto(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          timestamp: CantonTimestamp,
          commit: v1.Commit,
          from: SequencerId,
      )(originalByteString: OriginalByteString): ParsingResult[Commit] =
        for {
          hash <- Hash.fromProtoPrimitive(commit.blockHash)
          rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
        } yield ConsensusSegment.ConsensusMessage.Commit(
          blockMetadata,
          viewNumber,
          hash,
          timestamp,
          from,
        )(rpv, Some(originalByteString))

      override def versioningTable: VersioningTable = VersioningTable(
        ProtoVersion(30) ->
          VersionedProtoCodec(
            ProtocolVersion.v33
          )(v1.ConsensusMessage)(
            supportedProtoVersionMemoized(_)(
              Commit.fromProtoConsensusMessage
            ),
            _.toProto,
          )
      )
    }

    final case class ViewChange private (
        blockMetadata: BlockMetadata,
        segmentIndex: Int,
        viewNumber: ViewNumber,
        localTimestamp: CantonTimestamp,
        consensusCerts: Seq[ConsensusCertificate],
        from: SequencerId,
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[ViewChange.type],
        override val deserializedFrom: Option[ByteString],
    ) extends PbftViewChangeMessage
        with HasProtocolVersionedWrapper[ViewChange] {
      override def toProto: v1.ConsensusMessage =
        v1.ConsensusMessage.of(
          Some(blockMetadata.toProto),
          viewNumber,
          from.uid.toProtoPrimitive,
          localTimestamp.toMicros,
          v1.ConsensusMessage.Message.ViewChange(
            v1.ViewChange.of(
              segmentIndex,
              consensusCerts.map(certificate =>
                v1.ConsensusCertificate.of(certificate match {
                  case pc: PrepareCertificate =>
                    v1.ConsensusCertificate.Certificate.PrepareCertificate(pc.toProto)
                  case cc: CommitCertificate =>
                    v1.ConsensusCertificate.Certificate.CommitCertificate(cc.toProto)
                })
              ),
            )
          ),
        )

      override protected val companionObj: ViewChange.type = ViewChange

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object ViewChange extends VersioningCompanionNoContextMemoization[ViewChange] {
      override def name: String = "ViewChange"
      def create(
          blockMetadata: BlockMetadata,
          segmentIndex: Int,
          viewNumber: ViewNumber,
          localTimestamp: CantonTimestamp,
          consensusCerts: Seq[ConsensusCertificate],
          from: SequencerId,
      ): ViewChange =
        ViewChange(blockMetadata, segmentIndex, viewNumber, localTimestamp, consensusCerts, from)(
          protocolVersionRepresentativeFor(ProtocolVersion.minimum),
          None,
        )

      def fromProtoConsensusMessage(
          value: v1.ConsensusMessage
      )(originalByteString: OriginalByteString): ParsingResult[ViewChange] =
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
          )(originalByteString)
        } yield viewChange

      def fromProto(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          timestamp: CantonTimestamp,
          viewChange: v1.ViewChange,
          from: SequencerId,
      )(originalByteString: OriginalByteString): ParsingResult[ViewChange] =
        for {
          certs <- viewChange.consensusCerts.traverse(ConsensusCertificate.fromProto)
          rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
        } yield ConsensusSegment.ConsensusMessage.ViewChange(
          blockMetadata,
          viewChange.segmentIndex,
          viewNumber,
          timestamp,
          certs,
          from,
        )(rpv, Some(originalByteString))

      override def versioningTable: VersioningTable = VersioningTable(
        ProtoVersion(30) ->
          VersionedProtoCodec(
            ProtocolVersion.v33
          )(v1.ConsensusMessage)(
            supportedProtoVersionMemoized(_)(
              ViewChange.fromProtoConsensusMessage
            ),
            _.toProto,
          )
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
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[NewView.type],
        override val deserializedFrom: Option[ByteString],
    ) extends PbftViewChangeMessage
        with HasProtocolVersionedWrapper[NewView] {
      import NewView.*

      private lazy val sortedViewChanges: Seq[SignedMessage[ViewChange]] = viewChanges.sorted

      lazy val computedCertificatePerBlock: Map[BlockNumber, ConsensusCertificate] =
        computeCertificatePerBlock(viewChanges.map(_.message))

      lazy val stored = NewViewStored(blockMetadata, viewNumber)

      override def toProto: v1.ConsensusMessage =
        v1.ConsensusMessage.of(
          Some(blockMetadata.toProto),
          viewNumber,
          from.uid.toProtoPrimitive,
          localTimestamp.toMicros,
          v1.ConsensusMessage.Message.NewView(
            v1.NewView.of(
              segmentIndex,
              sortedViewChanges.map(_.toProto),
              prePrepares.map(_.toProto),
            )
          ),
        )

      override protected val companionObj: NewView.type = NewView

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString

    }

    @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
    object NewView extends VersioningCompanionNoContextMemoization[NewView] {
      override def name: String = "NewView"
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
      )(
        protocolVersionRepresentativeFor(ProtocolVersion.minimum),
        None,
      )
      implicit val ordering: Ordering[ViewChange] =
        Ordering.by(viewChange => (viewChange.from, viewChange.localTimestamp))

      def fromProtoConsensusMessage(
          value: v1.ConsensusMessage
      )(originalByteString: OriginalByteString): ParsingResult[NewView] =
        for {
          header <- headerFromProto(value)
          protoNewView <- value.message.newView.toRight(
            ProtoDeserializationError.OtherError(s"Not a $name message")
          )
          newView <- NewView.fromProto(
            header.blockMetadata,
            header.viewNumber,
            header.timestamp,
            protoNewView,
            header.from,
          )(originalByteString)
        } yield newView

      def fromProto(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          timestamp: CantonTimestamp,
          newView: v1.NewView,
          from: SequencerId,
      )(originalByteString: OriginalByteString): ParsingResult[NewView] =
        for {
          viewChanges <- newView.viewChanges.traverse(
            SignedMessage.fromProto(v1.ConsensusMessage)(ViewChange.fromProtoConsensusMessage)
          )
          prePrepares <- newView.prePrepares.traverse(
            SignedMessage.fromProto(v1.ConsensusMessage)(PrePrepare.fromProtoConsensusMessage)
          )
          rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
        } yield ConsensusSegment.ConsensusMessage.NewView(
          blockMetadata,
          newView.segmentIndex,
          viewNumber,
          timestamp,
          viewChanges,
          prePrepares,
          from,
        )(rpv, Some(originalByteString))

      override def versioningTable: VersioningTable = VersioningTable(
        ProtoVersion(30) ->
          VersionedProtoCodec(
            ProtocolVersion.v33
          )(v1.ConsensusMessage)(
            supportedProtoVersionMemoized(_)(
              NewView.fromProtoConsensusMessage
            ),
            _.toProto,
          )
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

    sealed trait PbftViewChangeEvent extends PbftEvent

    final case class SignedPrePrepares(
        blockMetadata: BlockMetadata,
        viewNumber: ViewNumber,
        signedMessages: Seq[SignedMessage[PrePrepare]],
    ) extends PbftViewChangeEvent

    final case class BlockOrdered(metadata: BlockMetadata) extends ConsensusMessage

    final case class CompletedEpoch(epochNumber: EpochNumber) extends ConsensusMessage

    final case class CancelEpoch(epochNumber: EpochNumber) extends ConsensusMessage
  }
}
