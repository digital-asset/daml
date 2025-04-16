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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
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
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  HasRepresentativeProtocolVersion,
  OriginalByteString,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanionMemoization,
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
      val orderedBlock: OrderedBlock =
        OrderedBlock(
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
        from: BftNodeId,
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
        from: BftNodeId,
        commitCertificate: CommitCertificate,
    ) extends PbftEvent {
      override def blockMetadata: BlockMetadata = commitCertificate.prePrepare.message.blockMetadata
      override def viewNumber: ViewNumber =
        commitCertificate.commits.headOption.map(_.message.viewNumber).getOrElse(ViewNumber.First)
    }

    /** Pbft consensus messages coming from the network The order of messages below correspond to
      * the protocol steps
      */
    sealed trait PbftNetworkMessage
        extends HasRepresentativeProtocolVersion
        with ProtocolVersionedMemoizedEvidence
        with MessageFrom {
      def blockMetadata: BlockMetadata
      def viewNumber: ViewNumber
      def toProto: v30.ConsensusMessage
    }

    /** A Signed Pbft consensus message that is an actual event that the consensus module will act
      * upon
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
          from: BftNodeId,
      )
      def headerFromProto(
          message: v30.ConsensusMessage
      ): ParsingResult[Header] =
        for {
          protoBlockMetadata <- ProtoConverter
            .required("blockMetadata", message.blockMetadata)
          blockMetadata <- BlockMetadata.fromProto(protoBlockMetadata)
          viewNumber = ViewNumber(message.viewNumber)
          from = BftNodeId(message.from)
        } yield Header(blockMetadata, viewNumber, from)
    }

    final case class PrePrepare private (
        blockMetadata: BlockMetadata,
        viewNumber: ViewNumber,
        block: OrderingBlock,
        canonicalCommitSet: CanonicalCommitSet,
        from: BftNodeId,
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[PrePrepare.type],
        override val deserializedFrom: Option[ByteString],
    ) extends PbftNormalCaseMessage
        with HasProtocolVersionedWrapper[PrePrepare] {

      lazy val stored: PrePrepareStored = PrePrepareStored(blockMetadata, viewNumber)

      /** The hash of the PrePrepare covers all fields of the message, including both the:
        *   - originating node's chosen content (e.g., OrderingBlock, CanonicalCommitSet)
        *   - common block metadata (e.g., BlockNumber, EpochNumber, ViewNumber)
        *
        * While only the originating node's chosen content is required, it is safe and convenient to
        * use a hash over the entire PrePrepare because we (a) ensure that the content of the
        * PrePrepare does not change across views (if the PrePrepare is bound), and (b) can leverage
        * the `getCryptographicEvidence` paradigm for efficiency and determinism.
        */
      lazy val hash: Hash = {
        val builder = Hash
          .build(HashPurpose.BftOrderingPbftBlock, HashAlgorithm.Sha256)
          .add(getCryptographicEvidence)
        builder.finish()
      }

      override def toProto: v30.ConsensusMessage =
        v30.ConsensusMessage.of(
          Some(blockMetadata.toProto),
          viewNumber,
          from,
          v30.ConsensusMessage.Message.PrePrepare(
            v30.PrePrepare.of(
              Some(block.toProto),
              Some(canonicalCommitSet.toProto),
            )
          ),
        )

      override protected val companionObj: PrePrepare.type = PrePrepare

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object PrePrepare extends VersioningCompanionMemoization[PrePrepare] {
      override def name: String = "PrePrepare"

      def create(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          block: OrderingBlock,
          canonicalCommitSet: CanonicalCommitSet,
          from: BftNodeId,
      ): PrePrepare =
        PrePrepare(blockMetadata, viewNumber, block, canonicalCommitSet, from)(
          protocolVersionRepresentativeFor(ProtocolVersion.minimum): RepresentativeProtocolVersion[
            PrePrepare.this.type
          ], // TODO(#23248)
          None,
        )

      def fromProtoConsensusMessage(
          value: v30.ConsensusMessage
      )(originalByteString: OriginalByteString): ParsingResult[PrePrepare] =
        for {
          header <- headerFromProto(value)
          protoPrePrepare <- value.message.prePrepare.toRight(
            ProtoDeserializationError.OtherError("Not a PrePrepare message")
          )
          prePrepare <- ConsensusSegment.ConsensusMessage.PrePrepare.fromProto(
            header.blockMetadata,
            header.viewNumber,
            protoPrePrepare,
            header.from,
          )(originalByteString)
        } yield prePrepare

      def fromProto(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          prePrepare: v30.PrePrepare,
          from: BftNodeId,
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
          OrderingBlock(proofs),
          canonicalCommitSet,
          from,
        )(rpv, Some(originalByteString))

      override def versioningTable: VersioningTable = VersioningTable(
        ProtoVersion(30) ->
          VersionedProtoCodec(
            ProtocolVersion.v34
          )(v30.ConsensusMessage)(
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
        from: BftNodeId,
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[Prepare.type],
        override val deserializedFrom: Option[ByteString],
    ) extends PbftNormalCaseMessage
        with HasProtocolVersionedWrapper[Prepare] {
      override def toProto: v30.ConsensusMessage =
        v30.ConsensusMessage.of(
          Some(blockMetadata.toProto),
          viewNumber,
          from,
          v30.ConsensusMessage.Message.Prepare(
            v30.Prepare.of(
              hash.getCryptographicEvidence
            )
          ),
        )

      override protected val companionObj: Prepare.type = Prepare

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object Prepare extends VersioningCompanionMemoization[Prepare] {
      override def name: String = "Prepare"
      implicit val ordering: Ordering[Prepare] = Ordering.by(prepare => prepare.from)

      def create(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          hash: Hash,
          from: BftNodeId,
      ): Prepare =
        Prepare(blockMetadata, viewNumber, hash, from)(
          protocolVersionRepresentativeFor(ProtocolVersion.minimum), // TODO(#23248)
          None,
        )

      def fromProtoConsensusMessage(
          value: v30.ConsensusMessage
      )(originalByteString: OriginalByteString): ParsingResult[Prepare] =
        for {
          header <- headerFromProto(value)
          protoPrepare <- value.message.prepare.toRight(
            ProtoDeserializationError.OtherError("Not a Prepare message")
          )
          prepare <- Prepare.fromProto(
            header.blockMetadata,
            header.viewNumber,
            protoPrepare,
            header.from,
          )(originalByteString)
        } yield prepare

      def fromProto(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          prepare: v30.Prepare,
          from: BftNodeId,
      )(originalByteString: OriginalByteString): ParsingResult[Prepare] =
        for {
          hash <- Hash.fromProtoPrimitive(prepare.blockHash)
          rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
        } yield ConsensusSegment.ConsensusMessage.Prepare(
          blockMetadata,
          viewNumber,
          hash,
          from,
        )(rpv, Some(originalByteString))

      override def versioningTable: VersioningTable = VersioningTable(
        ProtoVersion(30) ->
          VersionedProtoCodec(
            ProtocolVersion.v34
          )(v30.ConsensusMessage)(
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
        from: BftNodeId,
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[Commit.type],
        override val deserializedFrom: Option[ByteString],
    ) extends PbftNormalCaseMessage
        with HasProtocolVersionedWrapper[Commit] {
      override def toProto: v30.ConsensusMessage =
        v30.ConsensusMessage.of(
          Some(blockMetadata.toProto),
          viewNumber,
          from,
          v30.ConsensusMessage.Message.Commit(
            v30.Commit.of(hash.getCryptographicEvidence, localTimestamp.toMicros)
          ),
        )

      override protected val companionObj: Commit.type = Commit

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object Commit extends VersioningCompanionMemoization[Commit] {
      override def name: String = "Commit"
      implicit val ordering: Ordering[Commit] =
        Ordering.by(commit => (commit.from, commit.localTimestamp))

      def create(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          hash: Hash,
          localTimestamp: CantonTimestamp,
          from: BftNodeId,
      ): Commit = Commit(blockMetadata, viewNumber, hash, localTimestamp, from)(
        protocolVersionRepresentativeFor(ProtocolVersion.minimum), // TODO(#23248)
        None,
      )

      def fromProtoConsensusMessage(
          value: v30.ConsensusMessage
      )(originalByteString: OriginalByteString): ParsingResult[Commit] =
        for {
          header <- headerFromProto(value)
          protoCommit <- value.message.commit.toRight(
            ProtoDeserializationError.OtherError("Not a Commit message")
          )
          commit <- Commit.fromProto(
            header.blockMetadata,
            header.viewNumber,
            protoCommit,
            header.from,
          )(originalByteString)
        } yield commit

      def fromProto(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          commit: v30.Commit,
          from: BftNodeId,
      )(originalByteString: OriginalByteString): ParsingResult[Commit] =
        for {
          hash <- Hash.fromProtoPrimitive(commit.blockHash)
          timestamp <- CantonTimestamp.fromProtoPrimitive(commit.localTimestamp)
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
            ProtocolVersion.v34
          )(v30.ConsensusMessage)(
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
        consensusCerts: Seq[ConsensusCertificate],
        from: BftNodeId,
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[ViewChange.type],
        override val deserializedFrom: Option[ByteString],
    ) extends PbftViewChangeMessage
        with HasProtocolVersionedWrapper[ViewChange] {
      override def toProto: v30.ConsensusMessage =
        v30.ConsensusMessage.of(
          Some(blockMetadata.toProto),
          viewNumber,
          from,
          v30.ConsensusMessage.Message.ViewChange(
            v30.ViewChange.of(
              segmentIndex,
              consensusCerts.map(certificate =>
                v30.ConsensusCertificate.of(certificate match {
                  case pc: PrepareCertificate =>
                    v30.ConsensusCertificate.Certificate.PrepareCertificate(pc.toProto)
                  case cc: CommitCertificate =>
                    v30.ConsensusCertificate.Certificate.CommitCertificate(cc.toProto)
                })
              ),
            )
          ),
        )

      override protected val companionObj: ViewChange.type = ViewChange

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object ViewChange extends VersioningCompanionMemoization[ViewChange] {
      override def name: String = "ViewChange"
      def create(
          blockMetadata: BlockMetadata,
          segmentIndex: Int,
          viewNumber: ViewNumber,
          consensusCerts: Seq[ConsensusCertificate],
          from: BftNodeId,
      ): ViewChange =
        ViewChange(blockMetadata, segmentIndex, viewNumber, consensusCerts, from)(
          protocolVersionRepresentativeFor(ProtocolVersion.minimum), // TODO(#23248)
          None,
        )

      def fromProtoConsensusMessage(
          value: v30.ConsensusMessage
      )(originalByteString: OriginalByteString): ParsingResult[ViewChange] =
        for {
          header <- headerFromProto(value)
          protoViewChange <- value.message.viewChange.toRight(
            ProtoDeserializationError.OtherError("Not a ViewChange message")
          )
          viewChange <- ViewChange.fromProto(
            header.blockMetadata,
            header.viewNumber,
            protoViewChange,
            header.from,
          )(originalByteString)
        } yield viewChange

      def fromProto(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          viewChange: v30.ViewChange,
          from: BftNodeId,
      )(originalByteString: OriginalByteString): ParsingResult[ViewChange] =
        for {
          certs <- viewChange.consensusCerts.traverse(ConsensusCertificate.fromProto)
          rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
        } yield ConsensusSegment.ConsensusMessage.ViewChange(
          blockMetadata,
          viewChange.segmentIndex,
          viewNumber,
          certs,
          from,
        )(rpv, Some(originalByteString))

      override def versioningTable: VersioningTable = VersioningTable(
        ProtoVersion(30) ->
          VersionedProtoCodec(
            ProtocolVersion.v34
          )(v30.ConsensusMessage)(
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
        viewChanges: Seq[SignedMessage[ViewChange]],
        prePrepares: Seq[SignedMessage[PrePrepare]],
        from: BftNodeId,
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

      override def toProto: v30.ConsensusMessage =
        v30.ConsensusMessage.of(
          Some(blockMetadata.toProto),
          viewNumber,
          from,
          v30.ConsensusMessage.Message.NewView(
            v30.NewView.of(
              segmentIndex,
              sortedViewChanges.map(_.toProtoV1),
              prePrepares.map(_.toProtoV1),
            )
          ),
        )

      override protected val companionObj: NewView.type = NewView

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString

    }

    @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
    object NewView extends VersioningCompanionMemoization[NewView] {
      override def name: String = "NewView"
      def create(
          blockMetadata: BlockMetadata,
          segmentIndex: Int,
          viewNumber: ViewNumber,
          viewChanges: Seq[SignedMessage[ViewChange]],
          prePrepares: Seq[SignedMessage[PrePrepare]],
          from: BftNodeId,
      ): NewView = NewView(
        blockMetadata,
        segmentIndex,
        viewNumber,
        viewChanges,
        prePrepares,
        from,
      )(
        protocolVersionRepresentativeFor(ProtocolVersion.minimum), // TODO(#23248)
        None,
      )
      implicit val ordering: Ordering[ViewChange] = Ordering.by(viewChange => viewChange.from)

      def fromProtoConsensusMessage(
          value: v30.ConsensusMessage
      )(originalByteString: OriginalByteString): ParsingResult[NewView] =
        for {
          header <- headerFromProto(value)
          protoNewView <- value.message.newView.toRight(
            ProtoDeserializationError.OtherError(s"Not a $name message")
          )
          newView <- NewView.fromProto(
            header.blockMetadata,
            header.viewNumber,
            protoNewView,
            header.from,
          )(originalByteString)
        } yield newView

      def fromProto(
          blockMetadata: BlockMetadata,
          viewNumber: ViewNumber,
          newView: v30.NewView,
          from: BftNodeId,
      )(originalByteString: OriginalByteString): ParsingResult[NewView] =
        for {
          viewChanges <- newView.viewChanges.traverse(
            SignedMessage.fromProto(v30.ConsensusMessage)(ViewChange.fromProtoConsensusMessage)
          )
          prePrepares <- newView.prePrepares.traverse(
            SignedMessage.fromProto(v30.ConsensusMessage)(PrePrepare.fromProtoConsensusMessage)
          )
          rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
        } yield ConsensusSegment.ConsensusMessage.NewView(
          blockMetadata,
          newView.segmentIndex,
          viewNumber,
          viewChanges,
          prePrepares,
          from,
        )(rpv, Some(originalByteString))

      override def versioningTable: VersioningTable = VersioningTable(
        ProtoVersion(30) ->
          VersionedProtoCodec(
            ProtocolVersion.v34
          )(v30.ConsensusMessage)(
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
