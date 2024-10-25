// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules

import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose, Signature}
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1 as proto
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1.{
  AvailabilityMessage as ProtoAvailabilityMessage,
  BatchRequest as ProtoBatchRequest,
  BatchResponse as ProtoBatchResponse,
  BftOrderingMessageBody as ProtoBftOrderingMessageBody,
  StoreRequest as ProtoStoreRequest,
  StoreResponse as ProtoStoreResponse,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.availability.BatchesRequest
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.OrderingRequestBatch
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.availability.{
  BatchId,
  ProofOfAvailability,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.OrderedBlockForOutput
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.dependencies.AvailabilityModuleDependencies
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.{
  Env,
  Module,
  modules,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.version.{
  HasMemoizedProtocolVersionedWithContextCompanion,
  HasProtocolVersionedWrapper,
  HasRepresentativeProtocolVersion,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.protobuf.ByteString

object Availability {

  sealed trait Message[+E] extends Product

  final case object NoOp extends Message[Nothing]

  final case object Start extends Message[Nothing]

  sealed trait RemoteProtocolMessage
      extends Message[Nothing]
      with HasRepresentativeProtocolVersion
      with ProtocolVersionedMemoizedEvidence {
    def from: SequencerId

    protected def toProtoV30: proto.AvailabilityMessage
  }

  object RemoteProtocolMessage {

    /** Computes the [[crypto.Hash]] that is used to sign this message for transport
      */
    def hashForSignature(from: SequencerId, content: ByteString): Hash = Hash
      .build(HashPurpose.BftSignedAvailabilityMessage, HashAlgorithm.Sha256)
      .add(from.toString)
      .add(content)
      .finish()

    def toProto(message: ByteString): ProtoBftOrderingMessageBody =
      ProtoBftOrderingMessageBody(
        ProtoBftOrderingMessageBody.Message.AvailabilityMessage(
          message
        )
      )
  }

  final case class UnverifiedProtocolMessage(
      underlyingMessage: RemoteProtocolMessage,
      signature: Signature,
  ) extends Message[Nothing]

  sealed trait LocalProtocolMessage[+E] extends Message[E]

  /** Every replica runs the dissemination protocol and plays 2 roles:
    *
    * - The availability module of a replica trying to disseminate a batch produced by the local mempool.
    * - The availability module of a replica responding to store requests by originators.
    *
    * The order of messages below correspond to the protocol steps but some steps are carried out by the availability
    * storage module.
    */
  sealed trait LocalDissemination extends LocalProtocolMessage[Nothing]
  sealed trait RemoteDissemination extends RemoteProtocolMessage

  object LocalDissemination {

    final case class LocalBatchCreated(batchId: BatchId, batch: OrderingRequestBatch)
        extends LocalDissemination

    final case class LocalBatchStored(batchId: BatchId, batch: OrderingRequestBatch)
        extends LocalDissemination

    final case class LocalBatchStoredSigned(
        batchId: BatchId,
        batch: OrderingRequestBatch,
        signature: Signature,
    ) extends LocalDissemination

    final case class RemoteBatchStored(batchId: BatchId, from: SequencerId)
        extends LocalDissemination

    final case class RemoteBatchStoredSigned(
        batchId: BatchId,
        from: SequencerId,
        signature: Signature,
    ) extends LocalDissemination

    final case class RemoteBatchAcknowledgeVerified(
        batchId: BatchId,
        from: SequencerId,
        signature: Signature,
    ) extends LocalDissemination
  }

  object RemoteDissemination {
    final case class RemoteBatch private (
        batchId: BatchId,
        batch: OrderingRequestBatch,
        from: SequencerId,
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[RemoteBatch.type],
        override val deserializedFrom: Option[ByteString],
    ) extends RemoteDissemination
        with HasProtocolVersionedWrapper[RemoteBatch] {
      override protected val companionObj: RemoteBatch.type = RemoteBatch

      protected override def toProtoV30: proto.AvailabilityMessage =
        ProtoAvailabilityMessage.of(
          ProtoAvailabilityMessage.Message.StoreRequest(
            ProtoStoreRequest(batchId.hash.getCryptographicEvidence, Some(batch.toProto))
          )
        )

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object RemoteBatch
        extends HasMemoizedProtocolVersionedWithContextCompanion[RemoteBatch, SequencerId] {

      override def name: String = "RemoteBatch"

      override def supportedProtoVersions
          : modules.Availability.RemoteDissemination.RemoteBatch.SupportedProtoVersions =
        SupportedProtoVersions(
          ProtoVersion(30) ->
            VersionedProtoConverter(
              ProtocolVersion.v32
            )(proto.StoreRequest)(
              supportedProtoVersionMemoized(_)(
                RemoteBatch.fromProtoV30
              ),
              _.toProtoV30.toByteString,
            )
        )

      def fromProtoV30(from: SequencerId, batch: proto.StoreRequest)(
          bytes: ByteString
      ): ParsingResult[RemoteBatch] =
        for {
          id <- BatchId.fromProto(batch.batchId)
          batch <- OrderingRequestBatch.fromProto(batch.batch)
          rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
        } yield Availability.RemoteDissemination.RemoteBatch(id, batch, from)(
          rpv,
          deserializedFrom = Some(bytes),
        )

      def create(batchId: BatchId, batch: OrderingRequestBatch, from: SequencerId): RemoteBatch =
        RemoteBatch(batchId, batch, from)(
          protocolVersionRepresentativeFor(ProtocolVersion.minimum),
          deserializedFrom = None,
        )
    }

    final case class RemoteBatchAcknowledged private (
        batchId: BatchId,
        from: SequencerId,
        signature: Signature,
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[
          RemoteBatchAcknowledged.type
        ],
        override val deserializedFrom: Option[ByteString],
    ) extends RemoteDissemination
        with HasProtocolVersionedWrapper[RemoteBatchAcknowledged] {
      override protected val companionObj: RemoteBatchAcknowledged.type = RemoteBatchAcknowledged

      protected override def toProtoV30: proto.AvailabilityMessage =
        ProtoAvailabilityMessage.of(
          ProtoAvailabilityMessage.Message.StoreResponse(
            ProtoStoreResponse(
              batchId.hash.getCryptographicEvidence,
              Some(signature.toProtoV30),
            )
          )
        )

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object RemoteBatchAcknowledged
        extends HasMemoizedProtocolVersionedWithContextCompanion[
          RemoteBatchAcknowledged,
          SequencerId,
        ] {

      override def name: String = "RemoteBatchAcknowledged"

      override def supportedProtoVersions
          : framework.modules.Availability.RemoteDissemination.RemoteBatchAcknowledged.SupportedProtoVersions =
        SupportedProtoVersions(
          ProtoVersion(30) ->
            VersionedProtoConverter(
              ProtocolVersion.v32
            )(proto.StoreResponse)(
              supportedProtoVersionMemoized(_)(
                RemoteBatchAcknowledged.fromProtoV30
              ),
              _.toProtoV30.toByteString,
            )
        )

      def fromProtoV30(
          from: SequencerId,
          value: proto.StoreResponse,
      )(bytes: ByteString): ParsingResult[RemoteBatchAcknowledged] =
        for {
          id <- BatchId.fromProto(value.batchId)
          signature <- Signature.fromProtoV30(value.getSignature)
          rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
        } yield Availability.RemoteDissemination.RemoteBatchAcknowledged(id, from, signature)(
          rpv,
          deserializedFrom = Some(bytes),
        )

      def create(
          batchId: BatchId,
          from: SequencerId,
          signature: Signature,
      ): RemoteBatchAcknowledged =
        RemoteBatchAcknowledged(batchId, from, signature)(
          protocolVersionRepresentativeFor(ProtocolVersion.minimum),
          deserializedFrom = None,
        )
    }
  }

  /** Every replica runs the output-fetch protocol initiated by the output module requesting batches for a block;
    * there are 2 cases:
    *
    * - The availability module can provide all batches from local storage.
    * - If not, the availability module will ask other peers for the missing batches and then store them.
    *
    * In both cases, all the requested batches are fetched and returned to the output module.
    *
    * The order of messages below correspond to the protocol steps but some steps are carried out by the availability
    * storage module.
    */
  sealed trait LocalOutputFetch extends LocalProtocolMessage[Nothing]
  sealed trait RemoteOutputFetch extends RemoteProtocolMessage

  object LocalOutputFetch {

    final case class FetchBlockData(block: OrderedBlockForOutput) extends LocalOutputFetch

    final case class FetchedBlockDataFromStorage(
        request: BatchesRequest,
        result: AvailabilityStore.FetchBatchesResult,
    ) extends LocalOutputFetch

    final case class FetchBatchDataFromPeers(
        proofOfAvailability: ProofOfAvailability,
        mode: OrderedBlockForOutput.Mode,
    ) extends LocalOutputFetch

    final case class FetchRemoteBatchDataTimeout(batchId: BatchId) extends LocalOutputFetch

    final case class AttemptedBatchDataLoadForPeer(
        batchId: BatchId,
        batch: Option[OrderingRequestBatch],
    ) extends LocalOutputFetch

    final case class FetchedBatchStored(batchId: BatchId) extends LocalOutputFetch
  }

  object RemoteOutputFetch {
    final case class FetchRemoteBatchData private (
        batchId: BatchId,
        from: SequencerId,
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[
          FetchRemoteBatchData.type
        ],
        override val deserializedFrom: Option[ByteString],
    ) extends RemoteOutputFetch
        with HasProtocolVersionedWrapper[FetchRemoteBatchData] {

      override protected val companionObj: FetchRemoteBatchData.type = FetchRemoteBatchData

      protected override def toProtoV30 =
        proto.AvailabilityMessage.of(
          proto.AvailabilityMessage.Message.BatchRequest(
            ProtoBatchRequest(batchId.hash.getCryptographicEvidence)
          )
        )

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object FetchRemoteBatchData
        extends HasMemoizedProtocolVersionedWithContextCompanion[
          FetchRemoteBatchData,
          SequencerId,
        ] {

      override def name: String = "FetchRemoteBatchData"

      override def supportedProtoVersions
          : bftordering.framework.modules.Availability.RemoteOutputFetch.FetchRemoteBatchData.SupportedProtoVersions =
        SupportedProtoVersions(
          ProtoVersion(30) ->
            VersionedProtoConverter(
              ProtocolVersion.v32
            )(proto.BatchRequest)(
              supportedProtoVersionMemoized(_)(
                FetchRemoteBatchData.fromProtoV30
              ),
              _.toProtoV30.toByteString,
            )
        )

      def fromProtoV30(
          from: SequencerId,
          value: proto.BatchRequest,
      )(bytes: ByteString): ParsingResult[FetchRemoteBatchData] =
        for {
          id <- BatchId.fromProto(value.batchId)
          rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
        } yield Availability.RemoteOutputFetch.FetchRemoteBatchData(id, from)(
          rpv,
          deserializedFrom = Some(bytes),
        )

      def create(batchId: BatchId, from: SequencerId): FetchRemoteBatchData =
        FetchRemoteBatchData(batchId, from)(
          protocolVersionRepresentativeFor(ProtocolVersion.minimum),
          deserializedFrom = None,
        )

    }

    final case class RemoteBatchDataFetched private (
        from: SequencerId,
        batchId: BatchId,
        batch: OrderingRequestBatch,
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[
          RemoteBatchDataFetched.type
        ],
        override val deserializedFrom: Option[ByteString],
    ) extends RemoteOutputFetch
        with HasProtocolVersionedWrapper[RemoteBatchDataFetched] {
      override protected val companionObj: RemoteBatchDataFetched.type = RemoteBatchDataFetched

      protected override def toProtoV30 =
        proto.AvailabilityMessage.of(
          proto.AvailabilityMessage.Message.BatchResponse(
            ProtoBatchResponse(batchId.hash.getCryptographicEvidence, Some(batch.toProto))
          )
        )

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object RemoteBatchDataFetched
        extends HasMemoizedProtocolVersionedWithContextCompanion[
          RemoteBatchDataFetched,
          SequencerId,
        ] {

      override def name: String = "RemoteBatchDataFetched"

      override def supportedProtoVersions
          : block.bftordering.framework.modules.Availability.RemoteOutputFetch.RemoteBatchDataFetched.SupportedProtoVersions =
        SupportedProtoVersions(
          ProtoVersion(30) ->
            VersionedProtoConverter(
              ProtocolVersion.v32
            )(proto.BatchResponse)(
              supportedProtoVersionMemoized(_)(
                RemoteBatchDataFetched.fromProtoV30
              ),
              _.toProtoV30.toByteString,
            )
        )

      def fromProtoV30(
          from: SequencerId,
          value: proto.BatchResponse,
      )(bytes: ByteString): ParsingResult[RemoteBatchDataFetched] =
        for {
          id <- BatchId.fromProto(value.batchId)
          batch <- OrderingRequestBatch.fromProto(value.batch)
          rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
        } yield Availability.RemoteOutputFetch.RemoteBatchDataFetched(from, id, batch)(
          rpv,
          deserializedFrom = Some(bytes),
        )

      def create(
          thisPeer: SequencerId,
          batchId: BatchId,
          batch: OrderingRequestBatch,
      ): RemoteBatchDataFetched =
        RemoteBatchDataFetched(thisPeer, batchId, batch)(
          protocolVersionRepresentativeFor(ProtocolVersion.minimum),
          deserializedFrom = None,
        )
    }
  }

  sealed trait Consensus[+E] extends LocalProtocolMessage[E]
  object Consensus {
    final case class CreateProposal[E <: Env[E]](
        orderingTopology: OrderingTopology,
        cryptoProvider: CryptoProvider[E],
        epochNumber: EpochNumber,
        ack: Option[Ack] = None,
    ) extends Consensus[E]
    final case class Ack(batchIds: Seq[BatchId]) extends Consensus[Nothing]
    final case object LocalClockTick extends Consensus[Nothing]
  }
}

trait Availability[E <: Env[E]] extends Module[E, Availability.Message[E]] {
  val dependencies: AvailabilityModuleDependencies[E]
}
