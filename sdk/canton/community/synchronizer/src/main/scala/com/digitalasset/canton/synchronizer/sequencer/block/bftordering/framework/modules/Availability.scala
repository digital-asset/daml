// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.{
  BatchesRequest,
  DisseminationProgress,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  BatchId,
  ProofOfAvailability,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.OrderedBlockForOutput
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  OrderingRequest,
  OrderingRequestBatch,
  SignedMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.AvailabilityModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  Env,
  Module,
  SupportedVersions,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.tracing.Traced
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

object Availability {

  sealed trait Message[+E] extends Product

  final case object NoOp extends Message[Nothing]

  final case object Start extends Message[Nothing]

  sealed trait RemoteProtocolMessage
      extends Message[Nothing]
      with HasRepresentativeProtocolVersion
      with ProtocolVersionedMemoizedEvidence
      with MessageFrom {
    protected def toProtoV30: v30.AvailabilityMessage
  }

  final case class UnverifiedProtocolMessage(
      underlyingMessage: SignedMessage[RemoteProtocolMessage]
  ) extends Message[Nothing]

  sealed trait LocalProtocolMessage[+E] extends Message[E]

  /** Every replica runs the dissemination protocol and plays 2 roles:
    *
    *   - The availability module of a replica trying to disseminate a batch produced by the local
    *     mempool.
    *   - The availability module of a replica responding to store requests by originators.
    *
    * The order of messages below correspond to the protocol steps but some steps are carried out by
    * the availability storage module.
    */
  sealed trait LocalDissemination extends LocalProtocolMessage[Nothing]
  sealed trait RemoteDissemination extends RemoteProtocolMessage

  object LocalDissemination {

    final case class LocalBatchCreated(requests: Seq[Traced[OrderingRequest]])
        extends LocalDissemination

    final case class LocalBatchesStored(batches: Seq[(BatchId, OrderingRequestBatch)])
        extends LocalDissemination

    final case class LocalBatchStoredSigned(
        batchId: BatchId,
        batch: OrderingRequestBatch,
        progressOrSignature: Either[DisseminationProgress, Signature],
    )

    final case class LocalBatchesStoredSigned(
        batches: Seq[LocalBatchStoredSigned]
    ) extends LocalDissemination

    final case class RemoteBatchStored(
        batchId: BatchId,
        epochNumber: EpochNumber,
        from: BftNodeId,
    ) extends LocalDissemination

    final case class RemoteBatchStoredSigned(
        batchId: BatchId,
        from: BftNodeId,
        signature: Signature,
    ) extends LocalDissemination

    final case class RemoteBatchAcknowledgeVerified(
        batchId: BatchId,
        from: BftNodeId,
        signature: Signature,
    ) extends LocalDissemination
  }

  object RemoteDissemination {
    final case class RemoteBatch private (
        batchId: BatchId,
        batch: OrderingRequestBatch,
        from: BftNodeId,
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[RemoteBatch.type],
        override val deserializedFrom: Option[ByteString],
    ) extends RemoteDissemination
        with HasProtocolVersionedWrapper[RemoteBatch] {
      override protected val companionObj: RemoteBatch.type = RemoteBatch

      protected override def toProtoV30: v30.AvailabilityMessage =
        v30.AvailabilityMessage(
          v30.AvailabilityMessage.Message.StoreRequest(
            v30.StoreRequest(batchId.hash.getCryptographicEvidence, Some(batch.toProtoV30))
          )
        )

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object RemoteBatch extends VersioningCompanionContextMemoization[RemoteBatch, BftNodeId] {

      override def name: String = "RemoteBatch"

      override def versioningTable: VersioningTable =
        VersioningTable(
          SupportedVersions.ProtoData -> {
            VersionedProtoCodec(SupportedVersions.CantonProtocol)(v30.AvailabilityMessage)(
              supportedProtoVersionMemoized(_)(RemoteBatch.fromProtoAvailabilityMessage),
              _.toProtoV30,
            )
          }
        )

      def fromProtoAvailabilityMessage(from: BftNodeId, value: v30.AvailabilityMessage)(
          bytes: ByteString
      ): ParsingResult[RemoteBatch] = for {
        protoStoreRequest <- value.message.storeRequest.toRight(
          ProtoDeserializationError.OtherError(s"Not a $name message")
        )
        storeRequest <- fromProtoV30(from, protoStoreRequest)(bytes)
      } yield storeRequest

      def fromProtoV30(from: BftNodeId, storeRequest: v30.StoreRequest)(
          bytes: ByteString
      ): ParsingResult[RemoteBatch] =
        for {
          id <- BatchId.fromProto(storeRequest.batchId)
          batch <- storeRequest.batch match {
            case Some(batch) =>
              OrderingRequestBatch.fromProtoV30(batch)
            case None => Left(ProtoDeserializationError.FieldNotSet("batch"))
          }
          rpv <- protocolVersionRepresentativeFor(SupportedVersions.ProtoData)
        } yield Availability.RemoteDissemination.RemoteBatch(id, batch, from)(
          rpv,
          deserializedFrom = Some(bytes),
        )

      def create(
          batchId: BatchId,
          batch: OrderingRequestBatch,
          from: BftNodeId,
      )(implicit synchronizerProtocolVersion: ProtocolVersion): RemoteBatch =
        RemoteBatch(batchId, batch, from)(
          protocolVersionRepresentativeFor(synchronizerProtocolVersion),
          deserializedFrom = None,
        )
    }

    final case class RemoteBatchAcknowledged private (
        batchId: BatchId,
        from: BftNodeId,
        signature: Signature,
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[
          RemoteBatchAcknowledged.type
        ],
        override val deserializedFrom: Option[ByteString],
    ) extends RemoteDissemination
        with HasProtocolVersionedWrapper[RemoteBatchAcknowledged] {
      override protected val companionObj: RemoteBatchAcknowledged.type = RemoteBatchAcknowledged

      protected override def toProtoV30: v30.AvailabilityMessage =
        v30.AvailabilityMessage(
          v30.AvailabilityMessage.Message.StoreResponse(
            v30.StoreResponse(
              batchId.hash.getCryptographicEvidence,
              Some(signature.toProtoV30),
            )
          )
        )

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object RemoteBatchAcknowledged
        extends VersioningCompanionContextMemoization[
          RemoteBatchAcknowledged,
          BftNodeId,
        ] {

      override def name: String = "RemoteBatchAcknowledged"

      override def versioningTable: VersioningTable = VersioningTable(
        SupportedVersions.ProtoData ->
          VersionedProtoCodec(SupportedVersions.CantonProtocol)(v30.AvailabilityMessage)(
            supportedProtoVersionMemoized(_)(RemoteBatchAcknowledged.fromAvailabilityMessage),
            _.toProtoV30,
          )
      )

      def fromAvailabilityMessage(from: BftNodeId, value: v30.AvailabilityMessage)(
          bytes: ByteString
      ): ParsingResult[RemoteBatchAcknowledged] = for {
        protoStoreResponse <- value.message.storeResponse.toRight(
          ProtoDeserializationError.OtherError(s"Not a $name message")
        )
        storeResponse <- fromProtoV30(from, protoStoreResponse)(bytes)
      } yield storeResponse

      def fromProtoV30(
          from: BftNodeId,
          value: v30.StoreResponse,
      )(bytes: ByteString): ParsingResult[RemoteBatchAcknowledged] =
        for {
          id <- BatchId.fromProto(value.batchId)
          signature <- Signature.fromProtoV30(value.getSignature)
          rpv <- protocolVersionRepresentativeFor(SupportedVersions.ProtoData)
        } yield Availability.RemoteDissemination.RemoteBatchAcknowledged(id, from, signature)(
          rpv,
          deserializedFrom = Some(bytes),
        )

      def create(
          batchId: BatchId,
          from: BftNodeId,
          signature: Signature,
      )(implicit synchronizerProtocolVersion: ProtocolVersion): RemoteBatchAcknowledged =
        RemoteBatchAcknowledged(batchId, from, signature)(
          protocolVersionRepresentativeFor(synchronizerProtocolVersion),
          deserializedFrom = None,
        )
    }
  }

  /** Every replica runs the output-fetch protocol initiated by the output module requesting batches
    * for a block; there are 2 cases:
    *
    *   - The availability module can provide all batches from local storage.
    *   - If not, the availability module will ask other nodes for the missing batches and then
    *     store them.
    *
    * In both cases, all the requested batches are fetched and returned to the output module.
    *
    * The order of messages below correspond to the protocol steps but some steps are carried out by
    * the availability storage module.
    */
  sealed trait LocalOutputFetch extends LocalProtocolMessage[Nothing]
  sealed trait RemoteOutputFetch extends RemoteProtocolMessage

  object LocalOutputFetch {

    final case class FetchBlockData(block: OrderedBlockForOutput) extends LocalOutputFetch

    final case class FetchedBlockDataFromStorage(
        request: BatchesRequest,
        result: AvailabilityStore.FetchBatchesResult,
    ) extends LocalOutputFetch

    final case class FetchBatchDataFromNodes(
        proofOfAvailability: ProofOfAvailability,
        mode: OrderedBlockForOutput.Mode,
    ) extends LocalOutputFetch

    final case class FetchRemoteBatchDataTimeout(batchId: BatchId) extends LocalOutputFetch

    final case class AttemptedBatchDataLoadForNode(
        batchId: BatchId,
        batch: Option[OrderingRequestBatch],
    ) extends LocalOutputFetch

    final case class FetchedBatchStored(batchId: BatchId) extends LocalOutputFetch
  }

  object RemoteOutputFetch {
    final case class FetchRemoteBatchData private (
        batchId: BatchId,
        from: BftNodeId,
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[
          FetchRemoteBatchData.type
        ],
        override val deserializedFrom: Option[ByteString],
    ) extends RemoteOutputFetch
        with HasProtocolVersionedWrapper[FetchRemoteBatchData] {

      override protected val companionObj: FetchRemoteBatchData.type = FetchRemoteBatchData

      protected override def toProtoV30: v30.AvailabilityMessage =
        v30.AvailabilityMessage(
          v30.AvailabilityMessage.Message.BatchRequest(
            v30.BatchRequest(batchId.hash.getCryptographicEvidence)
          )
        )

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object FetchRemoteBatchData
        extends VersioningCompanionContextMemoization[
          FetchRemoteBatchData,
          BftNodeId,
        ] {

      override def name: String = "FetchRemoteBatchData"

      override def versioningTable: VersioningTable = VersioningTable(
        SupportedVersions.ProtoData ->
          VersionedProtoCodec(SupportedVersions.CantonProtocol)(v30.AvailabilityMessage)(
            supportedProtoVersionMemoized(_)(
              FetchRemoteBatchData.fromAvailabilityMessage
            ),
            _.toProtoV30,
          )
      )

      def fromAvailabilityMessage(
          from: BftNodeId,
          value: v30.AvailabilityMessage,
      )(bytes: ByteString): ParsingResult[FetchRemoteBatchData] = for {
        protoFetchRemoteBatchData <- value.message.batchRequest.toRight(
          ProtoDeserializationError.OtherError(s"Not a $name message")
        )
        fetchRemoteBatchData <- fromProtoV30(from, protoFetchRemoteBatchData)(bytes)
      } yield fetchRemoteBatchData

      def fromProtoV30(
          from: BftNodeId,
          value: v30.BatchRequest,
      )(bytes: ByteString): ParsingResult[FetchRemoteBatchData] =
        for {
          id <- BatchId.fromProto(value.batchId)
          rpv <- protocolVersionRepresentativeFor(SupportedVersions.ProtoData)
        } yield Availability.RemoteOutputFetch.FetchRemoteBatchData(id, from)(
          rpv,
          deserializedFrom = Some(bytes),
        )

      def create(
          batchId: BatchId,
          from: BftNodeId,
      )(implicit synchronizerProtocolVersion: ProtocolVersion): FetchRemoteBatchData =
        FetchRemoteBatchData(batchId, from)(
          protocolVersionRepresentativeFor(synchronizerProtocolVersion),
          deserializedFrom = None,
        )

    }

    final case class RemoteBatchDataFetched private (
        from: BftNodeId,
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

      protected override def toProtoV30: v30.AvailabilityMessage =
        v30.AvailabilityMessage(
          v30.AvailabilityMessage.Message.BatchResponse(
            v30.BatchResponse(batchId.hash.getCryptographicEvidence, Some(batch.toProtoV30))
          )
        )

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object RemoteBatchDataFetched
        extends VersioningCompanionContextMemoization[
          RemoteBatchDataFetched,
          BftNodeId,
        ] {

      override def name: String = "RemoteBatchDataFetched"

      override def versioningTable: VersioningTable =
        VersioningTable(
          SupportedVersions.ProtoData ->
            VersionedProtoCodec(SupportedVersions.CantonProtocol)(v30.AvailabilityMessage)(
              supportedProtoVersionMemoized(_)(RemoteBatchDataFetched.fromProtoAvailabilityMessage),
              _.toProtoV30,
            )
        )

      def fromProtoAvailabilityMessage(
          from: BftNodeId,
          value: v30.AvailabilityMessage,
      )(bytes: ByteString): ParsingResult[RemoteBatchDataFetched] = for {
        protoRemoteBatchDataFetched <- value.message.batchResponse.toRight(
          ProtoDeserializationError.OtherError(s"Not a $name message")
        )
        remoteBatchDataFetched <- fromProtoV30(from, protoRemoteBatchDataFetched)(bytes)
      } yield remoteBatchDataFetched

      def fromProtoV30(
          from: BftNodeId,
          value: v30.BatchResponse,
      )(bytes: ByteString): ParsingResult[RemoteBatchDataFetched] =
        for {
          id <- BatchId.fromProto(value.batchId)
          batch <- value.batch match {
            case Some(batch) =>
              OrderingRequestBatch.fromProtoV30(batch)
            case None => Left(ProtoDeserializationError.FieldNotSet("batch"))
          }
          rpv <- protocolVersionRepresentativeFor(SupportedVersions.ProtoData)
        } yield Availability.RemoteOutputFetch.RemoteBatchDataFetched(from, id, batch)(
          rpv,
          deserializedFrom = Some(bytes),
        )

      def create(
          thisNode: BftNodeId,
          batchId: BatchId,
          batch: OrderingRequestBatch,
      )(implicit synchronizerProtocolVersion: ProtocolVersion): RemoteBatchDataFetched =
        RemoteBatchDataFetched(thisNode, batchId, batch)(
          protocolVersionRepresentativeFor(synchronizerProtocolVersion),
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
        orderedBatchIds: Seq[BatchId] = Seq.empty,
    ) extends Consensus[E]

    final case class UpdateTopologyDuringStateTransfer[E <: Env[E]](
        orderingTopology: OrderingTopology,
        cryptoProvider: CryptoProvider[E],
    ) extends Consensus[E]

    final case class Ordered(batchIds: Seq[BatchId]) extends Consensus[Nothing]

    final case object LocalClockTick extends Consensus[Nothing]
  }
}

trait Availability[E <: Env[E]] extends Module[E, Availability.Message[E]] {
  val dependencies: AvailabilityModuleDependencies[E]
}
