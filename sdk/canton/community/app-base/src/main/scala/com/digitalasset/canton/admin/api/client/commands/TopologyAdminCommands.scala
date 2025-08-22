// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.admin.api.client.data.*
import com.digitalasset.canton.admin.api.client.data.topology.*
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Fingerprint, Hash}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.{BaseQuery, TopologyStoreId}
import com.digitalasset.canton.topology.admin.v30
import com.digitalasset.canton.topology.admin.v30.*
import com.digitalasset.canton.topology.admin.v30.AuthorizeRequest.Type.{Proposal, TransactionHash}
import com.digitalasset.canton.topology.admin.v30.IdentityInitializationServiceGrpc.IdentityInitializationServiceStub
import com.digitalasset.canton.topology.admin.v30.TopologyAggregationServiceGrpc.TopologyAggregationServiceStub
import com.digitalasset.canton.topology.admin.v30.TopologyManagerReadServiceGrpc.TopologyManagerReadServiceStub
import com.digitalasset.canton.topology.admin.v30.TopologyManagerWriteServiceGrpc.TopologyManagerWriteServiceStub
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyMapping,
  TopologyTransaction,
}
import com.digitalasset.canton.util.GrpcStreamingUtils
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import io.grpc.Context.CancellableContext
import io.grpc.stub.StreamObserver
import io.grpc.{Context, ManagedChannel}

import java.time.Instant
import scala.concurrent.Future
import scala.reflect.ClassTag

object TopologyAdminCommands {

  object Read {

    abstract class BaseCommand[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = TopologyManagerReadServiceStub
      override def createService(channel: ManagedChannel): TopologyManagerReadServiceStub =
        v30.TopologyManagerReadServiceGrpc.stub(channel)

      //  command will potentially take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ListNamespaceDelegation(
        query: BaseQuery,
        filterNamespace: String,
        filterTargetKey: Option[Fingerprint],
    ) extends BaseCommand[
          v30.ListNamespaceDelegationRequest,
          v30.ListNamespaceDelegationResponse,
          Seq[ListNamespaceDelegationResult],
        ] {

      override protected def createRequest(): Either[String, v30.ListNamespaceDelegationRequest] =
        Right(
          new v30.ListNamespaceDelegationRequest(
            baseQuery = Some(query.toProtoV1),
            filterNamespace = filterNamespace,
            filterTargetKeyFingerprint = filterTargetKey.map(_.toProtoPrimitive).getOrElse(""),
          )
        )

      override protected def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v30.ListNamespaceDelegationRequest,
      ): Future[v30.ListNamespaceDelegationResponse] =
        service.listNamespaceDelegation(request)

      override protected def handleResponse(
          response: v30.ListNamespaceDelegationResponse
      ): Either[String, Seq[ListNamespaceDelegationResult]] =
        response.results.traverse(ListNamespaceDelegationResult.fromProtoV30).leftMap(_.toString)
    }

    final case class ListDecentralizedNamespaceDefinition(
        query: BaseQuery,
        filterNamespace: String,
    ) extends BaseCommand[
          v30.ListDecentralizedNamespaceDefinitionRequest,
          v30.ListDecentralizedNamespaceDefinitionResponse,
          Seq[ListDecentralizedNamespaceDefinitionResult],
        ] {

      override protected def createRequest()
          : Either[String, v30.ListDecentralizedNamespaceDefinitionRequest] =
        Right(
          new v30.ListDecentralizedNamespaceDefinitionRequest(
            baseQuery = Some(query.toProtoV1),
            filterNamespace = filterNamespace,
          )
        )

      override protected def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v30.ListDecentralizedNamespaceDefinitionRequest,
      ): Future[v30.ListDecentralizedNamespaceDefinitionResponse] =
        service.listDecentralizedNamespaceDefinition(request)

      override protected def handleResponse(
          response: v30.ListDecentralizedNamespaceDefinitionResponse
      ): Either[String, Seq[ListDecentralizedNamespaceDefinitionResult]] =
        response.results
          .traverse(ListDecentralizedNamespaceDefinitionResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class ListOwnerToKeyMapping(
        query: BaseQuery,
        filterKeyOwnerType: Option[MemberCode],
        filterKeyOwnerUid: String,
    ) extends BaseCommand[v30.ListOwnerToKeyMappingRequest, v30.ListOwnerToKeyMappingResponse, Seq[
          ListOwnerToKeyMappingResult
        ]] {

      override protected def createRequest(): Either[String, v30.ListOwnerToKeyMappingRequest] =
        Right(
          new v30.ListOwnerToKeyMappingRequest(
            baseQuery = Some(query.toProtoV1),
            filterKeyOwnerType = filterKeyOwnerType.map(_.toProtoPrimitive).getOrElse(""),
            filterKeyOwnerUid = filterKeyOwnerUid,
          )
        )

      override protected def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v30.ListOwnerToKeyMappingRequest,
      ): Future[v30.ListOwnerToKeyMappingResponse] =
        service.listOwnerToKeyMapping(request)

      override protected def handleResponse(
          response: v30.ListOwnerToKeyMappingResponse
      ): Either[String, Seq[ListOwnerToKeyMappingResult]] =
        response.results.traverse(ListOwnerToKeyMappingResult.fromProtoV30).leftMap(_.toString)
    }

    final case class ListPartyToKeyMapping(
        query: BaseQuery,
        filterParty: String,
    ) extends BaseCommand[v30.ListPartyToKeyMappingRequest, v30.ListPartyToKeyMappingResponse, Seq[
          ListPartyToKeyMappingResult
        ]] {

      override protected def createRequest(): Either[String, v30.ListPartyToKeyMappingRequest] =
        Right(
          new v30.ListPartyToKeyMappingRequest(
            baseQuery = Some(query.toProtoV1),
            filterParty = filterParty,
          )
        )

      override protected def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v30.ListPartyToKeyMappingRequest,
      ): Future[v30.ListPartyToKeyMappingResponse] =
        service.listPartyToKeyMapping(request)

      override protected def handleResponse(
          response: v30.ListPartyToKeyMappingResponse
      ): Either[String, Seq[ListPartyToKeyMappingResult]] =
        response.results.traverse(ListPartyToKeyMappingResult.fromProtoV30).leftMap(_.toString)
    }

    final case class ListSynchronizerTrustCertificate(
        query: BaseQuery,
        filterUid: String,
    ) extends BaseCommand[
          v30.ListSynchronizerTrustCertificateRequest,
          v30.ListSynchronizerTrustCertificateResponse,
          Seq[ListSynchronizerTrustCertificateResult],
        ] {

      override protected def createRequest()
          : Either[String, v30.ListSynchronizerTrustCertificateRequest] =
        Right(
          new v30.ListSynchronizerTrustCertificateRequest(
            baseQuery = Some(query.toProtoV1),
            filterUid = filterUid,
          )
        )

      override protected def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v30.ListSynchronizerTrustCertificateRequest,
      ): Future[v30.ListSynchronizerTrustCertificateResponse] =
        service.listSynchronizerTrustCertificate(request)

      override protected def handleResponse(
          response: v30.ListSynchronizerTrustCertificateResponse
      ): Either[String, Seq[ListSynchronizerTrustCertificateResult]] =
        response.results
          .traverse(ListSynchronizerTrustCertificateResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class ListParticipantSynchronizerPermission(
        query: BaseQuery,
        filterUid: String,
    ) extends BaseCommand[
          v30.ListParticipantSynchronizerPermissionRequest,
          v30.ListParticipantSynchronizerPermissionResponse,
          Seq[ListParticipantSynchronizerPermissionResult],
        ] {

      override protected def createRequest()
          : Either[String, v30.ListParticipantSynchronizerPermissionRequest] =
        Right(
          new v30.ListParticipantSynchronizerPermissionRequest(
            baseQuery = Some(query.toProtoV1),
            filterUid = filterUid,
          )
        )

      override protected def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v30.ListParticipantSynchronizerPermissionRequest,
      ): Future[v30.ListParticipantSynchronizerPermissionResponse] =
        service.listParticipantSynchronizerPermission(request)

      override protected def handleResponse(
          response: v30.ListParticipantSynchronizerPermissionResponse
      ): Either[String, Seq[ListParticipantSynchronizerPermissionResult]] =
        response.results
          .traverse(ListParticipantSynchronizerPermissionResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class ListPartyHostingLimits(
        query: BaseQuery,
        filterUid: String,
    ) extends BaseCommand[
          v30.ListPartyHostingLimitsRequest,
          v30.ListPartyHostingLimitsResponse,
          Seq[ListPartyHostingLimitsResult],
        ] {

      override protected def createRequest(): Either[String, v30.ListPartyHostingLimitsRequest] =
        Right(
          new v30.ListPartyHostingLimitsRequest(
            baseQuery = Some(query.toProtoV1),
            filterUid = filterUid,
          )
        )

      override protected def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v30.ListPartyHostingLimitsRequest,
      ): Future[v30.ListPartyHostingLimitsResponse] =
        service.listPartyHostingLimits(request)

      override protected def handleResponse(
          response: v30.ListPartyHostingLimitsResponse
      ): Either[String, Seq[ListPartyHostingLimitsResult]] =
        response.results
          .traverse(ListPartyHostingLimitsResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class ListVettedPackages(
        query: BaseQuery,
        filterParticipant: String,
    ) extends BaseCommand[
          v30.ListVettedPackagesRequest,
          v30.ListVettedPackagesResponse,
          Seq[ListVettedPackagesResult],
        ] {

      override protected def createRequest(): Either[String, v30.ListVettedPackagesRequest] =
        Right(
          new v30.ListVettedPackagesRequest(
            baseQuery = Some(query.toProtoV1),
            filterParticipant = filterParticipant,
          )
        )

      override protected def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v30.ListVettedPackagesRequest,
      ): Future[v30.ListVettedPackagesResponse] =
        service.listVettedPackages(request)

      override protected def handleResponse(
          response: v30.ListVettedPackagesResponse
      ): Either[String, Seq[ListVettedPackagesResult]] =
        response.results
          .traverse(ListVettedPackagesResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class ListPartyToParticipant(
        query: BaseQuery,
        filterParty: String,
        filterParticipant: String,
    ) extends BaseCommand[
          v30.ListPartyToParticipantRequest,
          v30.ListPartyToParticipantResponse,
          Seq[ListPartyToParticipantResult],
        ] {

      override protected def createRequest(): Either[String, v30.ListPartyToParticipantRequest] =
        Right(
          new v30.ListPartyToParticipantRequest(
            baseQuery = Some(query.toProtoV1),
            filterParty = filterParty,
            filterParticipant = filterParticipant,
          )
        )

      override protected def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v30.ListPartyToParticipantRequest,
      ): Future[v30.ListPartyToParticipantResponse] =
        service.listPartyToParticipant(request)

      override protected def handleResponse(
          response: v30.ListPartyToParticipantResponse
      ): Either[String, Seq[ListPartyToParticipantResult]] =
        response.results
          .traverse(ListPartyToParticipantResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class ListSynchronizerParametersState(
        query: BaseQuery,
        filterSynchronizerId: String,
    ) extends BaseCommand[
          v30.ListSynchronizerParametersStateRequest,
          v30.ListSynchronizerParametersStateResponse,
          Seq[ListSynchronizerParametersStateResult],
        ] {

      override protected def createRequest()
          : Either[String, v30.ListSynchronizerParametersStateRequest] =
        Right(
          new v30.ListSynchronizerParametersStateRequest(
            baseQuery = Some(query.toProtoV1),
            filterSynchronizerId = filterSynchronizerId,
          )
        )

      override protected def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v30.ListSynchronizerParametersStateRequest,
      ): Future[v30.ListSynchronizerParametersStateResponse] =
        service.listSynchronizerParametersState(request)

      override protected def handleResponse(
          response: v30.ListSynchronizerParametersStateResponse
      ): Either[String, Seq[ListSynchronizerParametersStateResult]] =
        response.results
          .traverse(ListSynchronizerParametersStateResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class ListMediatorSynchronizerState(
        query: BaseQuery,
        filterSynchronizerId: String,
    ) extends BaseCommand[
          v30.ListMediatorSynchronizerStateRequest,
          v30.ListMediatorSynchronizerStateResponse,
          Seq[ListMediatorSynchronizerStateResult],
        ] {

      override protected def createRequest()
          : Either[String, v30.ListMediatorSynchronizerStateRequest] =
        Right(
          v30.ListMediatorSynchronizerStateRequest(
            baseQuery = Some(query.toProtoV1),
            filterSynchronizerId = filterSynchronizerId,
          )
        )

      override protected def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v30.ListMediatorSynchronizerStateRequest,
      ): Future[v30.ListMediatorSynchronizerStateResponse] =
        service.listMediatorSynchronizerState(request)

      override protected def handleResponse(
          response: v30.ListMediatorSynchronizerStateResponse
      ): Either[String, Seq[ListMediatorSynchronizerStateResult]] =
        response.results
          .traverse(ListMediatorSynchronizerStateResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class ListSequencerSynchronizerState(
        query: BaseQuery,
        filterSynchronizerId: String,
    ) extends BaseCommand[
          v30.ListSequencerSynchronizerStateRequest,
          v30.ListSequencerSynchronizerStateResponse,
          Seq[ListSequencerSynchronizerStateResult],
        ] {

      override protected def createRequest()
          : Either[String, v30.ListSequencerSynchronizerStateRequest] =
        Right(
          new v30.ListSequencerSynchronizerStateRequest(
            baseQuery = Some(query.toProtoV1),
            filterSynchronizerId = filterSynchronizerId,
          )
        )

      override protected def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v30.ListSequencerSynchronizerStateRequest,
      ): Future[v30.ListSequencerSynchronizerStateResponse] =
        service.listSequencerSynchronizerState(request)

      override protected def handleResponse(
          response: v30.ListSequencerSynchronizerStateResponse
      ): Either[String, Seq[ListSequencerSynchronizerStateResult]] =
        response.results
          .traverse(ListSequencerSynchronizerStateResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class ListSynchronizerUpgradeAnnouncement(
        query: BaseQuery,
        filterSynchronizerId: String,
    ) extends BaseCommand[
          v30.ListSynchronizerUpgradeAnnouncementRequest,
          v30.ListSynchronizerUpgradeAnnouncementResponse,
          Seq[ListSynchronizerUpgradeAnnouncementResult],
        ] {

      override protected def createRequest()
          : Either[String, v30.ListSynchronizerUpgradeAnnouncementRequest] =
        Right(
          new ListSynchronizerUpgradeAnnouncementRequest(
            baseQuery = Some(query.toProtoV1),
            filterSynchronizerId = filterSynchronizerId,
          )
        )

      override protected def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v30.ListSynchronizerUpgradeAnnouncementRequest,
      ): Future[v30.ListSynchronizerUpgradeAnnouncementResponse] =
        service.listSynchronizerUpgradeAnnouncement(request)

      override protected def handleResponse(
          response: v30.ListSynchronizerUpgradeAnnouncementResponse
      ): Either[String, Seq[ListSynchronizerUpgradeAnnouncementResult]] =
        response.results
          .traverse(ListSynchronizerUpgradeAnnouncementResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class ListSequencerConnectionSuccessor(
        query: BaseQuery,
        filterSequencerId: String,
    ) extends BaseCommand[
          v30.ListSequencerConnectionSuccessorRequest,
          v30.ListSequencerConnectionSuccessorResponse,
          Seq[ListSequencerConnectionSuccessorResult],
        ] {

      override protected def createRequest()
          : Either[String, v30.ListSequencerConnectionSuccessorRequest] =
        Right(
          new ListSequencerConnectionSuccessorRequest(
            baseQuery = Some(query.toProtoV1),
            filterSequencerId = filterSequencerId,
          )
        )

      override protected def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v30.ListSequencerConnectionSuccessorRequest,
      ): Future[v30.ListSequencerConnectionSuccessorResponse] =
        service.listSequencerConnectionSuccessor(request)

      override protected def handleResponse(
          response: v30.ListSequencerConnectionSuccessorResponse
      ): Either[String, Seq[ListSequencerConnectionSuccessorResult]] =
        response.results
          .traverse(ListSequencerConnectionSuccessorResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class ListStores()
        extends BaseCommand[v30.ListAvailableStoresRequest, v30.ListAvailableStoresResponse, Seq[
          TopologyStoreId
        ]] {

      override protected def createRequest(): Either[String, v30.ListAvailableStoresRequest] =
        Right(v30.ListAvailableStoresRequest())

      override protected def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v30.ListAvailableStoresRequest,
      ): Future[v30.ListAvailableStoresResponse] =
        service.listAvailableStores(request)

      override protected def handleResponse(
          response: v30.ListAvailableStoresResponse
      ): Either[String, Seq[TopologyStoreId]] =
        response.storeIds.traverse(TopologyStoreId.fromProtoV30(_, "store_ids")).leftMap(_.message)
    }

    final case class ListAll(
        query: BaseQuery,
        excludeMappings: Seq[String],
        filterNamespace: String,
    ) extends BaseCommand[
          v30.ListAllRequest,
          v30.ListAllResponse,
          GenericStoredTopologyTransactions,
        ] {
      override protected def createRequest(): Either[String, v30.ListAllRequest] =
        Right(
          new v30.ListAllRequest(
            baseQuery = Some(query.toProtoV1),
            excludeMappings = excludeMappings,
            filterNamespace = filterNamespace,
          )
        )

      override protected def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v30.ListAllRequest,
      ): Future[v30.ListAllResponse] = service.listAll(request)

      override protected def handleResponse(
          response: v30.ListAllResponse
      ): Either[String, GenericStoredTopologyTransactions] =
        response.result
          .fold[Either[String, GenericStoredTopologyTransactions]](
            Right(StoredTopologyTransactions.empty)
          ) { collection =>
            StoredTopologyTransactions.fromProtoV30(collection).leftMap(_.toString)
          }
    }
    final case class ExportTopologySnapshot(
        observer: StreamObserver[ExportTopologySnapshotResponse],
        query: BaseQuery,
        excludeMappings: Seq[String],
        filterNamespace: String,
    ) extends BaseCommand[
          v30.ExportTopologySnapshotRequest,
          CancellableContext,
          CancellableContext,
        ] {
      override protected def createRequest(): Either[String, v30.ExportTopologySnapshotRequest] =
        Right(
          new v30.ExportTopologySnapshotRequest(
            baseQuery = Some(query.toProtoV1),
            excludeMappings = excludeMappings,
            filterNamespace = filterNamespace,
          )
        )

      override protected def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v30.ExportTopologySnapshotRequest,
      ): Future[CancellableContext] = {
        val context = Context.current().withCancellation()
        context.run(() => service.exportTopologySnapshot(request, observer))
        Future.successful(context)
      }

      override protected def handleResponse(
          response: CancellableContext
      ): Either[String, CancellableContext] =
        Right(response)

      override def timeoutType: TimeoutType = DefaultUnboundedTimeout
    }

    final case class GenesisState(
        observer: StreamObserver[GenesisStateResponse],
        synchronizerStore: Option[TopologyStoreId.Synchronizer],
        timestamp: Option[CantonTimestamp],
    ) extends BaseCommand[
          v30.GenesisStateRequest,
          CancellableContext,
          CancellableContext,
        ] {
      override protected def createRequest(): Either[String, v30.GenesisStateRequest] =
        Right(
          v30.GenesisStateRequest(
            synchronizerStore.map(_.toProtoV30),
            timestamp.map(_.toProtoTimestamp),
          )
        )

      override protected def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v30.GenesisStateRequest,
      ): Future[CancellableContext] = {
        val context = Context.current().withCancellation()
        context.run(() => service.genesisState(request, observer))
        Future.successful(context)
      }

      override protected def handleResponse(
          response: CancellableContext
      ): Either[String, CancellableContext] =
        Right(response)

      override def timeoutType: TimeoutType = DefaultUnboundedTimeout
    }

    final case class LogicalUpgradeState(
        observer: StreamObserver[LogicalUpgradeStateResponse]
    ) extends BaseCommand[
          v30.LogicalUpgradeStateRequest,
          CancellableContext,
          CancellableContext,
        ] {
      override protected def createRequest(): Either[String, v30.LogicalUpgradeStateRequest] =
        Right(
          v30.LogicalUpgradeStateRequest()
        )

      override protected def submitRequest(
          service: TopologyManagerReadServiceStub,
          request: v30.LogicalUpgradeStateRequest,
      ): Future[CancellableContext] = {
        val context = Context.current().withCancellation()
        context.run(() => service.logicalUpgradeState(request, observer))
        Future.successful(context)
      }

      override protected def handleResponse(
          response: CancellableContext
      ): Either[String, CancellableContext] =
        Right(response)

      override def timeoutType: TimeoutType = DefaultUnboundedTimeout
    }
  }

  object Aggregation {

    abstract class BaseCommand[Req, Res, Result] extends GrpcAdminCommand[Req, Res, Result] {
      override type Svc = TopologyAggregationServiceStub

      override def createService(channel: ManagedChannel): TopologyAggregationServiceStub =
        v30.TopologyAggregationServiceGrpc.stub(channel)
    }

    final case class ListParties(
        synchronizerIds: Set[SynchronizerId],
        filterParty: String,
        filterParticipant: String,
        asOf: Option[Instant],
        limit: PositiveInt,
    ) extends BaseCommand[v30.ListPartiesRequest, v30.ListPartiesResponse, Seq[
          ListPartiesResult
        ]] {

      override protected def createRequest(): Either[String, v30.ListPartiesRequest] =
        Right(
          v30.ListPartiesRequest(
            synchronizerIds = synchronizerIds.map(_.toProtoPrimitive).toSeq,
            filterParty = filterParty,
            filterParticipant = filterParticipant,
            asOf = asOf.map(ts => Timestamp(ts.getEpochSecond)),
            limit = limit.value,
          )
        )

      override protected def submitRequest(
          service: TopologyAggregationServiceStub,
          request: v30.ListPartiesRequest,
      ): Future[v30.ListPartiesResponse] =
        service.listParties(request)

      override protected def handleResponse(
          response: v30.ListPartiesResponse
      ): Either[String, Seq[ListPartiesResult]] =
        response.results.traverse(ListPartiesResult.fromProtoV30).leftMap(_.toString)

      //  command will potentially take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ListKeyOwners(
        synchronizerIds: Set[SynchronizerId],
        filterKeyOwnerType: Option[MemberCode],
        filterKeyOwnerUid: String,
        asOf: Option[Instant],
        limit: PositiveInt,
    ) extends BaseCommand[v30.ListKeyOwnersRequest, v30.ListKeyOwnersResponse, Seq[
          ListKeyOwnersResult
        ]] {

      override protected def createRequest(): Either[String, v30.ListKeyOwnersRequest] =
        Right(
          v30.ListKeyOwnersRequest(
            synchronizerIds = synchronizerIds.toSeq.map(_.toProtoPrimitive),
            filterKeyOwnerType = filterKeyOwnerType.map(_.toProtoPrimitive).getOrElse(""),
            filterKeyOwnerUid = filterKeyOwnerUid,
            asOf = asOf.map(ts => Timestamp(ts.getEpochSecond)),
            limit = limit.value,
          )
        )

      override protected def submitRequest(
          service: TopologyAggregationServiceStub,
          request: v30.ListKeyOwnersRequest,
      ): Future[v30.ListKeyOwnersResponse] =
        service.listKeyOwners(request)

      override protected def handleResponse(
          response: v30.ListKeyOwnersResponse
      ): Either[String, Seq[ListKeyOwnersResult]] =
        response.results.traverse(ListKeyOwnersResult.fromProtoV30).leftMap(_.toString)

      //  command will potentially take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }
  }

  object Write {
    abstract class BaseWriteCommand[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = TopologyManagerWriteServiceStub

      override def createService(channel: ManagedChannel): TopologyManagerWriteServiceStub =
        v30.TopologyManagerWriteServiceGrpc.stub(channel)

      //  command will potentially take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout
    }

    final case class AddTransactions(
        transactions: Seq[GenericSignedTopologyTransaction],
        store: TopologyStoreId,
        forceChanges: ForceFlags,
        waitToBecomeEffective: Option[NonNegativeDuration],
    ) extends BaseWriteCommand[AddTransactionsRequest, AddTransactionsResponse, Unit] {
      override protected def createRequest(): Either[String, AddTransactionsRequest] =
        Right(
          AddTransactionsRequest(
            transactions.map(_.toProtoV30),
            forceChanges = forceChanges.toProtoV30,
            Some(store.toProtoV30),
            waitToBecomeEffective.map(_.asNonNegativeFiniteApproximation.toProtoPrimitive),
          )
        )
      override protected def submitRequest(
          service: TopologyManagerWriteServiceStub,
          request: AddTransactionsRequest,
      ): Future[AddTransactionsResponse] = service.addTransactions(request)
      override protected def handleResponse(
          response: AddTransactionsResponse
      ): Either[String, Unit] =
        Either.unit
    }
    final case class ImportTopologySnapshot(
        topologySnapshot: ByteString,
        store: TopologyStoreId,
        waitToBecomeEffective: Option[NonNegativeDuration],
    ) extends BaseWriteCommand[
          ImportTopologySnapshotRequest,
          ImportTopologySnapshotResponse,
          Unit,
        ] {
      override protected def createRequest(): Either[String, ImportTopologySnapshotRequest] =
        Right(
          ImportTopologySnapshotRequest(
            topologySnapshot,
            Some(store.toProtoV30),
            waitToBecomeEffective.map(_.asNonNegativeFiniteApproximation.toProtoPrimitive),
          )
        )
      override protected def submitRequest(
          service: TopologyManagerWriteServiceStub,
          request: ImportTopologySnapshotRequest,
      ): Future[ImportTopologySnapshotResponse] =
        GrpcStreamingUtils.streamToServer(
          service.importTopologySnapshot,
          bytes =>
            ImportTopologySnapshotRequest(
              ByteString.copyFrom(bytes),
              Some(store.toProtoV30),
              waitToBecomeEffective.map(_.toProtoPrimitive),
            ),
          topologySnapshot,
        )
      override protected def handleResponse(
          response: ImportTopologySnapshotResponse
      ): Either[String, Unit] = Either.unit
    }

    final case class SignTransactions(
        transactions: Seq[GenericSignedTopologyTransaction],
        store: TopologyStoreId,
        signedBy: Seq[Fingerprint],
        forceFlags: ForceFlags,
    ) extends BaseWriteCommand[SignTransactionsRequest, SignTransactionsResponse, Seq[
          GenericSignedTopologyTransaction
        ]] {
      override protected def createRequest(): Either[String, SignTransactionsRequest] =
        Right(
          SignTransactionsRequest(
            transactions.map(_.toProtoV30),
            signedBy.map(_.toProtoPrimitive),
            Some(store.toProtoV30),
            forceFlags.toProtoV30,
          )
        )

      override protected def submitRequest(
          service: TopologyManagerWriteServiceStub,
          request: SignTransactionsRequest,
      ): Future[SignTransactionsResponse] = service.signTransactions(request)

      override protected def handleResponse(
          response: SignTransactionsResponse
      ): Either[String, Seq[GenericSignedTopologyTransaction]] =
        response.transactions
          .traverse(tx =>
            SignedTopologyTransaction.fromProtoV30(ProtocolVersionValidation.NoValidation, tx)
          )
          .leftMap(_.message)
    }

    final case class GenerateTransactions(
        proposals: Seq[GenerateTransactions.Proposal]
    ) extends BaseWriteCommand[
          GenerateTransactionsRequest,
          GenerateTransactionsResponse,
          Seq[TopologyTransaction[TopologyChangeOp, TopologyMapping]],
        ] {

      override protected def createRequest(): Either[String, GenerateTransactionsRequest] =
        Right(GenerateTransactionsRequest(proposals.map(_.toGenerateTransactionProposal)))
      override protected def submitRequest(
          service: TopologyManagerWriteServiceStub,
          request: GenerateTransactionsRequest,
      ): Future[GenerateTransactionsResponse] = service.generateTransactions(request)

      override protected def handleResponse(
          response: GenerateTransactionsResponse
      ): Either[String, Seq[TopologyTransaction[TopologyChangeOp, TopologyMapping]]] =
        response.generatedTransactions
          .traverse { generatedTransaction =>
            val serializedTransaction = generatedTransaction.serializedTransaction
            val serializedHash = generatedTransaction.transactionHash
            for {
              parsedTopologyTransaction <-
                TopologyTransaction
                  .fromByteString(ProtocolVersionValidation.NoValidation, serializedTransaction)
                  .leftMap(_.message)
              // We don't really need the hash from the response here because we can re-build it from the deserialized
              // topology transaction. But users of the API without access to this code wouldn't be able to do that,
              // which is why the hash is returned by the API. Let's still verify that they match here.
              parsedHash <- Hash.fromByteString(serializedHash).leftMap(_.message)
              _ = Either.cond(
                parsedTopologyTransaction.hash.hash.compare(parsedHash) == 0,
                (),
                s"Response hash did not match transaction hash",
              )
            } yield parsedTopologyTransaction
          }
    }
    object GenerateTransactions {
      final case class Proposal(
          mapping: TopologyMapping,
          store: TopologyStoreId,
          change: TopologyChangeOp = TopologyChangeOp.Replace,
          serial: Option[PositiveInt] = None,
      ) {
        def toGenerateTransactionProposal: GenerateTransactionsRequest.Proposal =
          GenerateTransactionsRequest.Proposal(
            change.toProto,
            serial.map(_.value).getOrElse(0),
            Some(mapping.toProtoV30),
            Some(store.toProtoV30),
          )
      }
    }

    final case class Propose[M <: TopologyMapping: ClassTag](
        mapping: Either[String, M],
        signedBy: Seq[Fingerprint],
        change: TopologyChangeOp,
        serial: Option[PositiveInt],
        mustFullyAuthorize: Boolean,
        forceChanges: ForceFlags,
        store: TopologyStoreId,
        waitToBecomeEffective: Option[NonNegativeDuration],
    ) extends BaseWriteCommand[
          AuthorizeRequest,
          AuthorizeResponse,
          SignedTopologyTransaction[TopologyChangeOp, M],
        ] {

      override protected def createRequest(): Either[String, AuthorizeRequest] = mapping.map(m =>
        AuthorizeRequest(
          Proposal(
            AuthorizeRequest.Proposal(
              change.toProto,
              serial.map(_.value).getOrElse(0),
              Some(m.toProtoV30),
            )
          ),
          mustFullyAuthorize = mustFullyAuthorize,
          forceChanges = forceChanges.toProtoV30,
          signedBy = signedBy.map(_.toProtoPrimitive),
          store = Some(store.toProtoV30),
          waitToBecomeEffective =
            waitToBecomeEffective.map(_.asNonNegativeFiniteApproximation.toProtoPrimitive),
        )
      )
      override protected def submitRequest(
          service: TopologyManagerWriteServiceStub,
          request: AuthorizeRequest,
      ): Future[AuthorizeResponse] = service.authorize(request)

      override protected def handleResponse(
          response: AuthorizeResponse
      ): Either[String, SignedTopologyTransaction[TopologyChangeOp, M]] = response.transaction
        .toRight("no transaction in response")
        .flatMap(
          SignedTopologyTransaction
            .fromProtoV30(ProtocolVersionValidation.NoValidation, _)
            .leftMap(_.message)
            .flatMap(tx =>
              tx.selectMapping[M]
                .toRight(
                  s"Expected mapping ${ClassTag[M].getClass.getSimpleName}, but received: ${tx.mapping.getClass.getSimpleName}"
                )
            )
        )
    }
    object Propose {
      def apply[M <: TopologyMapping: ClassTag](
          mapping: M,
          signedBy: Seq[Fingerprint],
          store: TopologyStoreId,
          serial: Option[PositiveInt] = None,
          change: TopologyChangeOp = TopologyChangeOp.Replace,
          mustFullyAuthorize: Boolean = false,
          forceChanges: ForceFlags = ForceFlags.none,
          waitToBecomeEffective: Option[NonNegativeDuration],
      ): Propose[M] =
        Propose(
          Right(mapping),
          signedBy,
          change,
          serial,
          mustFullyAuthorize,
          forceChanges,
          store,
          waitToBecomeEffective,
        )

    }

    final case class Authorize[M <: TopologyMapping: ClassTag](
        transactionHash: String,
        mustFullyAuthorize: Boolean,
        signedBy: Seq[Fingerprint],
        store: TopologyStoreId,
        waitToBecomeEffective: Option[NonNegativeDuration] = None,
    ) extends BaseWriteCommand[
          AuthorizeRequest,
          AuthorizeResponse,
          SignedTopologyTransaction[TopologyChangeOp, M],
        ] {

      override protected def createRequest(): Either[String, AuthorizeRequest] = Right(
        AuthorizeRequest(
          TransactionHash(transactionHash),
          mustFullyAuthorize = mustFullyAuthorize,
          forceChanges = Seq.empty,
          signedBy = signedBy.map(_.toProtoPrimitive),
          store = Some(store.toProtoV30),
          waitToBecomeEffective.map(_.asNonNegativeFiniteApproximation.toProtoPrimitive),
        )
      )

      override protected def submitRequest(
          service: TopologyManagerWriteServiceStub,
          request: AuthorizeRequest,
      ): Future[AuthorizeResponse] = service.authorize(request)

      override protected def handleResponse(
          response: AuthorizeResponse
      ): Either[String, SignedTopologyTransaction[TopologyChangeOp, M]] = response.transaction
        .toRight("no transaction in response")
        .flatMap(
          SignedTopologyTransaction
            .fromProtoV30(ProtocolVersionValidation.NoValidation, _)
            .leftMap(_.message)
            .flatMap(tx =>
              tx.selectMapping[M]
                .toRight(
                  s"Expected mapping ${ClassTag[M].getClass.getSimpleName}, but received: ${tx.mapping.getClass.getSimpleName}"
                )
            )
        )
    }

    final case class CreateTemporaryStore(name: String, protocolVersion: ProtocolVersion)
        extends BaseWriteCommand[
          v30.CreateTemporaryTopologyStoreRequest,
          v30.CreateTemporaryTopologyStoreResponse,
          TopologyStoreId.Temporary,
        ] {
      override protected def createRequest()
          : Either[String, v30.CreateTemporaryTopologyStoreRequest] = Right(
        v30.CreateTemporaryTopologyStoreRequest(name, protocolVersion.toProtoPrimitive)
      )

      override protected def submitRequest(
          service: TopologyManagerWriteServiceStub,
          request: CreateTemporaryTopologyStoreRequest,
      ): Future[CreateTemporaryTopologyStoreResponse] =
        service.createTemporaryTopologyStore(request)

      override protected def handleResponse(
          response: CreateTemporaryTopologyStoreResponse
      ): Either[String, TopologyStoreId.Temporary] =
        ProtoConverter
          .parseRequired(
            TopologyStoreId.Temporary.fromProtoV30,
            "store_id",
            response.storeId,
          )
          .leftMap(_.toString)
    }

    final case class DropTemporaryStore(temporaryStoreId: TopologyStoreId.Temporary)
        extends BaseWriteCommand[
          v30.DropTemporaryTopologyStoreRequest,
          v30.DropTemporaryTopologyStoreResponse,
          Unit,
        ] {
      override protected def createRequest()
          : Either[String, v30.DropTemporaryTopologyStoreRequest] = Right(
        v30.DropTemporaryTopologyStoreRequest(Some(temporaryStoreId.toProtoV30.getTemporary))
      )

      override protected def submitRequest(
          service: TopologyManagerWriteServiceStub,
          request: DropTemporaryTopologyStoreRequest,
      ): Future[DropTemporaryTopologyStoreResponse] =
        service.dropTemporaryTopologyStore(request)

      override protected def handleResponse(
          response: DropTemporaryTopologyStoreResponse
      ): Either[String, Unit] =
        Right(())
    }

  }

  object Init {

    abstract class BaseInitializationService[Req, Resp, Res]
        extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = IdentityInitializationServiceStub
      override def createService(channel: ManagedChannel): IdentityInitializationServiceStub =
        v30.IdentityInitializationServiceGrpc.stub(channel)
    }

    final case class InitId(
        identifier: String,
        namespace: String,
        delegations: Seq[GenericSignedTopologyTransaction],
    ) extends BaseInitializationService[v30.InitIdRequest, v30.InitIdResponse, Unit] {

      override protected def createRequest(): Either[String, v30.InitIdRequest] =
        Right(v30.InitIdRequest(identifier, namespace, delegations.map(_.toProtoV30)))

      override protected def submitRequest(
          service: IdentityInitializationServiceStub,
          request: v30.InitIdRequest,
      ): Future[v30.InitIdResponse] =
        service.initId(request)

      override protected def handleResponse(response: v30.InitIdResponse): Either[String, Unit] =
        Either.unit
    }

    final case class GetId()
        extends BaseInitializationService[v30.GetIdRequest, v30.GetIdResponse, UniqueIdentifier] {
      override protected def createRequest(): Either[String, v30.GetIdRequest] =
        Right(v30.GetIdRequest())

      override protected def submitRequest(
          service: IdentityInitializationServiceStub,
          request: v30.GetIdRequest,
      ): Future[v30.GetIdResponse] =
        service.getId(request)

      override protected def handleResponse(
          response: v30.GetIdResponse
      ): Either[String, UniqueIdentifier] =
        if (response.uniqueIdentifier.nonEmpty)
          UniqueIdentifier.fromProtoPrimitive_(response.uniqueIdentifier).leftMap(_.message)
        else
          Left(
            s"Node is not initialized and therefore does not have an Id assigned yet."
          )
    }
  }
}
