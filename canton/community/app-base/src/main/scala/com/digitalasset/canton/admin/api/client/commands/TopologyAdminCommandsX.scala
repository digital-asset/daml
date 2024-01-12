// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.admin.api.client.data.topologyx.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.BaseQueryX
import com.digitalasset.canton.topology.admin.v1
import com.digitalasset.canton.topology.admin.v1.AuthorizeRequest.Type.{Proposal, TransactionHash}
import com.digitalasset.canton.topology.admin.v1.IdentityInitializationServiceXGrpc.IdentityInitializationServiceXStub
import com.digitalasset.canton.topology.admin.v1.TopologyManagerReadServiceXGrpc.TopologyManagerReadServiceXStub
import com.digitalasset.canton.topology.admin.v1.TopologyManagerWriteServiceXGrpc.TopologyManagerWriteServiceXStub
import com.digitalasset.canton.topology.admin.v1.{
  AddTransactionsRequest,
  AddTransactionsResponse,
  AuthorizeRequest,
  AuthorizeResponse,
  ListTrafficStateRequest,
  SignTransactionsRequest,
  SignTransactionsResponse,
}
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.GenericStoredTopologyTransactionsX
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.{
  SignedTopologyTransactionX,
  TopologyChangeOpX,
  TopologyMappingX,
}
import com.digitalasset.canton.version.ProtocolVersionValidation
import com.google.protobuf.empty.Empty
import io.grpc.ManagedChannel

import scala.concurrent.Future
import scala.reflect.ClassTag

object TopologyAdminCommandsX {

  object Read {

    abstract class BaseCommand[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = TopologyManagerReadServiceXStub
      override def createService(channel: ManagedChannel): TopologyManagerReadServiceXStub =
        v1.TopologyManagerReadServiceXGrpc.stub(channel)

      //  command will potentially take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ListTrafficControlState(
        query: BaseQueryX,
        filterMember: String,
    ) extends BaseCommand[
          v1.ListTrafficStateRequest,
          v1.ListTrafficStateResult,
          Seq[ListTrafficStateResult],
        ] {

      override def createRequest(): Either[String, v1.ListTrafficStateRequest] =
        Right(
          new ListTrafficStateRequest(
            baseQuery = Some(query.toProtoV1),
            filterMember = filterMember,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v1.ListTrafficStateRequest,
      ): Future[v1.ListTrafficStateResult] =
        service.listTrafficState(request)

      override def handleResponse(
          response: v1.ListTrafficStateResult
      ): Either[String, Seq[ListTrafficStateResult]] =
        response.results
          .traverse(ListTrafficStateResult.fromProtoV1)
          .leftMap(_.toString)
    }

    final case class ListNamespaceDelegation(
        query: BaseQueryX,
        filterNamespace: String,
        filterTargetKey: Option[Fingerprint],
    ) extends BaseCommand[
          v1.ListNamespaceDelegationRequest,
          v1.ListNamespaceDelegationResult,
          Seq[ListNamespaceDelegationResult],
        ] {

      override def createRequest(): Either[String, v1.ListNamespaceDelegationRequest] =
        Right(
          new v1.ListNamespaceDelegationRequest(
            baseQuery = Some(query.toProtoV1),
            filterNamespace = filterNamespace,
            filterTargetKeyFingerprint = filterTargetKey.map(_.toProtoPrimitive).getOrElse(""),
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v1.ListNamespaceDelegationRequest,
      ): Future[v1.ListNamespaceDelegationResult] =
        service.listNamespaceDelegation(request)

      override def handleResponse(
          response: v1.ListNamespaceDelegationResult
      ): Either[String, Seq[ListNamespaceDelegationResult]] =
        response.results.traverse(ListNamespaceDelegationResult.fromProtoV1).leftMap(_.toString)
    }

    final case class ListUnionspaceDefinition(
        query: BaseQueryX,
        filterNamespace: String,
    ) extends BaseCommand[
          v1.ListUnionspaceDefinitionRequest,
          v1.ListUnionspaceDefinitionResult,
          Seq[ListUnionspaceDefinitionResult],
        ] {

      override def createRequest(): Either[String, v1.ListUnionspaceDefinitionRequest] =
        Right(
          new v1.ListUnionspaceDefinitionRequest(
            baseQuery = Some(query.toProtoV1),
            filterNamespace = filterNamespace,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v1.ListUnionspaceDefinitionRequest,
      ): Future[v1.ListUnionspaceDefinitionResult] =
        service.listUnionspaceDefinition(request)

      override def handleResponse(
          response: v1.ListUnionspaceDefinitionResult
      ): Either[String, Seq[ListUnionspaceDefinitionResult]] =
        response.results.traverse(ListUnionspaceDefinitionResult.fromProtoV1).leftMap(_.toString)
    }

    final case class ListIdentifierDelegation(
        query: BaseQueryX,
        filterUid: String,
        filterTargetKey: Option[Fingerprint],
    ) extends BaseCommand[
          v1.ListIdentifierDelegationRequest,
          v1.ListIdentifierDelegationResult,
          Seq[ListIdentifierDelegationResult],
        ] {

      override def createRequest(): Either[String, v1.ListIdentifierDelegationRequest] =
        Right(
          new v1.ListIdentifierDelegationRequest(
            baseQuery = Some(query.toProtoV1),
            filterUid = filterUid,
            filterTargetKeyFingerprint = filterTargetKey.map(_.toProtoPrimitive).getOrElse(""),
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v1.ListIdentifierDelegationRequest,
      ): Future[v1.ListIdentifierDelegationResult] =
        service.listIdentifierDelegation(request)

      override def handleResponse(
          response: v1.ListIdentifierDelegationResult
      ): Either[String, Seq[ListIdentifierDelegationResult]] =
        response.results.traverse(ListIdentifierDelegationResult.fromProtoV1).leftMap(_.toString)
    }

    final case class ListOwnerToKeyMapping(
        query: BaseQueryX,
        filterKeyOwnerType: Option[KeyOwnerCode],
        filterKeyOwnerUid: String,
    ) extends BaseCommand[v1.ListOwnerToKeyMappingRequest, v1.ListOwnerToKeyMappingResult, Seq[
          ListOwnerToKeyMappingResult
        ]] {

      override def createRequest(): Either[String, v1.ListOwnerToKeyMappingRequest] =
        Right(
          new v1.ListOwnerToKeyMappingRequest(
            baseQuery = Some(query.toProtoV1),
            filterKeyOwnerType = filterKeyOwnerType.map(_.toProtoPrimitive).getOrElse(""),
            filterKeyOwnerUid = filterKeyOwnerUid,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v1.ListOwnerToKeyMappingRequest,
      ): Future[v1.ListOwnerToKeyMappingResult] =
        service.listOwnerToKeyMapping(request)

      override def handleResponse(
          response: v1.ListOwnerToKeyMappingResult
      ): Either[String, Seq[ListOwnerToKeyMappingResult]] =
        response.results.traverse(ListOwnerToKeyMappingResult.fromProtoV1).leftMap(_.toString)
    }

    final case class ListDomainTrustCertificate(
        query: BaseQueryX,
        filterUid: String,
    ) extends BaseCommand[
          v1.ListDomainTrustCertificateRequest,
          v1.ListDomainTrustCertificateResult,
          Seq[ListDomainTrustCertificateResult],
        ] {

      override def createRequest(): Either[String, v1.ListDomainTrustCertificateRequest] =
        Right(
          new v1.ListDomainTrustCertificateRequest(
            baseQuery = Some(query.toProtoV1),
            filterUid = filterUid,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v1.ListDomainTrustCertificateRequest,
      ): Future[v1.ListDomainTrustCertificateResult] =
        service.listDomainTrustCertificate(request)

      override def handleResponse(
          response: v1.ListDomainTrustCertificateResult
      ): Either[String, Seq[ListDomainTrustCertificateResult]] =
        response.results.traverse(ListDomainTrustCertificateResult.fromProtoV1).leftMap(_.toString)
    }

    final case class ListParticipantDomainPermission(
        query: BaseQueryX,
        filterUid: String,
    ) extends BaseCommand[
          v1.ListParticipantDomainPermissionRequest,
          v1.ListParticipantDomainPermissionResult,
          Seq[ListParticipantDomainPermissionResult],
        ] {

      override def createRequest(): Either[String, v1.ListParticipantDomainPermissionRequest] =
        Right(
          new v1.ListParticipantDomainPermissionRequest(
            baseQuery = Some(query.toProtoV1),
            filterUid = filterUid,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v1.ListParticipantDomainPermissionRequest,
      ): Future[v1.ListParticipantDomainPermissionResult] =
        service.listParticipantDomainPermission(request)

      override def handleResponse(
          response: v1.ListParticipantDomainPermissionResult
      ): Either[String, Seq[ListParticipantDomainPermissionResult]] =
        response.results
          .traverse(ListParticipantDomainPermissionResult.fromProtoV1)
          .leftMap(_.toString)
    }

    final case class ListPartyHostingLimits(
        query: BaseQueryX,
        filterUid: String,
    ) extends BaseCommand[
          v1.ListPartyHostingLimitsRequest,
          v1.ListPartyHostingLimitsResult,
          Seq[ListPartyHostingLimitsResult],
        ] {

      override def createRequest(): Either[String, v1.ListPartyHostingLimitsRequest] =
        Right(
          new v1.ListPartyHostingLimitsRequest(
            baseQuery = Some(query.toProtoV1),
            filterUid = filterUid,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v1.ListPartyHostingLimitsRequest,
      ): Future[v1.ListPartyHostingLimitsResult] =
        service.listPartyHostingLimits(request)

      override def handleResponse(
          response: v1.ListPartyHostingLimitsResult
      ): Either[String, Seq[ListPartyHostingLimitsResult]] =
        response.results
          .traverse(ListPartyHostingLimitsResult.fromProtoV1)
          .leftMap(_.toString)
    }

    final case class ListVettedPackages(
        query: BaseQueryX,
        filterParticipant: String,
    ) extends BaseCommand[
          v1.ListVettedPackagesRequest,
          v1.ListVettedPackagesResult,
          Seq[ListVettedPackagesResult],
        ] {

      override def createRequest(): Either[String, v1.ListVettedPackagesRequest] =
        Right(
          new v1.ListVettedPackagesRequest(
            baseQuery = Some(query.toProtoV1),
            filterParticipant = filterParticipant,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v1.ListVettedPackagesRequest,
      ): Future[v1.ListVettedPackagesResult] =
        service.listVettedPackages(request)

      override def handleResponse(
          response: v1.ListVettedPackagesResult
      ): Either[String, Seq[ListVettedPackagesResult]] =
        response.results
          .traverse(ListVettedPackagesResult.fromProtoV1)
          .leftMap(_.toString)
    }

    final case class ListPartyToParticipant(
        query: BaseQueryX,
        filterParty: String,
        filterParticipant: String,
    ) extends BaseCommand[
          v1.ListPartyToParticipantRequest,
          v1.ListPartyToParticipantResult,
          Seq[ListPartyToParticipantResult],
        ] {

      override def createRequest(): Either[String, v1.ListPartyToParticipantRequest] =
        Right(
          new v1.ListPartyToParticipantRequest(
            baseQuery = Some(query.toProtoV1),
            filterParty = filterParty,
            filterParticipant = filterParticipant,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v1.ListPartyToParticipantRequest,
      ): Future[v1.ListPartyToParticipantResult] =
        service.listPartyToParticipant(request)

      override def handleResponse(
          response: v1.ListPartyToParticipantResult
      ): Either[String, Seq[ListPartyToParticipantResult]] =
        response.results
          .traverse(ListPartyToParticipantResult.fromProtoV1)
          .leftMap(_.toString)
    }

    final case class ListAuthorityOf(
        query: BaseQueryX,
        filterParty: String,
    ) extends BaseCommand[
          v1.ListAuthorityOfRequest,
          v1.ListAuthorityOfResult,
          Seq[ListAuthorityOfResult],
        ] {

      override def createRequest(): Either[String, v1.ListAuthorityOfRequest] =
        Right(
          new v1.ListAuthorityOfRequest(
            baseQuery = Some(query.toProtoV1),
            filterParty = filterParty,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v1.ListAuthorityOfRequest,
      ): Future[v1.ListAuthorityOfResult] =
        service.listAuthorityOf(request)

      override def handleResponse(
          response: v1.ListAuthorityOfResult
      ): Either[String, Seq[ListAuthorityOfResult]] =
        response.results
          .traverse(ListAuthorityOfResult.fromProtoV1)
          .leftMap(_.toString)
    }

    final case class DomainParametersState(
        query: BaseQueryX,
        filterDomain: String,
    ) extends BaseCommand[
          v1.ListDomainParametersStateRequest,
          v1.ListDomainParametersStateResult,
          Seq[ListDomainParametersStateResult],
        ] {

      override def createRequest(): Either[String, v1.ListDomainParametersStateRequest] =
        Right(
          new v1.ListDomainParametersStateRequest(
            baseQuery = Some(query.toProtoV1),
            filterDomain = filterDomain,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v1.ListDomainParametersStateRequest,
      ): Future[v1.ListDomainParametersStateResult] =
        service.listDomainParametersState(request)

      override def handleResponse(
          response: v1.ListDomainParametersStateResult
      ): Either[String, Seq[ListDomainParametersStateResult]] =
        response.results
          .traverse(ListDomainParametersStateResult.fromProtoV1)
          .leftMap(_.toString)
    }

    final case class MediatorDomainState(
        query: BaseQueryX,
        filterDomain: String,
    ) extends BaseCommand[
          v1.ListMediatorDomainStateRequest,
          v1.ListMediatorDomainStateResult,
          Seq[ListMediatorDomainStateResult],
        ] {

      override def createRequest(): Either[String, v1.ListMediatorDomainStateRequest] =
        Right(
          new v1.ListMediatorDomainStateRequest(
            baseQuery = Some(query.toProtoV1),
            filterDomain = filterDomain,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v1.ListMediatorDomainStateRequest,
      ): Future[v1.ListMediatorDomainStateResult] =
        service.listMediatorDomainState(request)

      override def handleResponse(
          response: v1.ListMediatorDomainStateResult
      ): Either[String, Seq[ListMediatorDomainStateResult]] =
        response.results
          .traverse(ListMediatorDomainStateResult.fromProtoV1)
          .leftMap(_.toString)
    }

    final case class SequencerDomainState(
        query: BaseQueryX,
        filterDomain: String,
    ) extends BaseCommand[
          v1.ListSequencerDomainStateRequest,
          v1.ListSequencerDomainStateResult,
          Seq[ListSequencerDomainStateResult],
        ] {

      override def createRequest(): Either[String, v1.ListSequencerDomainStateRequest] =
        Right(
          new v1.ListSequencerDomainStateRequest(
            baseQuery = Some(query.toProtoV1),
            filterDomain = filterDomain,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v1.ListSequencerDomainStateRequest,
      ): Future[v1.ListSequencerDomainStateResult] =
        service.listSequencerDomainState(request)

      override def handleResponse(
          response: v1.ListSequencerDomainStateResult
      ): Either[String, Seq[ListSequencerDomainStateResult]] =
        response.results
          .traverse(ListSequencerDomainStateResult.fromProtoV1)
          .leftMap(_.toString)
    }

    final case class PurgeTopologyTransactionX(
        query: BaseQueryX,
        filterDomain: String,
    ) extends BaseCommand[
          v1.ListPurgeTopologyTransactionXRequest,
          v1.ListPurgeTopologyTransactionXResult,
          Seq[ListPurgeTopologyTransactionXResult],
        ] {

      override def createRequest(): Either[String, v1.ListPurgeTopologyTransactionXRequest] =
        Right(
          new v1.ListPurgeTopologyTransactionXRequest(
            baseQuery = Some(query.toProtoV1),
            filterDomain = filterDomain,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v1.ListPurgeTopologyTransactionXRequest,
      ): Future[v1.ListPurgeTopologyTransactionXResult] =
        service.listPurgeTopologyTransactionX(request)

      override def handleResponse(
          response: v1.ListPurgeTopologyTransactionXResult
      ): Either[String, Seq[ListPurgeTopologyTransactionXResult]] =
        response.results
          .traverse(ListPurgeTopologyTransactionXResult.fromProtoV1)
          .leftMap(_.toString)
    }

    final case class ListStores()
        extends BaseCommand[v1.ListAvailableStoresRequest, v1.ListAvailableStoresResult, Seq[
          String
        ]] {

      override def createRequest(): Either[String, v1.ListAvailableStoresRequest] =
        Right(v1.ListAvailableStoresRequest())

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v1.ListAvailableStoresRequest,
      ): Future[v1.ListAvailableStoresResult] =
        service.listAvailableStores(request)

      override def handleResponse(
          response: v1.ListAvailableStoresResult
      ): Either[String, Seq[String]] =
        Right(response.storeIds)
    }

    final case class ListAll(query: BaseQueryX)
        extends BaseCommand[
          v1.ListAllRequest,
          v1.ListAllResponse,
          GenericStoredTopologyTransactionsX,
        ] {
      override def createRequest(): Either[String, v1.ListAllRequest] =
        Right(new v1.ListAllRequest(Some(query.toProtoV1)))

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v1.ListAllRequest,
      ): Future[v1.ListAllResponse] = service.listAll(request)

      override def handleResponse(
          response: v1.ListAllResponse
      ): Either[String, GenericStoredTopologyTransactionsX] =
        response.result
          .fold[Either[String, GenericStoredTopologyTransactionsX]](
            Right(StoredTopologyTransactionsX.empty)
          ) { collection =>
            StoredTopologyTransactionsX.fromProtoV0(collection).leftMap(_.toString)
          }
    }
  }

  object Write {
    abstract class BaseWriteCommand[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = TopologyManagerWriteServiceXStub

      override def createService(channel: ManagedChannel): TopologyManagerWriteServiceXStub =
        v1.TopologyManagerWriteServiceXGrpc.stub(channel)

      //  command will potentially take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout
    }

    final case class AddTransactions(
        transactions: Seq[GenericSignedTopologyTransactionX],
        store: String,
    ) extends BaseWriteCommand[AddTransactionsRequest, AddTransactionsResponse, Unit] {
      override def createRequest(): Either[String, AddTransactionsRequest] = {
        Right(AddTransactionsRequest(transactions.map(_.toProtoV2), forceChange = false, store))
      }
      override def submitRequest(
          service: TopologyManagerWriteServiceXStub,
          request: AddTransactionsRequest,
      ): Future[AddTransactionsResponse] = service.addTransactions(request)
      override def handleResponse(response: AddTransactionsResponse): Either[String, Unit] =
        Right(())
    }

    final case class SignTransactions(
        transactions: Seq[GenericSignedTopologyTransactionX],
        signedBy: Seq[Fingerprint],
    ) extends BaseWriteCommand[SignTransactionsRequest, SignTransactionsResponse, Seq[
          GenericSignedTopologyTransactionX
        ]] {
      override def createRequest(): Either[String, SignTransactionsRequest] = {
        Right(
          SignTransactionsRequest(transactions.map(_.toProtoV2), signedBy.map(_.toProtoPrimitive))
        )
      }

      override def submitRequest(
          service: TopologyManagerWriteServiceXStub,
          request: SignTransactionsRequest,
      ): Future[SignTransactionsResponse] = service.signTransactions(request)

      override def handleResponse(
          response: SignTransactionsResponse
      ): Either[String, Seq[GenericSignedTopologyTransactionX]] =
        response.transactions
          .traverse(tx =>
            SignedTopologyTransactionX.fromProtoV2(ProtocolVersionValidation.NoValidation, tx)
          )
          .leftMap(_.message)
    }

    final case class Propose[M <: TopologyMappingX: ClassTag](
        mapping: Either[String, M],
        signedBy: Seq[Fingerprint],
        change: TopologyChangeOpX,
        serial: Option[PositiveInt],
        mustFullyAuthorize: Boolean,
        store: String,
    ) extends BaseWriteCommand[
          AuthorizeRequest,
          AuthorizeResponse,
          SignedTopologyTransactionX[TopologyChangeOpX, M],
        ] {

      override def createRequest(): Either[String, AuthorizeRequest] = mapping.map(m =>
        AuthorizeRequest(
          Proposal(
            AuthorizeRequest.Proposal(
              change.toProto,
              serial.map(_.value).getOrElse(0),
              Some(m.toProtoV2),
            )
          ),
          mustFullyAuthorize = mustFullyAuthorize,
          forceChange = false,
          signedBy = signedBy.map(_.toProtoPrimitive),
          store,
        )
      )
      override def submitRequest(
          service: TopologyManagerWriteServiceXStub,
          request: AuthorizeRequest,
      ): Future[AuthorizeResponse] = service.authorize(request)

      override def handleResponse(
          response: AuthorizeResponse
      ): Either[String, SignedTopologyTransactionX[TopologyChangeOpX, M]] = response.transaction
        .toRight("no transaction in response")
        .flatMap(
          SignedTopologyTransactionX
            .fromProtoV2(ProtocolVersionValidation.NoValidation, _)
            .leftMap(_.message)
            .flatMap(tx =>
              tx.selectMapping[M]
                .toRight(
                  s"Expected mapping ${ClassTag[M].getClass.getSimpleName}, but received: ${tx.transaction.mapping.getClass.getSimpleName}"
                )
            )
        )
    }
    object Propose {
      def apply[M <: TopologyMappingX: ClassTag](
          mapping: M,
          signedBy: Seq[Fingerprint],
          store: String,
          serial: Option[PositiveInt] = None,
          change: TopologyChangeOpX = TopologyChangeOpX.Replace,
          mustFullyAuthorize: Boolean = false,
      ): Propose[M] =
        Propose(Right(mapping), signedBy, change, serial, mustFullyAuthorize, store)

    }

    final case class Authorize[M <: TopologyMappingX: ClassTag](
        transactionHash: String,
        mustFullyAuthorize: Boolean,
        signedBy: Seq[Fingerprint],
        store: String,
    ) extends BaseWriteCommand[
          AuthorizeRequest,
          AuthorizeResponse,
          SignedTopologyTransactionX[TopologyChangeOpX, M],
        ] {

      override def createRequest(): Either[String, AuthorizeRequest] = Right(
        AuthorizeRequest(
          TransactionHash(transactionHash),
          mustFullyAuthorize = mustFullyAuthorize,
          forceChange = false,
          signedBy = signedBy.map(_.toProtoPrimitive),
          store = store,
        )
      )

      override def submitRequest(
          service: TopologyManagerWriteServiceXStub,
          request: AuthorizeRequest,
      ): Future[AuthorizeResponse] = service.authorize(request)

      override def handleResponse(
          response: AuthorizeResponse
      ): Either[String, SignedTopologyTransactionX[TopologyChangeOpX, M]] = response.transaction
        .toRight("no transaction in response")
        .flatMap(
          SignedTopologyTransactionX
            .fromProtoV2(ProtocolVersionValidation.NoValidation, _)
            .leftMap(_.message)
            .flatMap(tx =>
              tx.selectMapping[M]
                .toRight(
                  s"Expected mapping ${ClassTag[M].getClass.getSimpleName}, but received: ${tx.transaction.mapping.getClass.getSimpleName}"
                )
            )
        )
    }
  }

  object Init {

    abstract class BaseInitializationService[Req, Resp, Res]
        extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = IdentityInitializationServiceXStub
      override def createService(channel: ManagedChannel): IdentityInitializationServiceXStub =
        v1.IdentityInitializationServiceXGrpc.stub(channel)
    }

    final case class InitId(identifier: String)
        extends BaseInitializationService[v1.InitIdRequest, v1.InitIdResponse, Unit] {

      override def createRequest(): Either[String, v1.InitIdRequest] =
        Right(v1.InitIdRequest(identifier))

      override def submitRequest(
          service: IdentityInitializationServiceXStub,
          request: v1.InitIdRequest,
      ): Future[v1.InitIdResponse] =
        service.initId(request)

      override def handleResponse(response: v1.InitIdResponse): Either[String, Unit] =
        Right(())
    }

    final case class GetId()
        extends BaseInitializationService[Empty, v1.GetIdResponse, UniqueIdentifier] {
      override def createRequest(): Either[String, Empty] =
        Right(Empty())

      override def submitRequest(
          service: IdentityInitializationServiceXStub,
          request: Empty,
      ): Future[v1.GetIdResponse] =
        service.getId(request)

      override def handleResponse(
          response: v1.GetIdResponse
      ): Either[String, UniqueIdentifier] = {
        if (response.uniqueIdentifier.nonEmpty)
          UniqueIdentifier.fromProtoPrimitive_(response.uniqueIdentifier)
        else
          Left(
            s"Node is not initialized and therefore does not have an Id assigned yet."
          )
      }
    }
  }
}
