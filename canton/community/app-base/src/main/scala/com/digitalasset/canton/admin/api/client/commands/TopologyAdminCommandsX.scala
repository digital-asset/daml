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
import com.digitalasset.canton.topology.admin.v30
import com.digitalasset.canton.topology.admin.v30.AuthorizeRequest.Type.{Proposal, TransactionHash}
import com.digitalasset.canton.topology.admin.v30.IdentityInitializationServiceXGrpc.IdentityInitializationServiceXStub
import com.digitalasset.canton.topology.admin.v30.TopologyManagerReadServiceXGrpc.TopologyManagerReadServiceXStub
import com.digitalasset.canton.topology.admin.v30.TopologyManagerWriteServiceXGrpc.TopologyManagerWriteServiceXStub
import com.digitalasset.canton.topology.admin.v30.{
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
        v30.TopologyManagerReadServiceXGrpc.stub(channel)

      //  command will potentially take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ListTrafficControlState(
        query: BaseQueryX,
        filterMember: String,
    ) extends BaseCommand[
          v30.ListTrafficStateRequest,
          v30.ListTrafficStateResult,
          Seq[ListTrafficStateResult],
        ] {

      override def createRequest(): Either[String, v30.ListTrafficStateRequest] =
        Right(
          new ListTrafficStateRequest(
            baseQuery = Some(query.toProtoV1),
            filterMember = filterMember,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v30.ListTrafficStateRequest,
      ): Future[v30.ListTrafficStateResult] =
        service.listTrafficState(request)

      override def handleResponse(
          response: v30.ListTrafficStateResult
      ): Either[String, Seq[ListTrafficStateResult]] =
        response.results
          .traverse(ListTrafficStateResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class ListNamespaceDelegation(
        query: BaseQueryX,
        filterNamespace: String,
        filterTargetKey: Option[Fingerprint],
    ) extends BaseCommand[
          v30.ListNamespaceDelegationRequest,
          v30.ListNamespaceDelegationResult,
          Seq[ListNamespaceDelegationResult],
        ] {

      override def createRequest(): Either[String, v30.ListNamespaceDelegationRequest] =
        Right(
          new v30.ListNamespaceDelegationRequest(
            baseQuery = Some(query.toProtoV1),
            filterNamespace = filterNamespace,
            filterTargetKeyFingerprint = filterTargetKey.map(_.toProtoPrimitive).getOrElse(""),
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v30.ListNamespaceDelegationRequest,
      ): Future[v30.ListNamespaceDelegationResult] =
        service.listNamespaceDelegation(request)

      override def handleResponse(
          response: v30.ListNamespaceDelegationResult
      ): Either[String, Seq[ListNamespaceDelegationResult]] =
        response.results.traverse(ListNamespaceDelegationResult.fromProtoV30).leftMap(_.toString)
    }

    final case class ListDecentralizedNamespaceDefinition(
        query: BaseQueryX,
        filterNamespace: String,
    ) extends BaseCommand[
          v30.ListDecentralizedNamespaceDefinitionRequest,
          v30.ListDecentralizedNamespaceDefinitionResult,
          Seq[ListDecentralizedNamespaceDefinitionResult],
        ] {

      override def createRequest()
          : Either[String, v30.ListDecentralizedNamespaceDefinitionRequest] =
        Right(
          new v30.ListDecentralizedNamespaceDefinitionRequest(
            baseQuery = Some(query.toProtoV1),
            filterNamespace = filterNamespace,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v30.ListDecentralizedNamespaceDefinitionRequest,
      ): Future[v30.ListDecentralizedNamespaceDefinitionResult] =
        service.listDecentralizedNamespaceDefinition(request)

      override def handleResponse(
          response: v30.ListDecentralizedNamespaceDefinitionResult
      ): Either[String, Seq[ListDecentralizedNamespaceDefinitionResult]] =
        response.results
          .traverse(ListDecentralizedNamespaceDefinitionResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class ListIdentifierDelegation(
        query: BaseQueryX,
        filterUid: String,
        filterTargetKey: Option[Fingerprint],
    ) extends BaseCommand[
          v30.ListIdentifierDelegationRequest,
          v30.ListIdentifierDelegationResult,
          Seq[ListIdentifierDelegationResult],
        ] {

      override def createRequest(): Either[String, v30.ListIdentifierDelegationRequest] =
        Right(
          new v30.ListIdentifierDelegationRequest(
            baseQuery = Some(query.toProtoV1),
            filterUid = filterUid,
            filterTargetKeyFingerprint = filterTargetKey.map(_.toProtoPrimitive).getOrElse(""),
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v30.ListIdentifierDelegationRequest,
      ): Future[v30.ListIdentifierDelegationResult] =
        service.listIdentifierDelegation(request)

      override def handleResponse(
          response: v30.ListIdentifierDelegationResult
      ): Either[String, Seq[ListIdentifierDelegationResult]] =
        response.results.traverse(ListIdentifierDelegationResult.fromProtoV30).leftMap(_.toString)
    }

    final case class ListOwnerToKeyMapping(
        query: BaseQueryX,
        filterKeyOwnerType: Option[MemberCode],
        filterKeyOwnerUid: String,
    ) extends BaseCommand[v30.ListOwnerToKeyMappingRequest, v30.ListOwnerToKeyMappingResult, Seq[
          ListOwnerToKeyMappingResult
        ]] {

      override def createRequest(): Either[String, v30.ListOwnerToKeyMappingRequest] =
        Right(
          new v30.ListOwnerToKeyMappingRequest(
            baseQuery = Some(query.toProtoV1),
            filterKeyOwnerType = filterKeyOwnerType.map(_.toProtoPrimitive).getOrElse(""),
            filterKeyOwnerUid = filterKeyOwnerUid,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v30.ListOwnerToKeyMappingRequest,
      ): Future[v30.ListOwnerToKeyMappingResult] =
        service.listOwnerToKeyMapping(request)

      override def handleResponse(
          response: v30.ListOwnerToKeyMappingResult
      ): Either[String, Seq[ListOwnerToKeyMappingResult]] =
        response.results.traverse(ListOwnerToKeyMappingResult.fromProtoV30).leftMap(_.toString)
    }

    final case class ListDomainTrustCertificate(
        query: BaseQueryX,
        filterUid: String,
    ) extends BaseCommand[
          v30.ListDomainTrustCertificateRequest,
          v30.ListDomainTrustCertificateResult,
          Seq[ListDomainTrustCertificateResult],
        ] {

      override def createRequest(): Either[String, v30.ListDomainTrustCertificateRequest] =
        Right(
          new v30.ListDomainTrustCertificateRequest(
            baseQuery = Some(query.toProtoV1),
            filterUid = filterUid,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v30.ListDomainTrustCertificateRequest,
      ): Future[v30.ListDomainTrustCertificateResult] =
        service.listDomainTrustCertificate(request)

      override def handleResponse(
          response: v30.ListDomainTrustCertificateResult
      ): Either[String, Seq[ListDomainTrustCertificateResult]] =
        response.results.traverse(ListDomainTrustCertificateResult.fromProtoV30).leftMap(_.toString)
    }

    final case class ListParticipantDomainPermission(
        query: BaseQueryX,
        filterUid: String,
    ) extends BaseCommand[
          v30.ListParticipantDomainPermissionRequest,
          v30.ListParticipantDomainPermissionResult,
          Seq[ListParticipantDomainPermissionResult],
        ] {

      override def createRequest(): Either[String, v30.ListParticipantDomainPermissionRequest] =
        Right(
          new v30.ListParticipantDomainPermissionRequest(
            baseQuery = Some(query.toProtoV1),
            filterUid = filterUid,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v30.ListParticipantDomainPermissionRequest,
      ): Future[v30.ListParticipantDomainPermissionResult] =
        service.listParticipantDomainPermission(request)

      override def handleResponse(
          response: v30.ListParticipantDomainPermissionResult
      ): Either[String, Seq[ListParticipantDomainPermissionResult]] =
        response.results
          .traverse(ListParticipantDomainPermissionResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class ListPartyHostingLimits(
        query: BaseQueryX,
        filterUid: String,
    ) extends BaseCommand[
          v30.ListPartyHostingLimitsRequest,
          v30.ListPartyHostingLimitsResult,
          Seq[ListPartyHostingLimitsResult],
        ] {

      override def createRequest(): Either[String, v30.ListPartyHostingLimitsRequest] =
        Right(
          new v30.ListPartyHostingLimitsRequest(
            baseQuery = Some(query.toProtoV1),
            filterUid = filterUid,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v30.ListPartyHostingLimitsRequest,
      ): Future[v30.ListPartyHostingLimitsResult] =
        service.listPartyHostingLimits(request)

      override def handleResponse(
          response: v30.ListPartyHostingLimitsResult
      ): Either[String, Seq[ListPartyHostingLimitsResult]] =
        response.results
          .traverse(ListPartyHostingLimitsResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class ListVettedPackages(
        query: BaseQueryX,
        filterParticipant: String,
    ) extends BaseCommand[
          v30.ListVettedPackagesRequest,
          v30.ListVettedPackagesResult,
          Seq[ListVettedPackagesResult],
        ] {

      override def createRequest(): Either[String, v30.ListVettedPackagesRequest] =
        Right(
          new v30.ListVettedPackagesRequest(
            baseQuery = Some(query.toProtoV1),
            filterParticipant = filterParticipant,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v30.ListVettedPackagesRequest,
      ): Future[v30.ListVettedPackagesResult] =
        service.listVettedPackages(request)

      override def handleResponse(
          response: v30.ListVettedPackagesResult
      ): Either[String, Seq[ListVettedPackagesResult]] =
        response.results
          .traverse(ListVettedPackagesResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class ListPartyToParticipant(
        query: BaseQueryX,
        filterParty: String,
        filterParticipant: String,
    ) extends BaseCommand[
          v30.ListPartyToParticipantRequest,
          v30.ListPartyToParticipantResult,
          Seq[ListPartyToParticipantResult],
        ] {

      override def createRequest(): Either[String, v30.ListPartyToParticipantRequest] =
        Right(
          new v30.ListPartyToParticipantRequest(
            baseQuery = Some(query.toProtoV1),
            filterParty = filterParty,
            filterParticipant = filterParticipant,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v30.ListPartyToParticipantRequest,
      ): Future[v30.ListPartyToParticipantResult] =
        service.listPartyToParticipant(request)

      override def handleResponse(
          response: v30.ListPartyToParticipantResult
      ): Either[String, Seq[ListPartyToParticipantResult]] =
        response.results
          .traverse(ListPartyToParticipantResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class ListAuthorityOf(
        query: BaseQueryX,
        filterParty: String,
    ) extends BaseCommand[
          v30.ListAuthorityOfRequest,
          v30.ListAuthorityOfResult,
          Seq[ListAuthorityOfResult],
        ] {

      override def createRequest(): Either[String, v30.ListAuthorityOfRequest] =
        Right(
          new v30.ListAuthorityOfRequest(
            baseQuery = Some(query.toProtoV1),
            filterParty = filterParty,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v30.ListAuthorityOfRequest,
      ): Future[v30.ListAuthorityOfResult] =
        service.listAuthorityOf(request)

      override def handleResponse(
          response: v30.ListAuthorityOfResult
      ): Either[String, Seq[ListAuthorityOfResult]] =
        response.results
          .traverse(ListAuthorityOfResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class DomainParametersState(
        query: BaseQueryX,
        filterDomain: String,
    ) extends BaseCommand[
          v30.ListDomainParametersStateRequest,
          v30.ListDomainParametersStateResult,
          Seq[ListDomainParametersStateResult],
        ] {

      override def createRequest(): Either[String, v30.ListDomainParametersStateRequest] =
        Right(
          new v30.ListDomainParametersStateRequest(
            baseQuery = Some(query.toProtoV1),
            filterDomain = filterDomain,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v30.ListDomainParametersStateRequest,
      ): Future[v30.ListDomainParametersStateResult] =
        service.listDomainParametersState(request)

      override def handleResponse(
          response: v30.ListDomainParametersStateResult
      ): Either[String, Seq[ListDomainParametersStateResult]] =
        response.results
          .traverse(ListDomainParametersStateResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class MediatorDomainState(
        query: BaseQueryX,
        filterDomain: String,
    ) extends BaseCommand[
          v30.ListMediatorDomainStateRequest,
          v30.ListMediatorDomainStateResult,
          Seq[ListMediatorDomainStateResult],
        ] {

      override def createRequest(): Either[String, v30.ListMediatorDomainStateRequest] =
        Right(
          v30.ListMediatorDomainStateRequest(
            baseQuery = Some(query.toProtoV1),
            filterDomain = filterDomain,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v30.ListMediatorDomainStateRequest,
      ): Future[v30.ListMediatorDomainStateResult] =
        service.listMediatorDomainState(request)

      override def handleResponse(
          response: v30.ListMediatorDomainStateResult
      ): Either[String, Seq[ListMediatorDomainStateResult]] =
        response.results
          .traverse(ListMediatorDomainStateResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class SequencerDomainState(
        query: BaseQueryX,
        filterDomain: String,
    ) extends BaseCommand[
          v30.ListSequencerDomainStateRequest,
          v30.ListSequencerDomainStateResult,
          Seq[ListSequencerDomainStateResult],
        ] {

      override def createRequest(): Either[String, v30.ListSequencerDomainStateRequest] =
        Right(
          new v30.ListSequencerDomainStateRequest(
            baseQuery = Some(query.toProtoV1),
            filterDomain = filterDomain,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v30.ListSequencerDomainStateRequest,
      ): Future[v30.ListSequencerDomainStateResult] =
        service.listSequencerDomainState(request)

      override def handleResponse(
          response: v30.ListSequencerDomainStateResult
      ): Either[String, Seq[ListSequencerDomainStateResult]] =
        response.results
          .traverse(ListSequencerDomainStateResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class PurgeTopologyTransactionX(
        query: BaseQueryX,
        filterDomain: String,
    ) extends BaseCommand[
          v30.ListPurgeTopologyTransactionXRequest,
          v30.ListPurgeTopologyTransactionXResult,
          Seq[ListPurgeTopologyTransactionXResult],
        ] {

      override def createRequest(): Either[String, v30.ListPurgeTopologyTransactionXRequest] =
        Right(
          new v30.ListPurgeTopologyTransactionXRequest(
            baseQuery = Some(query.toProtoV1),
            filterDomain = filterDomain,
          )
        )

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v30.ListPurgeTopologyTransactionXRequest,
      ): Future[v30.ListPurgeTopologyTransactionXResult] =
        service.listPurgeTopologyTransactionX(request)

      override def handleResponse(
          response: v30.ListPurgeTopologyTransactionXResult
      ): Either[String, Seq[ListPurgeTopologyTransactionXResult]] =
        response.results
          .traverse(ListPurgeTopologyTransactionXResult.fromProtoV30)
          .leftMap(_.toString)
    }

    final case class ListStores()
        extends BaseCommand[v30.ListAvailableStoresRequest, v30.ListAvailableStoresResult, Seq[
          String
        ]] {

      override def createRequest(): Either[String, v30.ListAvailableStoresRequest] =
        Right(v30.ListAvailableStoresRequest())

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v30.ListAvailableStoresRequest,
      ): Future[v30.ListAvailableStoresResult] =
        service.listAvailableStores(request)

      override def handleResponse(
          response: v30.ListAvailableStoresResult
      ): Either[String, Seq[String]] =
        Right(response.storeIds)
    }

    final case class ListAll(query: BaseQueryX)
        extends BaseCommand[
          v30.ListAllRequest,
          v30.ListAllResponse,
          GenericStoredTopologyTransactionsX,
        ] {
      override def createRequest(): Either[String, v30.ListAllRequest] =
        Right(new v30.ListAllRequest(Some(query.toProtoV1)))

      override def submitRequest(
          service: TopologyManagerReadServiceXStub,
          request: v30.ListAllRequest,
      ): Future[v30.ListAllResponse] = service.listAll(request)

      override def handleResponse(
          response: v30.ListAllResponse
      ): Either[String, GenericStoredTopologyTransactionsX] =
        response.result
          .fold[Either[String, GenericStoredTopologyTransactionsX]](
            Right(StoredTopologyTransactionsX.empty)
          ) { collection =>
            StoredTopologyTransactionsX.fromProtoV30(collection).leftMap(_.toString)
          }
    }
  }

  object Write {
    abstract class BaseWriteCommand[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
      override type Svc = TopologyManagerWriteServiceXStub

      override def createService(channel: ManagedChannel): TopologyManagerWriteServiceXStub =
        v30.TopologyManagerWriteServiceXGrpc.stub(channel)

      //  command will potentially take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout
    }

    final case class AddTransactions(
        transactions: Seq[GenericSignedTopologyTransactionX],
        store: String,
    ) extends BaseWriteCommand[AddTransactionsRequest, AddTransactionsResponse, Unit] {
      override def createRequest(): Either[String, AddTransactionsRequest] = {
        Right(AddTransactionsRequest(transactions.map(_.toProtoV30), forceChange = false, store))
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
          SignTransactionsRequest(transactions.map(_.toProtoV30), signedBy.map(_.toProtoPrimitive))
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
            SignedTopologyTransactionX.fromProtoV30(ProtocolVersionValidation.NoValidation, tx)
          )
          .leftMap(_.message)
    }

    final case class Propose[M <: TopologyMappingX: ClassTag](
        mapping: Either[String, M],
        signedBy: Seq[Fingerprint],
        change: TopologyChangeOpX,
        serial: Option[PositiveInt],
        mustFullyAuthorize: Boolean,
        forceChange: Boolean,
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
              Some(m.toProtoV30),
            )
          ),
          mustFullyAuthorize = mustFullyAuthorize,
          forceChange = forceChange,
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
            .fromProtoV30(ProtocolVersionValidation.NoValidation, _)
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
          forceChange: Boolean = false,
      ): Propose[M] =
        Propose(Right(mapping), signedBy, change, serial, mustFullyAuthorize, forceChange, store)

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
            .fromProtoV30(ProtocolVersionValidation.NoValidation, _)
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
        v30.IdentityInitializationServiceXGrpc.stub(channel)
    }

    final case class InitId(identifier: String)
        extends BaseInitializationService[v30.InitIdRequest, v30.InitIdResponse, Unit] {

      override def createRequest(): Either[String, v30.InitIdRequest] =
        Right(v30.InitIdRequest(identifier))

      override def submitRequest(
          service: IdentityInitializationServiceXStub,
          request: v30.InitIdRequest,
      ): Future[v30.InitIdResponse] =
        service.initId(request)

      override def handleResponse(response: v30.InitIdResponse): Either[String, Unit] =
        Right(())
    }

    final case class GetId()
        extends BaseInitializationService[Empty, v30.GetIdResponse, UniqueIdentifier] {
      override def createRequest(): Either[String, Empty] =
        Right(Empty())

      override def submitRequest(
          service: IdentityInitializationServiceXStub,
          request: Empty,
      ): Future[v30.GetIdResponse] =
        service.getId(request)

      override def handleResponse(
          response: v30.GetIdResponse
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
