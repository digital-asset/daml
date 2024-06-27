// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.ledger.api.v2.admin.command_inspection_service.CommandInspectionServiceGrpc.CommandInspectionServiceStub
import com.daml.ledger.api.v2.admin.command_inspection_service.{
  CommandInspectionServiceGrpc,
  CommandState,
  GetCommandStatusRequest,
  GetCommandStatusResponse,
}
import com.daml.ledger.api.v2.admin.identity_provider_config_service.IdentityProviderConfigServiceGrpc.IdentityProviderConfigServiceStub
import com.daml.ledger.api.v2.admin.identity_provider_config_service.*
import com.daml.ledger.api.v2.admin.metering_report_service.MeteringReportServiceGrpc.MeteringReportServiceStub
import com.daml.ledger.api.v2.admin.metering_report_service.{
  GetMeteringReportRequest,
  GetMeteringReportResponse,
  MeteringReportServiceGrpc,
}
import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementServiceStub
import com.daml.ledger.api.v2.admin.package_management_service.*
import com.daml.ledger.api.v2.admin.participant_pruning_service.ParticipantPruningServiceGrpc.ParticipantPruningServiceStub
import com.daml.ledger.api.v2.admin.participant_pruning_service.*
import com.daml.ledger.api.v2.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementServiceStub
import com.daml.ledger.api.v2.admin.party_management_service.*
import com.daml.ledger.api.v2.admin.user_management_service.UserManagementServiceGrpc.UserManagementServiceStub
import com.daml.ledger.api.v2.admin.user_management_service.{
  CreateUserRequest,
  CreateUserResponse,
  DeleteUserRequest,
  DeleteUserResponse,
  GetUserRequest,
  GetUserResponse,
  GrantUserRightsRequest,
  GrantUserRightsResponse,
  ListUserRightsRequest,
  ListUserRightsResponse,
  ListUsersRequest,
  ListUsersResponse,
  RevokeUserRightsRequest,
  RevokeUserRightsResponse,
  Right as UserRight,
  UpdateUserIdentityProviderIdRequest,
  UpdateUserIdentityProviderIdResponse,
  UpdateUserRequest,
  UpdateUserResponse,
  User,
  UserManagementServiceGrpc,
}
import com.daml.ledger.api.v2.checkpoint.Checkpoint
import com.daml.ledger.api.v2.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionServiceStub
import com.daml.ledger.api.v2.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionStreamRequest,
  CompletionStreamResponse,
}
import com.daml.ledger.api.v2.command_service.CommandServiceGrpc.CommandServiceStub
import com.daml.ledger.api.v2.command_service.{
  CommandServiceGrpc,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
  SubmitAndWaitRequest,
}
import com.daml.ledger.api.v2.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionServiceStub
import com.daml.ledger.api.v2.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitReassignmentRequest,
  SubmitReassignmentResponse,
  SubmitRequest,
  SubmitResponse,
}
import com.daml.ledger.api.v2.commands.{Command, Commands, DisclosedContract}
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.event_query_service.EventQueryServiceGrpc.EventQueryServiceStub
import com.daml.ledger.api.v2.event_query_service.{
  EventQueryServiceGrpc,
  GetEventsByContractIdRequest,
  GetEventsByContractIdResponse,
}
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.api.v2.reassignment.{AssignedEvent, Reassignment, UnassignedEvent}
import com.daml.ledger.api.v2.reassignment_command.{
  AssignCommand,
  ReassignmentCommand,
  UnassignCommand,
}
import com.daml.ledger.api.v2.state_service.StateServiceGrpc.StateServiceStub
import com.daml.ledger.api.v2.state_service.{
  GetActiveContractsRequest,
  GetActiveContractsResponse,
  GetConnectedDomainsRequest,
  GetConnectedDomainsResponse,
  GetLedgerEndRequest,
  GetLedgerEndResponse,
  StateServiceGrpc,
}
import com.daml.ledger.api.v2.testing.time_service.TimeServiceGrpc.TimeServiceStub
import com.daml.ledger.api.v2.testing.time_service.{
  GetTimeRequest,
  GetTimeResponse,
  SetTimeRequest,
  TimeServiceGrpc,
}
import com.daml.ledger.api.v2.transaction.{Transaction, TransactionTree}
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  Filters,
  TemplateFilter,
  TransactionFilter,
}
import com.daml.ledger.api.v2.update_service.UpdateServiceGrpc.UpdateServiceStub
import com.daml.ledger.api.v2.update_service.{
  GetTransactionByIdRequest,
  GetTransactionTreeResponse,
  GetUpdateTreesResponse,
  GetUpdatesRequest,
  GetUpdatesResponse,
  UpdateServiceGrpc,
}
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  ServerEnforcedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.admin.api.client.data.{
  LedgerApiUser,
  LedgerMeteringReport,
  ListLedgerApiUsersResult,
  TemplateId,
  UserRights,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.{CantonTimestamp, DeduplicationPeriod}
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.{IdentityProviderId, JwksUrl}
import com.digitalasset.canton.ledger.client.services.admin.IdentityProviderConfigClient
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.networking.grpc.ForwardingStreamObserver
import com.digitalasset.canton.platform.apiserver.execution.CommandStatus
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.{DomainId, PartyId}
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.{LfPackageId, LfPartyId, config}
import com.google.protobuf.empty.Empty
import com.google.protobuf.field_mask.FieldMask
import io.grpc.*
import io.grpc.stub.StreamObserver

import java.time.Instant
import java.util.UUID
import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

object LedgerApiCommands {

  final val defaultApplicationId = "CantonConsole"

  object PartyManagementService {
    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = PartyManagementServiceStub

      override def createService(channel: ManagedChannel): PartyManagementServiceStub =
        PartyManagementServiceGrpc.stub(channel)
    }

    final case class AllocateParty(
        partyIdHint: String,
        displayName: String,
        annotations: Map[String, String],
        identityProviderId: String,
    ) extends BaseCommand[AllocatePartyRequest, AllocatePartyResponse, PartyDetails] {
      override def createRequest(): Either[String, AllocatePartyRequest] =
        Right(
          AllocatePartyRequest(
            partyIdHint = partyIdHint,
            displayName = displayName,
            localMetadata = Some(ObjectMeta(annotations = annotations)),
            identityProviderId = identityProviderId,
          )
        )
      override def submitRequest(
          service: PartyManagementServiceStub,
          request: AllocatePartyRequest,
      ): Future[AllocatePartyResponse] =
        service.allocateParty(request)
      override def handleResponse(response: AllocatePartyResponse): Either[String, PartyDetails] =
        response.partyDetails.toRight("Party could not be created")
    }

    final case class Update(
        party: PartyId,
        annotationsUpdate: Option[Map[String, String]],
        resourceVersionO: Option[String],
        identityProviderId: String,
    ) extends BaseCommand[UpdatePartyDetailsRequest, UpdatePartyDetailsResponse, PartyDetails] {

      override def submitRequest(
          service: PartyManagementServiceStub,
          request: UpdatePartyDetailsRequest,
      ): Future[UpdatePartyDetailsResponse] =
        service.updatePartyDetails(request)

      override def createRequest(): Either[String, UpdatePartyDetailsRequest] = {
        val metadata = ObjectMeta(
          annotations = annotationsUpdate.getOrElse(Map.empty),
          resourceVersion = resourceVersionO.getOrElse(""),
        )
        val partyDetails =
          PartyDetails(
            party = party.toProtoPrimitive,
            localMetadata = Some(metadata),
            identityProviderId = identityProviderId,
          )
        val updatePaths =
          annotationsUpdate.fold(Seq.empty[String])(_ => Seq("local_metadata.annotations"))
        val req = UpdatePartyDetailsRequest(
          partyDetails = Some(partyDetails),
          updateMask = Some(FieldMask(paths = updatePaths)),
        )
        Right(req)
      }

      override def handleResponse(
          response: UpdatePartyDetailsResponse
      ): Either[String, PartyDetails] =
        response.partyDetails.toRight("Failed to update the party details")

    }

    final case class ListKnownParties(identityProviderId: String)
        extends BaseCommand[ListKnownPartiesRequest, ListKnownPartiesResponse, Seq[
          PartyDetails
        ]] {
      override def createRequest(): Either[String, ListKnownPartiesRequest] =
        Right(
          ListKnownPartiesRequest(
            identityProviderId = identityProviderId
          )
        )
      override def submitRequest(
          service: PartyManagementServiceStub,
          request: ListKnownPartiesRequest,
      ): Future[ListKnownPartiesResponse] =
        service.listKnownParties(request)
      override def handleResponse(
          response: ListKnownPartiesResponse
      ): Either[String, Seq[PartyDetails]] =
        Right(response.partyDetails)
    }

    final case class GetParty(party: PartyId, identityProviderId: String)
        extends BaseCommand[GetPartiesRequest, GetPartiesResponse, PartyDetails] {

      override def createRequest(): Either[String, GetPartiesRequest] =
        Right(
          GetPartiesRequest(
            parties = Seq(party.toProtoPrimitive),
            identityProviderId = identityProviderId,
          )
        )

      override def submitRequest(
          service: PartyManagementServiceStub,
          request: GetPartiesRequest,
      ): Future[GetPartiesResponse] = service.getParties(request)

      override def handleResponse(
          response: GetPartiesResponse
      ): Either[String, PartyDetails] = {
        response.partyDetails.headOption.toRight("PARTY_NOT_FOUND")
      }
    }

    final case class UpdateIdp(
        party: PartyId,
        sourceIdentityProviderId: String,
        targetIdentityProviderId: String,
    ) extends BaseCommand[
          UpdatePartyIdentityProviderIdRequest,
          UpdatePartyIdentityProviderIdResponse,
          Unit,
        ] {

      override def submitRequest(
          service: PartyManagementServiceStub,
          request: UpdatePartyIdentityProviderIdRequest,
      ): Future[UpdatePartyIdentityProviderIdResponse] =
        service.updatePartyIdentityProviderId(request)

      override def createRequest(): Either[String, UpdatePartyIdentityProviderIdRequest] = Right(
        UpdatePartyIdentityProviderIdRequest(
          party = party.toProtoPrimitive,
          sourceIdentityProviderId = sourceIdentityProviderId,
          targetIdentityProviderId = targetIdentityProviderId,
        )
      )

      override def handleResponse(
          response: UpdatePartyIdentityProviderIdResponse
      ): Either[String, Unit] = Right(())

    }
  }

  object PackageService {

    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = PackageManagementServiceStub
      override def createService(channel: ManagedChannel): PackageManagementServiceStub =
        PackageManagementServiceGrpc.stub(channel)
    }

    final case class UploadDarFile(darPath: String)
        extends BaseCommand[UploadDarFileRequest, UploadDarFileResponse, Unit] {

      override def createRequest(): Either[String, UploadDarFileRequest] =
        for {
          bytes <- BinaryFileUtil.readByteStringFromFile(darPath)
        } yield UploadDarFileRequest(bytes)
      override def submitRequest(
          service: PackageManagementServiceStub,
          request: UploadDarFileRequest,
      ): Future[UploadDarFileResponse] =
        service.uploadDarFile(request)
      override def handleResponse(response: UploadDarFileResponse): Either[String, Unit] =
        Right(())

      // package upload time might take long if it is a big package
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ListKnownPackages(limit: PositiveInt)
        extends BaseCommand[ListKnownPackagesRequest, ListKnownPackagesResponse, Seq[
          PackageDetails
        ]] {

      override def createRequest(): Either[String, ListKnownPackagesRequest] = Right(
        ListKnownPackagesRequest()
      )

      override def submitRequest(
          service: PackageManagementServiceStub,
          request: ListKnownPackagesRequest,
      ): Future[ListKnownPackagesResponse] =
        service.listKnownPackages(request)

      override def handleResponse(
          response: ListKnownPackagesResponse
      ): Either[String, Seq[PackageDetails]] =
        Right(response.packageDetails.take(limit.value))
    }

  }

  object CommandInspectionService {
    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = CommandInspectionServiceStub

      override def createService(channel: ManagedChannel): CommandInspectionServiceStub =
        CommandInspectionServiceGrpc.stub(channel)
    }

    final case class GetCommandStatus(commandIdPrefix: String, state: CommandState, limit: Int)
        extends BaseCommand[GetCommandStatusRequest, GetCommandStatusResponse, Seq[CommandStatus]] {
      override def createRequest(): Either[String, GetCommandStatusRequest] = Right(
        GetCommandStatusRequest(commandIdPrefix = commandIdPrefix, state = state, limit = limit)
      )

      override def submitRequest(
          service: CommandInspectionServiceStub,
          request: GetCommandStatusRequest,
      ): Future[GetCommandStatusResponse] = service.getCommandStatus(request)

      override def handleResponse(
          response: GetCommandStatusResponse
      ): Either[String, Seq[CommandStatus]] = {
        response.commandStatus.traverse(CommandStatus.fromProto).leftMap(_.message)
      }
    }
  }

  object ParticipantPruningService {
    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = ParticipantPruningServiceStub
      override def createService(channel: ManagedChannel): ParticipantPruningServiceStub =
        ParticipantPruningServiceGrpc.stub(channel)

      // all pruning commands will take a long time
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class Prune(pruneUpTo: ParticipantOffset)
        extends BaseCommand[PruneRequest, PruneResponse, Unit] {

      override def timeoutType: TimeoutType =
        DefaultUnboundedTimeout // pruning can take a very long time

      override def createRequest(): Either[String, PruneRequest] =
        pruneUpTo.value.absolute
          .toRight("The pruneUpTo ledger offset needs to be absolute")
          .map(
            PruneRequest(
              _,
              // canton always prunes divulged contracts both in the ledger api index-db and in canton stores
              pruneAllDivulgedContracts = true,
            )
          )

      override def submitRequest(
          service: ParticipantPruningServiceStub,
          request: PruneRequest,
      ): Future[PruneResponse] =
        service.prune(request)

      override def handleResponse(response: PruneResponse): Either[String, Unit] = Right(())
    }
  }

  object Users {
    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = UserManagementServiceStub

      override def createService(channel: ManagedChannel): UserManagementServiceStub =
        UserManagementServiceGrpc.stub(channel)
    }

    trait HasRights {
      def actAs: Set[LfPartyId]
      def readAs: Set[LfPartyId]
      def participantAdmin: Boolean
      def readAsAnyParty: Boolean

      protected def getRights: Seq[UserRight] = {
        actAs.toSeq.map(x => UserRight().withCanActAs(UserRight.CanActAs(x))) ++
          readAs.toSeq.map(x => UserRight().withCanReadAs(UserRight.CanReadAs(x))) ++
          (if (participantAdmin) Seq(UserRight().withParticipantAdmin(UserRight.ParticipantAdmin()))
           else Seq()) ++
          (if (readAsAnyParty) Seq(UserRight().withCanReadAsAnyParty(UserRight.CanReadAsAnyParty()))
           else Seq())
      }
    }

    final case class Create(
        id: String,
        actAs: Set[LfPartyId],
        primaryParty: Option[LfPartyId],
        readAs: Set[LfPartyId],
        participantAdmin: Boolean,
        isDeactivated: Boolean,
        annotations: Map[String, String],
        identityProviderId: String,
        readAsAnyParty: Boolean,
    ) extends BaseCommand[CreateUserRequest, CreateUserResponse, LedgerApiUser]
        with HasRights {

      override def submitRequest(
          service: UserManagementServiceStub,
          request: CreateUserRequest,
      ): Future[CreateUserResponse] =
        service.createUser(request)

      override def createRequest(): Either[String, CreateUserRequest] = Right(
        CreateUserRequest(
          user = Some(
            User(
              id = id,
              primaryParty = primaryParty.getOrElse(""),
              isDeactivated = isDeactivated,
              metadata = Some(ObjectMeta(annotations = annotations)),
              identityProviderId = identityProviderId,
            )
          ),
          rights = getRights,
        )
      )

      override def handleResponse(response: CreateUserResponse): Either[String, LedgerApiUser] =
        ProtoConverter
          .parseRequired(LedgerApiUser.fromProtoV0, "user", response.user)
          .leftMap(_.toString)

    }

    final case class Update(
        id: String,
        primaryPartyUpdate: Option[Option[PartyId]],
        isDeactivatedUpdate: Option[Boolean],
        annotationsUpdate: Option[Map[String, String]],
        resourceVersionO: Option[String],
        identityProviderId: String,
    ) extends BaseCommand[UpdateUserRequest, UpdateUserResponse, LedgerApiUser] {

      override def submitRequest(
          service: UserManagementServiceStub,
          request: UpdateUserRequest,
      ): Future[UpdateUserResponse] =
        service.updateUser(request)

      override def createRequest(): Either[String, UpdateUserRequest] = {
        val user = User(
          id = id,
          primaryParty = primaryPartyUpdate.fold("")(_.fold("")(_.toProtoPrimitive)),
          isDeactivated = isDeactivatedUpdate.getOrElse(false),
          metadata = Some(
            ObjectMeta(
              annotations = annotationsUpdate.getOrElse(Map.empty),
              resourceVersion = resourceVersionO.getOrElse(""),
            )
          ),
          identityProviderId = identityProviderId,
        )
        val updatePaths: Seq[String] = Seq(
          primaryPartyUpdate.map(_ => "primary_party"),
          isDeactivatedUpdate.map(_ => "is_deactivated"),
          annotationsUpdate.map(_ => "metadata.annotations"),
        ).flatten
        Right(
          UpdateUserRequest(
            user = Some(user),
            updateMask = Some(FieldMask(paths = updatePaths)),
          )
        )
      }

      override def handleResponse(response: UpdateUserResponse): Either[String, LedgerApiUser] =
        ProtoConverter
          .parseRequired(LedgerApiUser.fromProtoV0, "user", response.user)
          .leftMap(_.toString)

    }

    final case class Get(
        id: String,
        identityProviderId: String,
    ) extends BaseCommand[GetUserRequest, GetUserResponse, LedgerApiUser] {

      override def submitRequest(
          service: UserManagementServiceStub,
          request: GetUserRequest,
      ): Future[GetUserResponse] =
        service.getUser(request)

      override def createRequest(): Either[String, GetUserRequest] = Right(
        GetUserRequest(
          userId = id,
          identityProviderId = identityProviderId,
        )
      )

      override def handleResponse(response: GetUserResponse): Either[String, LedgerApiUser] =
        ProtoConverter
          .parseRequired(LedgerApiUser.fromProtoV0, "user", response.user)
          .leftMap(_.toString)

    }

    final case class Delete(
        id: String,
        identityProviderId: String,
    ) extends BaseCommand[DeleteUserRequest, DeleteUserResponse, Unit] {

      override def submitRequest(
          service: UserManagementServiceStub,
          request: DeleteUserRequest,
      ): Future[DeleteUserResponse] =
        service.deleteUser(request)

      override def createRequest(): Either[String, DeleteUserRequest] = Right(
        DeleteUserRequest(
          userId = id,
          identityProviderId = identityProviderId,
        )
      )

      override def handleResponse(response: DeleteUserResponse): Either[String, Unit] = Right(())

    }

    final case class UpdateIdp(
        id: String,
        sourceIdentityProviderId: String,
        targetIdentityProviderId: String,
    ) extends BaseCommand[
          UpdateUserIdentityProviderIdRequest,
          UpdateUserIdentityProviderIdResponse,
          Unit,
        ] {

      override def submitRequest(
          service: UserManagementServiceStub,
          request: UpdateUserIdentityProviderIdRequest,
      ): Future[UpdateUserIdentityProviderIdResponse] =
        service.updateUserIdentityProviderId(request)

      override def createRequest(): Either[String, UpdateUserIdentityProviderIdRequest] = Right(
        UpdateUserIdentityProviderIdRequest(
          userId = id,
          sourceIdentityProviderId = sourceIdentityProviderId,
          targetIdentityProviderId = targetIdentityProviderId,
        )
      )

      override def handleResponse(
          response: UpdateUserIdentityProviderIdResponse
      ): Either[String, Unit] = Right(())

    }

    final case class List(
        filterUser: String,
        pageToken: String,
        pageSize: Int,
        identityProviderId: String,
    ) extends BaseCommand[ListUsersRequest, ListUsersResponse, ListLedgerApiUsersResult] {

      override def submitRequest(
          service: UserManagementServiceStub,
          request: ListUsersRequest,
      ): Future[ListUsersResponse] =
        service.listUsers(request)

      override def createRequest(): Either[String, ListUsersRequest] = Right(
        ListUsersRequest(
          pageToken = pageToken,
          pageSize = pageSize,
          identityProviderId = identityProviderId,
        )
      )

      override def handleResponse(
          response: ListUsersResponse
      ): Either[String, ListLedgerApiUsersResult] =
        ListLedgerApiUsersResult.fromProtoV0(response, filterUser).leftMap(_.toString)

    }

    object Rights {
      final case class Grant(
          id: String,
          actAs: Set[LfPartyId],
          readAs: Set[LfPartyId],
          participantAdmin: Boolean,
          identityProviderId: String,
          readAsAnyParty: Boolean,
      ) extends BaseCommand[GrantUserRightsRequest, GrantUserRightsResponse, UserRights]
          with HasRights {

        override def submitRequest(
            service: UserManagementServiceStub,
            request: GrantUserRightsRequest,
        ): Future[GrantUserRightsResponse] =
          service.grantUserRights(request)

        override def createRequest(): Either[String, GrantUserRightsRequest] = Right(
          GrantUserRightsRequest(
            userId = id,
            rights = getRights,
            identityProviderId = identityProviderId,
          )
        )

        override def handleResponse(response: GrantUserRightsResponse): Either[String, UserRights] =
          UserRights.fromProtoV0(response.newlyGrantedRights).leftMap(_.toString)

      }

      final case class Revoke(
          id: String,
          actAs: Set[LfPartyId],
          readAs: Set[LfPartyId],
          participantAdmin: Boolean,
          identityProviderId: String,
          readAsAnyParty: Boolean,
      ) extends BaseCommand[RevokeUserRightsRequest, RevokeUserRightsResponse, UserRights]
          with HasRights {

        override def submitRequest(
            service: UserManagementServiceStub,
            request: RevokeUserRightsRequest,
        ): Future[RevokeUserRightsResponse] =
          service.revokeUserRights(request)

        override def createRequest(): Either[String, RevokeUserRightsRequest] = Right(
          RevokeUserRightsRequest(
            userId = id,
            rights = getRights,
            identityProviderId = identityProviderId,
          )
        )

        override def handleResponse(
            response: RevokeUserRightsResponse
        ): Either[String, UserRights] =
          UserRights.fromProtoV0(response.newlyRevokedRights).leftMap(_.toString)

      }

      final case class List(id: String, identityProviderId: String)
          extends BaseCommand[ListUserRightsRequest, ListUserRightsResponse, UserRights] {

        override def submitRequest(
            service: UserManagementServiceStub,
            request: ListUserRightsRequest,
        ): Future[ListUserRightsResponse] =
          service.listUserRights(request)

        override def createRequest(): Either[String, ListUserRightsRequest] = Right(
          ListUserRightsRequest(userId = id, identityProviderId = identityProviderId)
        )

        override def handleResponse(response: ListUserRightsResponse): Either[String, UserRights] =
          UserRights.fromProtoV0(response.rights).leftMap(_.toString)

      }

    }

  }

  object IdentityProviderConfigs {
    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = IdentityProviderConfigServiceStub

      override def createService(channel: ManagedChannel): IdentityProviderConfigServiceStub =
        IdentityProviderConfigServiceGrpc.stub(channel)
    }

    final case class Create(
        identityProviderId: IdentityProviderId.Id,
        isDeactivated: Boolean = false,
        jwksUrl: JwksUrl,
        issuer: String,
        audience: Option[String],
    ) extends BaseCommand[
          CreateIdentityProviderConfigRequest,
          CreateIdentityProviderConfigResponse,
          IdentityProviderConfig,
        ] {

      override def submitRequest(
          service: IdentityProviderConfigServiceStub,
          request: CreateIdentityProviderConfigRequest,
      ): Future[CreateIdentityProviderConfigResponse] =
        service.createIdentityProviderConfig(request)

      override def createRequest(): Either[String, CreateIdentityProviderConfigRequest] =
        Right(
          CreateIdentityProviderConfigRequest(
            Some(
              IdentityProviderConfig(
                identityProviderId = identityProviderId.value,
                isDeactivated = isDeactivated,
                issuer = issuer,
                jwksUrl = jwksUrl.value,
                audience = audience.getOrElse(""),
              )
            )
          )
        )

      override def handleResponse(
          response: CreateIdentityProviderConfigResponse
      ): Either[String, IdentityProviderConfig] =
        response.identityProviderConfig.toRight("config could not be created")
    }

    final case class Update(
        identityProviderConfig: domain.IdentityProviderConfig,
        updateMask: FieldMask,
    ) extends BaseCommand[
          UpdateIdentityProviderConfigRequest,
          UpdateIdentityProviderConfigResponse,
          IdentityProviderConfig,
        ] {

      override def submitRequest(
          service: IdentityProviderConfigServiceStub,
          request: UpdateIdentityProviderConfigRequest,
      ): Future[UpdateIdentityProviderConfigResponse] =
        service.updateIdentityProviderConfig(request)

      override def createRequest(): Either[String, UpdateIdentityProviderConfigRequest] =
        Right(
          UpdateIdentityProviderConfigRequest(
            identityProviderConfig =
              Some(IdentityProviderConfigClient.toProtoConfig(identityProviderConfig)),
            Some(updateMask),
          )
        )

      override def handleResponse(
          response: UpdateIdentityProviderConfigResponse
      ): Either[String, IdentityProviderConfig] =
        response.identityProviderConfig.toRight("config could not be updated")
    }

    final case class Delete(identityProviderId: IdentityProviderId)
        extends BaseCommand[
          DeleteIdentityProviderConfigRequest,
          DeleteIdentityProviderConfigResponse,
          Unit,
        ] {

      override def submitRequest(
          service: IdentityProviderConfigServiceStub,
          request: DeleteIdentityProviderConfigRequest,
      ): Future[DeleteIdentityProviderConfigResponse] =
        service.deleteIdentityProviderConfig(request)

      override def createRequest(): Either[String, DeleteIdentityProviderConfigRequest] =
        Right(
          DeleteIdentityProviderConfigRequest(identityProviderId =
            identityProviderId.toRequestString
          )
        )

      override def handleResponse(
          response: DeleteIdentityProviderConfigResponse
      ): Either[String, Unit] =
        Right(())
    }

    final case class Get(identityProviderId: IdentityProviderId)
        extends BaseCommand[
          GetIdentityProviderConfigRequest,
          GetIdentityProviderConfigResponse,
          IdentityProviderConfig,
        ] {

      override def submitRequest(
          service: IdentityProviderConfigServiceStub,
          request: GetIdentityProviderConfigRequest,
      ): Future[GetIdentityProviderConfigResponse] =
        service.getIdentityProviderConfig(request)

      override def createRequest(): Either[String, GetIdentityProviderConfigRequest] =
        Right(
          GetIdentityProviderConfigRequest(identityProviderId.toRequestString)
        )

      override def handleResponse(
          response: GetIdentityProviderConfigResponse
      ): Either[String, IdentityProviderConfig] =
        Right(response.getIdentityProviderConfig)
    }

    final case class List()
        extends BaseCommand[
          ListIdentityProviderConfigsRequest,
          ListIdentityProviderConfigsResponse,
          Seq[IdentityProviderConfig],
        ] {

      override def submitRequest(
          service: IdentityProviderConfigServiceStub,
          request: ListIdentityProviderConfigsRequest,
      ): Future[ListIdentityProviderConfigsResponse] =
        service.listIdentityProviderConfigs(request)

      override def createRequest(): Either[String, ListIdentityProviderConfigsRequest] =
        Right(
          ListIdentityProviderConfigsRequest()
        )

      override def handleResponse(
          response: ListIdentityProviderConfigsResponse
      ): Either[String, Seq[IdentityProviderConfig]] =
        Right(response.identityProviderConfigs)
    }

  }

  object Metering {
    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = MeteringReportServiceStub

      override def createService(channel: ManagedChannel): MeteringReportServiceStub =
        MeteringReportServiceGrpc.stub(channel)
    }

    final case class GetReport(
        from: CantonTimestamp,
        to: Option[CantonTimestamp],
        applicationId: Option[String],
    ) extends BaseCommand[
          GetMeteringReportRequest,
          GetMeteringReportResponse,
          String,
        ] {

      override def submitRequest(
          service: MeteringReportServiceStub,
          request: GetMeteringReportRequest,
      ): Future[GetMeteringReportResponse] =
        service.getMeteringReport(request)

      override def createRequest(): Either[String, GetMeteringReportRequest] =
        Right(
          GetMeteringReportRequest(
            from = Some(from.toProtoTimestamp),
            to = to.map(_.toProtoTimestamp),
            applicationId = applicationId.getOrElse(""),
          )
        )

      override def handleResponse(
          response: GetMeteringReportResponse
      ): Either[String, String] =
        LedgerMeteringReport.fromProtoV0(response).leftMap(_.toString)
    }
  }

  trait SubscribeBase[Req, Resp, Res] extends GrpcAdminCommand[Req, AutoCloseable, AutoCloseable] {
    // The subscription should never be cut short because of a gRPC timeout
    override def timeoutType: TimeoutType = ServerEnforcedTimeout

    def observer: StreamObserver[Res]

    def doRequest(
        service: this.Svc,
        request: Req,
        rawObserver: StreamObserver[Resp],
    ): Unit

    def extractResults(response: Resp): IterableOnce[Res]

    implicit def loggingContext: ErrorLoggingContext

    override def submitRequest(
        service: this.Svc,
        request: Req,
    ): Future[AutoCloseable] = {
      val rawObserver = new ForwardingStreamObserver[Resp, Res](observer, extractResults)
      val context = Context.current().withCancellation()
      context.run(() => doRequest(service, request, rawObserver))
      Future.successful(context)
    }

    override def handleResponse(response: AutoCloseable): Either[String, AutoCloseable] = Right(
      response
    )
  }

  object UpdateService {

    sealed trait UpdateTreeWrapper {
      def updateId: String
    }
    sealed trait UpdateWrapper {
      def updateId: String
      def createEvent: Option[CreatedEvent] =
        this match {
          case UpdateService.TransactionWrapper(t) => t.events.headOption.map(_.getCreated)
          case u: UpdateService.AssignedWrapper => u.assignedEvent.createdEvent
          case _: UpdateService.UnassignedWrapper => None
        }
    }
    object UpdateWrapper {
      def isUnassigedWrapper(wrapper: UpdateWrapper): Boolean = wrapper match {
        case UnassignedWrapper(_, _) => true
        case _ => false
      }
    }
    final case class TransactionTreeWrapper(transactionTree: TransactionTree)
        extends UpdateTreeWrapper {
      override def updateId: String = transactionTree.updateId
    }
    final case class TransactionWrapper(transaction: Transaction) extends UpdateWrapper {
      override def updateId: String = transaction.updateId
    }
    sealed trait ReassignmentWrapper extends UpdateTreeWrapper with UpdateWrapper {
      def reassignment: Reassignment
      def unassignId: String = reassignment.getUnassignedEvent.unassignId
      def offset: ParticipantOffset = ParticipantOffset(
        ParticipantOffset.Value.Absolute(reassignment.offset)
      )
    }
    object ReassignmentWrapper {
      def apply(reassignment: Reassignment): ReassignmentWrapper = {
        val event = reassignment.event
        event.assignedEvent
          .map[ReassignmentWrapper](AssignedWrapper(reassignment, _))
          .orElse(
            event.unassignedEvent.map[ReassignmentWrapper](UnassignedWrapper(reassignment, _))
          )
          .getOrElse(
            throw new IllegalStateException(
              s"Invalid reassignment event (only supported UnassignedEvent and AssignedEvent): ${reassignment.event}"
            )
          )
      }
    }
    final case class AssignedWrapper(reassignment: Reassignment, assignedEvent: AssignedEvent)
        extends ReassignmentWrapper {
      override def updateId: String = reassignment.updateId
    }
    final case class UnassignedWrapper(reassignment: Reassignment, unassignedEvent: UnassignedEvent)
        extends ReassignmentWrapper {
      override def updateId: String = reassignment.updateId
    }

    trait BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = UpdateServiceStub

      override def createService(channel: ManagedChannel): UpdateServiceStub =
        UpdateServiceGrpc.stub(channel)
    }

    trait SubscribeUpdateBase[Resp, Res]
        extends BaseCommand[GetUpdatesRequest, AutoCloseable, AutoCloseable]
        with SubscribeBase[GetUpdatesRequest, Resp, Res] {

      def beginExclusive: ParticipantOffset

      def endInclusive: Option[ParticipantOffset]

      def filter: TransactionFilter

      def verbose: Boolean

      override def createRequest(): Either[String, GetUpdatesRequest] = Right {
        GetUpdatesRequest(
          beginExclusive = Some(beginExclusive),
          endInclusive = endInclusive,
          verbose = verbose,
          filter = Some(filter),
        )
      }
    }

    final case class SubscribeTrees(
        override val observer: StreamObserver[UpdateTreeWrapper],
        override val beginExclusive: ParticipantOffset,
        override val endInclusive: Option[ParticipantOffset],
        override val filter: TransactionFilter,
        override val verbose: Boolean,
    )(override implicit val loggingContext: ErrorLoggingContext)
        extends SubscribeUpdateBase[GetUpdateTreesResponse, UpdateTreeWrapper] {
      override def doRequest(
          service: UpdateServiceStub,
          request: GetUpdatesRequest,
          rawObserver: StreamObserver[GetUpdateTreesResponse],
      ): Unit =
        service.getUpdateTrees(request, rawObserver)

      override def extractResults(
          response: GetUpdateTreesResponse
      ): IterableOnce[UpdateTreeWrapper] =
        response.update.transactionTree
          .map[UpdateTreeWrapper](TransactionTreeWrapper)
          .orElse(response.update.reassignment.map(ReassignmentWrapper(_)))
    }

    final case class SubscribeFlat(
        override val observer: StreamObserver[UpdateWrapper],
        override val beginExclusive: ParticipantOffset,
        override val endInclusive: Option[ParticipantOffset],
        override val filter: TransactionFilter,
        override val verbose: Boolean,
    )(override implicit val loggingContext: ErrorLoggingContext)
        extends SubscribeUpdateBase[GetUpdatesResponse, UpdateWrapper] {
      override def doRequest(
          service: UpdateServiceStub,
          request: GetUpdatesRequest,
          rawObserver: StreamObserver[GetUpdatesResponse],
      ): Unit =
        service.getUpdates(request, rawObserver)

      override def extractResults(response: GetUpdatesResponse): IterableOnce[UpdateWrapper] =
        response.update.transaction
          .map[UpdateWrapper](TransactionWrapper)
          .orElse(response.update.reassignment.map(ReassignmentWrapper(_)))
    }

    final case class GetTransactionById(parties: Set[LfPartyId], id: String)(implicit
        ec: ExecutionContext
    ) extends BaseCommand[GetTransactionByIdRequest, GetTransactionTreeResponse, Option[
          TransactionTree
        ]]
        with PrettyPrinting {
      override def createRequest(): Either[String, GetTransactionByIdRequest] = Right {
        GetTransactionByIdRequest(
          updateId = id,
          requestingParties = parties.toSeq,
        )
      }

      override def submitRequest(
          service: UpdateServiceStub,
          request: GetTransactionByIdRequest,
      ): Future[GetTransactionTreeResponse] = {
        // The Ledger API will throw an error if it can't find a transaction by ID.
        // However, as Canton is distributed, a transaction ID might show up later, so we don't treat this as
        // an error and change it to a None
        service.getTransactionTreeById(request).recover {
          case e: StatusRuntimeException if e.getStatus.getCode == Status.Code.NOT_FOUND =>
            GetTransactionTreeResponse(None)
        }
      }

      override def handleResponse(
          response: GetTransactionTreeResponse
      ): Either[String, Option[TransactionTree]] =
        Right(response.transaction)

      override def pretty: Pretty[GetTransactionById] =
        prettyOfClass(
          param("id", _.id.unquoted),
          param("parties", _.parties),
        )
    }

  }

  private[commands] trait SubmitCommand extends PrettyPrinting {
    def actAs: Seq[LfPartyId]
    def readAs: Seq[LfPartyId]
    def commands: Seq[Command]
    def workflowId: String
    def commandId: String
    def deduplicationPeriod: Option[DeduplicationPeriod]
    def submissionId: String
    def minLedgerTimeAbs: Option[Instant]
    def disclosedContracts: Seq[DisclosedContract]
    def domainId: Option[DomainId]
    def applicationId: String
    def packageIdSelectionPreference: Seq[LfPackageId]

    protected def mkCommand: Commands = Commands(
      workflowId = workflowId,
      applicationId = applicationId,
      commandId = if (commandId.isEmpty) UUID.randomUUID().toString else commandId,
      actAs = actAs,
      readAs = readAs,
      commands = commands,
      deduplicationPeriod = deduplicationPeriod.fold(
        Commands.DeduplicationPeriod.Empty: Commands.DeduplicationPeriod
      ) {
        case DeduplicationPeriod.DeduplicationDuration(duration) =>
          Commands.DeduplicationPeriod.DeduplicationDuration(
            ProtoConverter.DurationConverter.toProtoPrimitive(duration)
          )
        case DeduplicationPeriod.DeduplicationOffset(offset) =>
          Commands.DeduplicationPeriod.DeduplicationOffset(
            offset.toHexString
          )
      },
      minLedgerTimeAbs = minLedgerTimeAbs.map(ProtoConverter.InstantConverter.toProtoPrimitive),
      submissionId = submissionId,
      disclosedContracts = disclosedContracts,
      domainId = domainId.map(_.toProtoPrimitive).getOrElse(""),
      packageIdSelectionPreference = packageIdSelectionPreference.map(_.toString),
    )

    override def pretty: Pretty[this.type] =
      prettyOfClass(
        param("actAs", _.actAs),
        param("readAs", _.readAs),
        param("commandId", _.commandId.singleQuoted),
        param("workflowId", _.workflowId.singleQuoted),
        param("submissionId", _.submissionId.singleQuoted),
        param("deduplicationPeriod", _.deduplicationPeriod),
        paramIfDefined("minLedgerTimeAbs", _.minLedgerTimeAbs),
        paramWithoutValue("commands"),
      )
  }

  object CommandSubmissionService {
    trait BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = CommandSubmissionServiceStub
      override def createService(channel: ManagedChannel): CommandSubmissionServiceStub =
        CommandSubmissionServiceGrpc.stub(channel)
    }

    final case class Submit(
        override val actAs: Seq[LfPartyId],
        override val readAs: Seq[LfPartyId],
        override val commands: Seq[Command],
        override val workflowId: String,
        override val commandId: String,
        override val deduplicationPeriod: Option[DeduplicationPeriod],
        override val submissionId: String,
        override val minLedgerTimeAbs: Option[Instant],
        override val disclosedContracts: Seq[DisclosedContract],
        override val domainId: Option[DomainId],
        override val applicationId: String,
        override val packageIdSelectionPreference: Seq[LfPackageId],
    ) extends SubmitCommand
        with BaseCommand[SubmitRequest, SubmitResponse, Unit] {
      override def createRequest(): Either[String, SubmitRequest] = Right(
        SubmitRequest(commands = Some(mkCommand))
      )

      override def submitRequest(
          service: CommandSubmissionServiceStub,
          request: SubmitRequest,
      ): Future[SubmitResponse] = {
        service.submit(request)
      }

      override def handleResponse(response: SubmitResponse): Either[String, Unit] = Right(())
    }

    final case class SubmitAssignCommand(
        workflowId: String,
        applicationId: String,
        commandId: String,
        submitter: LfPartyId,
        submissionId: String,
        unassignId: String,
        source: DomainId,
        target: DomainId,
    ) extends BaseCommand[SubmitReassignmentRequest, SubmitReassignmentResponse, Unit] {
      override def createRequest(): Either[String, SubmitReassignmentRequest] = Right(
        SubmitReassignmentRequest(
          Some(
            ReassignmentCommand(
              workflowId = workflowId,
              applicationId = applicationId,
              commandId = commandId,
              submitter = submitter.toString,
              command = ReassignmentCommand.Command.AssignCommand(
                AssignCommand(
                  unassignId = unassignId,
                  source = source.toProtoPrimitive,
                  target = target.toProtoPrimitive,
                )
              ),
              submissionId = submissionId,
            )
          )
        )
      )

      override def submitRequest(
          service: CommandSubmissionServiceStub,
          request: SubmitReassignmentRequest,
      ): Future[SubmitReassignmentResponse] = {
        service.submitReassignment(request)
      }

      override def handleResponse(response: SubmitReassignmentResponse): Either[String, Unit] =
        Right(())
    }

    final case class SubmitUnassignCommand(
        workflowId: String,
        applicationId: String,
        commandId: String,
        submitter: LfPartyId,
        submissionId: String,
        contractId: LfContractId,
        source: DomainId,
        target: DomainId,
    ) extends BaseCommand[SubmitReassignmentRequest, SubmitReassignmentResponse, Unit] {
      override def createRequest(): Either[String, SubmitReassignmentRequest] = Right(
        SubmitReassignmentRequest(
          Some(
            ReassignmentCommand(
              workflowId = workflowId,
              applicationId = applicationId,
              commandId = commandId,
              submitter = submitter.toString,
              command = ReassignmentCommand.Command.UnassignCommand(
                UnassignCommand(
                  contractId = contractId.coid.toString,
                  source = source.toProtoPrimitive,
                  target = target.toProtoPrimitive,
                )
              ),
              submissionId = submissionId,
            )
          )
        )
      )

      override def submitRequest(
          service: CommandSubmissionServiceStub,
          request: SubmitReassignmentRequest,
      ): Future[SubmitReassignmentResponse] = {
        service.submitReassignment(request)
      }

      override def handleResponse(response: SubmitReassignmentResponse): Either[String, Unit] =
        Right(())
    }
  }

  object CommandService {
    trait BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = CommandServiceStub
      override def createService(channel: ManagedChannel): CommandServiceStub =
        CommandServiceGrpc.stub(channel)
    }

    final case class SubmitAndWaitTransactionTree(
        override val actAs: Seq[LfPartyId],
        override val readAs: Seq[LfPartyId],
        override val commands: Seq[Command],
        override val workflowId: String,
        override val commandId: String,
        override val deduplicationPeriod: Option[DeduplicationPeriod],
        override val submissionId: String,
        override val minLedgerTimeAbs: Option[Instant],
        override val disclosedContracts: Seq[DisclosedContract],
        override val domainId: Option[DomainId],
        override val applicationId: String,
        override val packageIdSelectionPreference: Seq[LfPackageId],
    ) extends SubmitCommand
        with BaseCommand[
          SubmitAndWaitRequest,
          SubmitAndWaitForTransactionTreeResponse,
          TransactionTree,
        ] {

      override def createRequest(): Either[String, SubmitAndWaitRequest] =
        Right(SubmitAndWaitRequest(commands = Some(mkCommand)))

      override def submitRequest(
          service: CommandServiceStub,
          request: SubmitAndWaitRequest,
      ): Future[SubmitAndWaitForTransactionTreeResponse] =
        service.submitAndWaitForTransactionTree(request)

      override def handleResponse(
          response: SubmitAndWaitForTransactionTreeResponse
      ): Either[String, TransactionTree] =
        response.transaction.toRight("Received response without any transaction tree")

      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class SubmitAndWaitTransaction(
        override val actAs: Seq[LfPartyId],
        override val readAs: Seq[LfPartyId],
        override val commands: Seq[Command],
        override val workflowId: String,
        override val commandId: String,
        override val deduplicationPeriod: Option[DeduplicationPeriod],
        override val submissionId: String,
        override val minLedgerTimeAbs: Option[Instant],
        override val disclosedContracts: Seq[DisclosedContract],
        override val domainId: Option[DomainId],
        override val applicationId: String,
        override val packageIdSelectionPreference: Seq[LfPackageId],
    ) extends SubmitCommand
        with BaseCommand[SubmitAndWaitRequest, SubmitAndWaitForTransactionResponse, Transaction] {

      override def createRequest(): Either[String, SubmitAndWaitRequest] =
        Right(SubmitAndWaitRequest(commands = Some(mkCommand)))

      override def submitRequest(
          service: CommandServiceStub,
          request: SubmitAndWaitRequest,
      ): Future[SubmitAndWaitForTransactionResponse] =
        service.submitAndWaitForTransaction(request)

      override def handleResponse(
          response: SubmitAndWaitForTransactionResponse
      ): Either[String, Transaction] =
        response.transaction.toRight("Received response without any transaction")

      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }
  }

  object StateService {
    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = StateServiceStub

      override def createService(channel: ManagedChannel): StateServiceStub =
        StateServiceGrpc.stub(channel)
    }

    final case class LedgerEnd()
        extends BaseCommand[GetLedgerEndRequest, GetLedgerEndResponse, ParticipantOffset] {

      override def createRequest(): Either[String, GetLedgerEndRequest] =
        Right(GetLedgerEndRequest())

      override def submitRequest(
          service: StateServiceStub,
          request: GetLedgerEndRequest,
      ): Future[GetLedgerEndResponse] =
        service.getLedgerEnd(request)

      override def handleResponse(
          response: GetLedgerEndResponse
      ): Either[String, ParticipantOffset] =
        response.offset.toRight("Empty LedgerEndResponse received without offset")
    }

    final case class GetConnectedDomains(partyId: LfPartyId)
        extends BaseCommand[
          GetConnectedDomainsRequest,
          GetConnectedDomainsResponse,
          GetConnectedDomainsResponse,
        ] {

      override def createRequest(): Either[String, GetConnectedDomainsRequest] =
        Right(GetConnectedDomainsRequest(partyId.toString))

      override def submitRequest(
          service: StateServiceStub,
          request: GetConnectedDomainsRequest,
      ): Future[GetConnectedDomainsResponse] =
        service.getConnectedDomains(request)

      override def handleResponse(
          response: GetConnectedDomainsResponse
      ): Either[String, GetConnectedDomainsResponse] =
        Right(response)
    }

    final case class GetActiveContracts(
        observer: StreamObserver[GetActiveContractsResponse],
        parties: Set[LfPartyId],
        limit: PositiveInt,
        templateFilter: Seq[TemplateId] = Seq.empty,
        activeAtOffset: String = "",
        verbose: Boolean = true,
        timeout: FiniteDuration,
        includeCreatedEventBlob: Boolean = false,
    )(override implicit val loggingContext: ErrorLoggingContext)
        extends BaseCommand[GetActiveContractsRequest, AutoCloseable, AutoCloseable]
        with SubscribeBase[
          GetActiveContractsRequest,
          GetActiveContractsResponse,
          GetActiveContractsResponse,
        ] {

      override def createRequest(): Either[String, GetActiveContractsRequest] = {
        val filter =
          if (templateFilter.nonEmpty) {
            Filters(
              templateFilter.map(tId =>
                CumulativeFilter(
                  IdentifierFilter.TemplateFilter(
                    TemplateFilter(Some(tId.toIdentifier), includeCreatedEventBlob)
                  )
                )
              )
            )
          } else Filters.defaultInstance
        Right(
          GetActiveContractsRequest(
            filter = Some(TransactionFilter(parties.map((_, filter)).toMap)),
            verbose = verbose,
            activeAtOffset = activeAtOffset,
          )
        )
      }

      override def doRequest(
          service: StateServiceStub,
          request: GetActiveContractsRequest,
          rawObserver: StreamObserver[GetActiveContractsResponse],
      ): Unit =
        service.getActiveContracts(request, rawObserver)

      override def extractResults(
          response: GetActiveContractsResponse
      ): IterableOnce[GetActiveContractsResponse] = List(response)

      // fetching ACS might take long if we fetch a lot of data
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout
    }
  }

  final case class CompletionWrapper(
      completion: Completion,
      checkpoint: Checkpoint,
      domainId: DomainId,
  )

  object CommandCompletionService {
    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = CommandCompletionServiceStub

      override def createService(channel: ManagedChannel): CommandCompletionServiceStub =
        CommandCompletionServiceGrpc.stub(channel)
    }

    final case class CompletionRequest(
        partyId: LfPartyId,
        beginOffset: ParticipantOffset,
        expectedCompletions: Int,
        timeout: java.time.Duration,
        applicationId: String,
    )(filter: CompletionWrapper => Boolean, scheduler: ScheduledExecutorService)
        extends BaseCommand[
          CompletionStreamRequest,
          Seq[CompletionWrapper],
          Seq[CompletionWrapper],
        ] {

      override def createRequest(): Either[String, CompletionStreamRequest] =
        Right(
          CompletionStreamRequest(
            applicationId = applicationId,
            parties = Seq(partyId),
            beginExclusive = Some(beginOffset),
          )
        )

      override def submitRequest(
          service: CommandCompletionServiceStub,
          request: CompletionStreamRequest,
      ): Future[Seq[CompletionWrapper]] = {
        import scala.jdk.DurationConverters.*
        GrpcAdminCommand
          .streamedResponse[CompletionStreamRequest, CompletionStreamResponse, CompletionWrapper](
            service.completionStream,
            response =>
              List(
                CompletionWrapper(
                  completion = response.completion.getOrElse(
                    throw new IllegalStateException("Completion should be present.")
                  ),
                  checkpoint = response.checkpoint.getOrElse(
                    throw new IllegalStateException("Checkpoint should be present.")
                  ),
                  domainId = DomainId.tryFromString(response.domainId),
                )
              ).filter(filter),
            request,
            expectedCompletions,
            timeout.toScala,
            scheduler,
          )
      }

      override def handleResponse(
          response: Seq[CompletionWrapper]
      ): Either[String, Seq[CompletionWrapper]] =
        Right(response)

      override def timeoutType: TimeoutType = ServerEnforcedTimeout
    }

    final case class CompletionCheckpointRequest(
        partyId: LfPartyId,
        beginExclusive: ParticipantOffset,
        expectedCompletions: Int,
        timeout: config.NonNegativeDuration,
        applicationId: String,
    )(filter: Completion => Boolean, scheduler: ScheduledExecutorService)
        extends BaseCommand[CompletionStreamRequest, Seq[(Completion, Option[Checkpoint])], Seq[
          (Completion, Option[Checkpoint])
        ]] {

      override def createRequest(): Either[String, CompletionStreamRequest] =
        Right(
          CompletionStreamRequest(
            applicationId = applicationId,
            parties = Seq(partyId),
            beginExclusive = Some(beginExclusive),
          )
        )

      override def submitRequest(
          service: CommandCompletionServiceStub,
          request: CompletionStreamRequest,
      ): Future[Seq[(Completion, Option[Checkpoint])]] = {
        def extract(response: CompletionStreamResponse): Seq[(Completion, Option[Checkpoint])] = {
          val checkpoint = response.checkpoint

          response.completion.filter(filter).map(_ -> checkpoint).toList
        }

        GrpcAdminCommand.streamedResponse[
          CompletionStreamRequest,
          CompletionStreamResponse,
          (Completion, Option[Checkpoint]),
        ](
          service.completionStream,
          extract,
          request,
          expectedCompletions,
          timeout.asFiniteApproximation,
          scheduler,
        )
      }

      override def handleResponse(
          response: Seq[(Completion, Option[Checkpoint])]
      ): Either[String, Seq[(Completion, Option[Checkpoint])]] =
        Right(response)

      override def timeoutType: TimeoutType = ServerEnforcedTimeout
    }

    final case class Subscribe(
        observer: StreamObserver[CompletionWrapper],
        parties: Seq[String],
        offset: Option[ParticipantOffset],
        applicationId: String,
    )(implicit loggingContext: ErrorLoggingContext)
        extends BaseCommand[CompletionStreamRequest, AutoCloseable, AutoCloseable] {
      // The subscription should never be cut short because of a gRPC timeout
      override def timeoutType: TimeoutType = ServerEnforcedTimeout

      override def createRequest(): Either[String, CompletionStreamRequest] = Right {
        CompletionStreamRequest(
          applicationId = applicationId,
          parties = parties,
          beginExclusive = offset,
        )
      }

      override def submitRequest(
          service: CommandCompletionServiceStub,
          request: CompletionStreamRequest,
      ): Future[AutoCloseable] = {
        val rawObserver = new ForwardingStreamObserver[CompletionStreamResponse, CompletionWrapper](
          observer,
          response =>
            List(
              CompletionWrapper(
                completion = response.completion.getOrElse(
                  throw new IllegalStateException("Completion should be present.")
                ),
                checkpoint = response.checkpoint.getOrElse(
                  throw new IllegalStateException("Checkpoint should be present.")
                ),
                domainId = DomainId.tryFromString(response.domainId),
              )
            ),
        )
        val context = Context.current().withCancellation()
        context.run(() => service.completionStream(request, rawObserver))
        Future.successful(context)
      }

      override def handleResponse(response: AutoCloseable): Either[String, AutoCloseable] = Right(
        response
      )
    }
  }

  object Time {
    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = TimeServiceStub

      override def createService(channel: ManagedChannel): TimeServiceStub =
        TimeServiceGrpc.stub(channel)
    }

    final object Get
        extends BaseCommand[
          GetTimeRequest,
          GetTimeResponse,
          CantonTimestamp,
        ] {

      override def submitRequest(
          service: TimeServiceStub,
          request: GetTimeRequest,
      ): Future[GetTimeResponse] = {
        service.getTime(request)
      }

      /** Create the request from configured options
        */
      override def createRequest(): Either[String, GetTimeRequest] = Right(GetTimeRequest())

      /** Handle the response the service has provided
        */
      override def handleResponse(
          response: GetTimeResponse
      ): Either[String, CantonTimestamp] =
        for {
          prototTimestamp <- response.currentTime.map(Right(_)).getOrElse(Left("currentTime empty"))
          result <- CantonTimestamp.fromProtoTimestamp(prototTimestamp).left.map(_.message)
        } yield result
    }

    final case class Set(currentTime: CantonTimestamp, newTime: CantonTimestamp)
        extends BaseCommand[
          SetTimeRequest,
          Empty,
          Unit,
        ] {

      override def submitRequest(service: TimeServiceStub, request: SetTimeRequest): Future[Empty] =
        service.setTime(request)

      override def createRequest(): Either[String, SetTimeRequest] =
        Right(
          SetTimeRequest(
            currentTime = Some(currentTime.toProtoTimestamp),
            newTime = Some(newTime.toProtoTimestamp),
          )
        )

      /** Handle the response the service has provided
        */
      override def handleResponse(response: Empty): Either[String, Unit] = Right(())

    }

  }

  object QueryService {

    abstract class BaseCommand[Req, Res] extends GrpcAdminCommand[Req, Res, Res] {
      override type Svc = EventQueryServiceStub

      override def createService(channel: ManagedChannel): EventQueryServiceStub =
        EventQueryServiceGrpc.stub(channel)

      override def handleResponse(response: Res): Either[String, Res] = Right(response)
    }

    final case class GetEventsByContractId(
        contractId: String,
        requestingParties: Seq[String],
    ) extends BaseCommand[
          GetEventsByContractIdRequest,
          GetEventsByContractIdResponse,
        ] {

      override def createRequest(): Either[String, GetEventsByContractIdRequest] = Right(
        GetEventsByContractIdRequest(
          contractId = contractId,
          requestingParties = requestingParties,
        )
      )

      override def submitRequest(
          service: EventQueryServiceStub,
          request: GetEventsByContractIdRequest,
      ): Future[GetEventsByContractIdResponse] = service.getEventsByContractId(request)

    }
  }
}
