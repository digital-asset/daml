// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.jwt.JwksUrl
import com.daml.ledger.api.v2.admin.command_inspection_service.CommandInspectionServiceGrpc.CommandInspectionServiceStub
import com.daml.ledger.api.v2.admin.command_inspection_service.{
  CommandInspectionServiceGrpc,
  CommandState,
  GetCommandStatusRequest,
  GetCommandStatusResponse,
}
import com.daml.ledger.api.v2.admin.identity_provider_config_service.*
import com.daml.ledger.api.v2.admin.identity_provider_config_service.IdentityProviderConfigServiceGrpc.IdentityProviderConfigServiceStub
import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.admin.package_management_service.*
import com.daml.ledger.api.v2.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementServiceStub
import com.daml.ledger.api.v2.admin.participant_pruning_service.*
import com.daml.ledger.api.v2.admin.participant_pruning_service.ParticipantPruningServiceGrpc.ParticipantPruningServiceStub
import com.daml.ledger.api.v2.admin.party_management_service.*
import com.daml.ledger.api.v2.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementServiceStub
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
import com.daml.ledger.api.v2.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionServiceStub
import com.daml.ledger.api.v2.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionStreamRequest,
  CompletionStreamResponse,
}
import com.daml.ledger.api.v2.command_service.CommandServiceGrpc.CommandServiceStub
import com.daml.ledger.api.v2.command_service.{
  CommandServiceGrpc,
  SubmitAndWaitForReassignmentRequest,
  SubmitAndWaitForReassignmentResponse,
  SubmitAndWaitForTransactionRequest,
  SubmitAndWaitForTransactionResponse,
}
import com.daml.ledger.api.v2.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionServiceStub
import com.daml.ledger.api.v2.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitReassignmentRequest,
  SubmitReassignmentResponse,
  SubmitRequest,
  SubmitResponse,
}
import com.daml.ledger.api.v2.commands.{Command, Commands, DisclosedContract, PrefetchContractKey}
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.event_query_service.EventQueryServiceGrpc.EventQueryServiceStub
import com.daml.ledger.api.v2.event_query_service.{
  EventQueryServiceGrpc,
  GetEventsByContractIdRequest,
  GetEventsByContractIdResponse,
}
import com.daml.ledger.api.v2.interactive.interactive_submission_service.InteractiveSubmissionServiceGrpc.InteractiveSubmissionServiceStub
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  ExecuteSubmissionAndWaitForTransactionRequest,
  ExecuteSubmissionAndWaitForTransactionResponse,
  ExecuteSubmissionAndWaitRequest,
  ExecuteSubmissionAndWaitResponse,
  ExecuteSubmissionRequest,
  ExecuteSubmissionResponse,
  GetPreferredPackageVersionRequest,
  GetPreferredPackageVersionResponse,
  GetPreferredPackagesRequest,
  GetPreferredPackagesResponse,
  HashingSchemeVersion,
  InteractiveSubmissionServiceGrpc,
  MinLedgerTime,
  PackagePreference,
  PackageVettingRequirement,
  PartySignatures,
  PrepareSubmissionRequest,
  PrepareSubmissionResponse,
  PreparedTransaction,
  SinglePartySignatures,
}
import com.daml.ledger.api.v2.reassignment.{
  AssignedEvent,
  Reassignment,
  ReassignmentEvent,
  UnassignedEvent,
}
import com.daml.ledger.api.v2.reassignment_commands.{
  AssignCommand,
  ReassignmentCommand,
  ReassignmentCommands,
  UnassignCommand,
}
import com.daml.ledger.api.v2.state_service.StateServiceGrpc.StateServiceStub
import com.daml.ledger.api.v2.state_service.{
  GetActiveContractsRequest,
  GetActiveContractsResponse,
  GetConnectedSynchronizersRequest,
  GetConnectedSynchronizersResponse,
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
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  TemplateFilter,
  TransactionFormat,
  TransactionShape,
  UpdateFormat,
  WildcardFilter,
}
import com.daml.ledger.api.v2.update_service.UpdateServiceGrpc.UpdateServiceStub
import com.daml.ledger.api.v2.update_service.{
  GetUpdateByIdRequest,
  GetUpdateByOffsetRequest,
  GetUpdateResponse,
  GetUpdatesRequest,
  GetUpdatesResponse,
  UpdateServiceGrpc,
}
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  ServerEnforcedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.{
  EmptyOrAssignedWrapper,
  EmptyOrUnassignedWrapper,
}
import com.digitalasset.canton.admin.api.client.data.{
  LedgerApiUser,
  ListLedgerApiUsersResult,
  TemplateId,
  UserRights,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.{CantonTimestamp, DeduplicationPeriod}
import com.digitalasset.canton.ledger.api.{
  IdentityProviderConfig as ApiIdentityProviderConfig,
  IdentityProviderId,
}
import com.digitalasset.canton.ledger.client.services.admin.IdentityProviderConfigClient
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.networking.grpc.ForwardingStreamObserver
import com.digitalasset.canton.platform.apiserver.execution.CommandStatus
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.{LfPackageId, LfPackageName, LfPartyId}
import com.google.protobuf.empty.Empty
import com.google.protobuf.field_mask.FieldMask
import io.grpc.*
import io.grpc.stub.StreamObserver
import io.scalaland.chimney.dsl.*

import java.time.Instant
import java.util.UUID
import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

object LedgerApiCommands {

  final val defaultUserId = "CantonConsole"

  object PartyManagementService {
    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = PartyManagementServiceStub

      override def createService(channel: ManagedChannel): PartyManagementServiceStub =
        PartyManagementServiceGrpc.stub(channel)
    }

    final case class AllocateParty(
        partyIdHint: String,
        annotations: Map[String, String],
        identityProviderId: String,
        synchronizerId: Option[SynchronizerId],
        userId: String,
    ) extends BaseCommand[AllocatePartyRequest, AllocatePartyResponse, PartyDetails] {
      override protected def createRequest(): Either[String, AllocatePartyRequest] =
        Right(
          AllocatePartyRequest(
            partyIdHint = partyIdHint,
            localMetadata = Some(ObjectMeta(resourceVersion = "", annotations = annotations)),
            identityProviderId = identityProviderId,
            synchronizerId = synchronizerId.map(_.toProtoPrimitive).getOrElse(""),
            userId = userId,
          )
        )
      override protected def submitRequest(
          service: PartyManagementServiceStub,
          request: AllocatePartyRequest,
      ): Future[AllocatePartyResponse] =
        service.allocateParty(request)
      override protected def handleResponse(
          response: AllocatePartyResponse
      ): Either[String, PartyDetails] =
        response.partyDetails.toRight("Party could not be created")
    }

    final case class Update(
        party: PartyId,
        annotationsUpdate: Option[Map[String, String]],
        resourceVersionO: Option[String],
        identityProviderId: String,
    ) extends BaseCommand[UpdatePartyDetailsRequest, UpdatePartyDetailsResponse, PartyDetails] {

      override protected def submitRequest(
          service: PartyManagementServiceStub,
          request: UpdatePartyDetailsRequest,
      ): Future[UpdatePartyDetailsResponse] =
        service.updatePartyDetails(request)

      override protected def createRequest(): Either[String, UpdatePartyDetailsRequest] = {
        val metadata = ObjectMeta(
          annotations = annotationsUpdate.getOrElse(Map.empty),
          resourceVersion = resourceVersionO.getOrElse(""),
        )
        val partyDetails =
          PartyDetails(
            party = party.toProtoPrimitive,
            isLocal = false,
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

      override protected def handleResponse(
          response: UpdatePartyDetailsResponse
      ): Either[String, PartyDetails] =
        response.partyDetails.toRight("Failed to update the party details")

    }

    final case class ListKnownParties(identityProviderId: String)
        extends BaseCommand[ListKnownPartiesRequest, ListKnownPartiesResponse, Seq[
          PartyDetails
        ]] {
      override protected def createRequest(): Either[String, ListKnownPartiesRequest] =
        Right(
          ListKnownPartiesRequest(
            pageToken = "",
            pageSize = 0,
            identityProviderId = identityProviderId,
          )
        )
      override protected def submitRequest(
          service: PartyManagementServiceStub,
          request: ListKnownPartiesRequest,
      ): Future[ListKnownPartiesResponse] =
        service.listKnownParties(request)
      override protected def handleResponse(
          response: ListKnownPartiesResponse
      ): Either[String, Seq[PartyDetails]] =
        Right(response.partyDetails)
    }

    final case class GetParty(party: PartyId, identityProviderId: String)
        extends BaseCommand[GetPartiesRequest, GetPartiesResponse, PartyDetails] {

      override protected def createRequest(): Either[String, GetPartiesRequest] =
        Right(
          GetPartiesRequest(
            parties = Seq(party.toProtoPrimitive),
            identityProviderId = identityProviderId,
          )
        )

      override protected def submitRequest(
          service: PartyManagementServiceStub,
          request: GetPartiesRequest,
      ): Future[GetPartiesResponse] = service.getParties(request)

      override protected def handleResponse(
          response: GetPartiesResponse
      ): Either[String, PartyDetails] =
        response.partyDetails.headOption.toRight("PARTY_NOT_FOUND")
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

      override protected def submitRequest(
          service: PartyManagementServiceStub,
          request: UpdatePartyIdentityProviderIdRequest,
      ): Future[UpdatePartyIdentityProviderIdResponse] =
        service.updatePartyIdentityProviderId(request)

      override protected def createRequest(): Either[String, UpdatePartyIdentityProviderIdRequest] =
        Right(
          UpdatePartyIdentityProviderIdRequest(
            party = party.toProtoPrimitive,
            sourceIdentityProviderId = sourceIdentityProviderId,
            targetIdentityProviderId = targetIdentityProviderId,
          )
        )

      override protected def handleResponse(
          response: UpdatePartyIdentityProviderIdResponse
      ): Either[String, Unit] = Either.unit

    }
  }

  object PackageManagementService {

    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = PackageManagementServiceStub
      override def createService(channel: ManagedChannel): PackageManagementServiceStub =
        PackageManagementServiceGrpc.stub(channel)
    }

    final case class UploadDarFile(darPath: String)
        extends BaseCommand[UploadDarFileRequest, UploadDarFileResponse, Unit] {

      override protected def createRequest(): Either[String, UploadDarFileRequest] =
        for {
          bytes <- BinaryFileUtil.readByteStringFromFile(darPath)
        } yield UploadDarFileRequest(bytes, submissionId = "")
      override protected def submitRequest(
          service: PackageManagementServiceStub,
          request: UploadDarFileRequest,
      ): Future[UploadDarFileResponse] =
        service.uploadDarFile(request)
      override protected def handleResponse(response: UploadDarFileResponse): Either[String, Unit] =
        Either.unit

      // package upload time might take long if it is a big package
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ValidateDarFile(darPath: String)
        extends BaseCommand[ValidateDarFileRequest, ValidateDarFileResponse, Unit] {

      override protected def createRequest(): Either[String, ValidateDarFileRequest] =
        for {
          bytes <- BinaryFileUtil.readByteStringFromFile(darPath)
        } yield ValidateDarFileRequest(bytes, submissionId = "")

      override protected def submitRequest(
          service: PackageManagementServiceStub,
          request: ValidateDarFileRequest,
      ): Future[ValidateDarFileResponse] =
        service.validateDarFile(request)
      override protected def handleResponse(
          response: ValidateDarFileResponse
      ): Either[String, Unit] = Either.unit

      override def timeoutType: TimeoutType = DefaultUnboundedTimeout
    }

    final case class ListKnownPackages(limit: PositiveInt)
        extends BaseCommand[ListKnownPackagesRequest, ListKnownPackagesResponse, Seq[
          PackageDetails
        ]] {

      override protected def createRequest(): Either[String, ListKnownPackagesRequest] = Right(
        ListKnownPackagesRequest()
      )

      override protected def submitRequest(
          service: PackageManagementServiceStub,
          request: ListKnownPackagesRequest,
      ): Future[ListKnownPackagesResponse] =
        service.listKnownPackages(request)

      override protected def handleResponse(
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
      override protected def createRequest(): Either[String, GetCommandStatusRequest] = Right(
        GetCommandStatusRequest(commandIdPrefix = commandIdPrefix, state = state, limit = limit)
      )

      override protected def submitRequest(
          service: CommandInspectionServiceStub,
          request: GetCommandStatusRequest,
      ): Future[GetCommandStatusResponse] = service.getCommandStatus(request)

      override protected def handleResponse(
          response: GetCommandStatusResponse
      ): Either[String, Seq[CommandStatus]] =
        response.commandStatus.traverse(CommandStatus.fromProto).leftMap(_.message)
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

    final case class Prune(pruneUpTo: Long) extends BaseCommand[PruneRequest, PruneResponse, Unit] {

      override def timeoutType: TimeoutType =
        DefaultUnboundedTimeout // pruning can take a very long time

      override protected def createRequest(): Either[String, PruneRequest] =
        Right(
          PruneRequest(
            pruneUpTo,
            submissionId = "",
            // canton always prunes divulged contracts both in the ledger api index-db and in canton stores
            pruneAllDivulgedContracts = true,
          )
        )

      override protected def submitRequest(
          service: ParticipantPruningServiceStub,
          request: PruneRequest,
      ): Future[PruneResponse] =
        service.prune(request)

      override protected def handleResponse(response: PruneResponse): Either[String, Unit] =
        Either.unit
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
      def identityProviderAdmin: Boolean
      def readAsAnyParty: Boolean

      protected def getRights: Seq[UserRight] =
        actAs.toSeq.map(x => UserRight.defaultInstance.withCanActAs(UserRight.CanActAs(x))) ++
          readAs.toSeq.map(x => UserRight.defaultInstance.withCanReadAs(UserRight.CanReadAs(x))) ++
          (if (participantAdmin)
             Seq(UserRight.defaultInstance.withParticipantAdmin(UserRight.ParticipantAdmin()))
           else Seq()) ++
          (if (identityProviderAdmin)
             Seq(
               UserRight.defaultInstance
                 .withIdentityProviderAdmin(UserRight.IdentityProviderAdmin())
             )
           else Seq()) ++
          (if (readAsAnyParty)
             Seq(UserRight.defaultInstance.withCanReadAsAnyParty(UserRight.CanReadAsAnyParty()))
           else Seq())
    }

    final case class Create(
        id: String,
        actAs: Set[LfPartyId],
        primaryParty: Option[LfPartyId],
        readAs: Set[LfPartyId],
        participantAdmin: Boolean,
        identityProviderAdmin: Boolean,
        isDeactivated: Boolean,
        annotations: Map[String, String],
        identityProviderId: String,
        readAsAnyParty: Boolean,
    ) extends BaseCommand[CreateUserRequest, CreateUserResponse, LedgerApiUser]
        with HasRights {

      override protected def submitRequest(
          service: UserManagementServiceStub,
          request: CreateUserRequest,
      ): Future[CreateUserResponse] =
        service.createUser(request)

      override protected def createRequest(): Either[String, CreateUserRequest] = Right(
        CreateUserRequest(
          user = Some(
            User(
              id = id,
              primaryParty = primaryParty.getOrElse(""),
              isDeactivated = isDeactivated,
              metadata = Some(ObjectMeta(resourceVersion = "", annotations = annotations)),
              identityProviderId = identityProviderId,
            )
          ),
          rights = getRights,
        )
      )

      override protected def handleResponse(
          response: CreateUserResponse
      ): Either[String, LedgerApiUser] =
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

      override protected def submitRequest(
          service: UserManagementServiceStub,
          request: UpdateUserRequest,
      ): Future[UpdateUserResponse] =
        service.updateUser(request)

      override protected def createRequest(): Either[String, UpdateUserRequest] = {
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

      override protected def handleResponse(
          response: UpdateUserResponse
      ): Either[String, LedgerApiUser] =
        ProtoConverter
          .parseRequired(LedgerApiUser.fromProtoV0, "user", response.user)
          .leftMap(_.toString)

    }

    final case class Get(
        id: String,
        identityProviderId: String,
    ) extends BaseCommand[GetUserRequest, GetUserResponse, LedgerApiUser] {

      override protected def submitRequest(
          service: UserManagementServiceStub,
          request: GetUserRequest,
      ): Future[GetUserResponse] =
        service.getUser(request)

      override protected def createRequest(): Either[String, GetUserRequest] = Right(
        GetUserRequest(
          userId = id,
          identityProviderId = identityProviderId,
        )
      )

      override protected def handleResponse(
          response: GetUserResponse
      ): Either[String, LedgerApiUser] =
        ProtoConverter
          .parseRequired(LedgerApiUser.fromProtoV0, "user", response.user)
          .leftMap(_.toString)

    }

    final case class Delete(
        id: String,
        identityProviderId: String,
    ) extends BaseCommand[DeleteUserRequest, DeleteUserResponse, Unit] {

      override protected def submitRequest(
          service: UserManagementServiceStub,
          request: DeleteUserRequest,
      ): Future[DeleteUserResponse] =
        service.deleteUser(request)

      override protected def createRequest(): Either[String, DeleteUserRequest] = Right(
        DeleteUserRequest(
          userId = id,
          identityProviderId = identityProviderId,
        )
      )

      override protected def handleResponse(response: DeleteUserResponse): Either[String, Unit] =
        Either.unit

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

      override protected def submitRequest(
          service: UserManagementServiceStub,
          request: UpdateUserIdentityProviderIdRequest,
      ): Future[UpdateUserIdentityProviderIdResponse] =
        service.updateUserIdentityProviderId(request)

      override protected def createRequest(): Either[String, UpdateUserIdentityProviderIdRequest] =
        Right(
          UpdateUserIdentityProviderIdRequest(
            userId = id,
            sourceIdentityProviderId = sourceIdentityProviderId,
            targetIdentityProviderId = targetIdentityProviderId,
          )
        )

      override protected def handleResponse(
          response: UpdateUserIdentityProviderIdResponse
      ): Either[String, Unit] = Either.unit

    }

    final case class List(
        filterUser: String,
        pageToken: String,
        pageSize: Int,
        identityProviderId: String,
    ) extends BaseCommand[ListUsersRequest, ListUsersResponse, ListLedgerApiUsersResult] {

      override protected def submitRequest(
          service: UserManagementServiceStub,
          request: ListUsersRequest,
      ): Future[ListUsersResponse] =
        service.listUsers(request)

      override protected def createRequest(): Either[String, ListUsersRequest] = Right(
        ListUsersRequest(
          pageToken = pageToken,
          pageSize = pageSize,
          identityProviderId = identityProviderId,
        )
      )

      override protected def handleResponse(
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
          identityProviderAdmin: Boolean,
          identityProviderId: String,
          readAsAnyParty: Boolean,
      ) extends BaseCommand[GrantUserRightsRequest, GrantUserRightsResponse, UserRights]
          with HasRights {

        override protected def submitRequest(
            service: UserManagementServiceStub,
            request: GrantUserRightsRequest,
        ): Future[GrantUserRightsResponse] =
          service.grantUserRights(request)

        override protected def createRequest(): Either[String, GrantUserRightsRequest] = Right(
          GrantUserRightsRequest(
            userId = id,
            rights = getRights,
            identityProviderId = identityProviderId,
          )
        )

        override protected def handleResponse(
            response: GrantUserRightsResponse
        ): Either[String, UserRights] =
          UserRights.fromProtoV0(response.newlyGrantedRights).leftMap(_.toString)

      }

      final case class Revoke(
          id: String,
          actAs: Set[LfPartyId],
          readAs: Set[LfPartyId],
          participantAdmin: Boolean,
          identityProviderAdmin: Boolean,
          identityProviderId: String,
          readAsAnyParty: Boolean,
      ) extends BaseCommand[RevokeUserRightsRequest, RevokeUserRightsResponse, UserRights]
          with HasRights {

        override protected def submitRequest(
            service: UserManagementServiceStub,
            request: RevokeUserRightsRequest,
        ): Future[RevokeUserRightsResponse] =
          service.revokeUserRights(request)

        override protected def createRequest(): Either[String, RevokeUserRightsRequest] = Right(
          RevokeUserRightsRequest(
            userId = id,
            rights = getRights,
            identityProviderId = identityProviderId,
          )
        )

        override protected def handleResponse(
            response: RevokeUserRightsResponse
        ): Either[String, UserRights] =
          UserRights.fromProtoV0(response.newlyRevokedRights).leftMap(_.toString)

      }

      final case class List(id: String, identityProviderId: String)
          extends BaseCommand[ListUserRightsRequest, ListUserRightsResponse, UserRights] {

        override protected def submitRequest(
            service: UserManagementServiceStub,
            request: ListUserRightsRequest,
        ): Future[ListUserRightsResponse] =
          service.listUserRights(request)

        override protected def createRequest(): Either[String, ListUserRightsRequest] = Right(
          ListUserRightsRequest(userId = id, identityProviderId = identityProviderId)
        )

        override protected def handleResponse(
            response: ListUserRightsResponse
        ): Either[String, UserRights] =
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

      override protected def submitRequest(
          service: IdentityProviderConfigServiceStub,
          request: CreateIdentityProviderConfigRequest,
      ): Future[CreateIdentityProviderConfigResponse] =
        service.createIdentityProviderConfig(request)

      override protected def createRequest(): Either[String, CreateIdentityProviderConfigRequest] =
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

      override protected def handleResponse(
          response: CreateIdentityProviderConfigResponse
      ): Either[String, IdentityProviderConfig] =
        response.identityProviderConfig.toRight("config could not be created")
    }

    final case class Update(
        identityProviderConfig: ApiIdentityProviderConfig,
        updateMask: FieldMask,
    ) extends BaseCommand[
          UpdateIdentityProviderConfigRequest,
          UpdateIdentityProviderConfigResponse,
          IdentityProviderConfig,
        ] {

      override protected def submitRequest(
          service: IdentityProviderConfigServiceStub,
          request: UpdateIdentityProviderConfigRequest,
      ): Future[UpdateIdentityProviderConfigResponse] =
        service.updateIdentityProviderConfig(request)

      override protected def createRequest(): Either[String, UpdateIdentityProviderConfigRequest] =
        Right(
          UpdateIdentityProviderConfigRequest(
            identityProviderConfig =
              Some(IdentityProviderConfigClient.toProtoConfig(identityProviderConfig)),
            Some(updateMask),
          )
        )

      override protected def handleResponse(
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

      override protected def submitRequest(
          service: IdentityProviderConfigServiceStub,
          request: DeleteIdentityProviderConfigRequest,
      ): Future[DeleteIdentityProviderConfigResponse] =
        service.deleteIdentityProviderConfig(request)

      override protected def createRequest(): Either[String, DeleteIdentityProviderConfigRequest] =
        Right(
          DeleteIdentityProviderConfigRequest(identityProviderId =
            identityProviderId.toRequestString
          )
        )

      override protected def handleResponse(
          response: DeleteIdentityProviderConfigResponse
      ): Either[String, Unit] =
        Either.unit
    }

    final case class Get(identityProviderId: IdentityProviderId)
        extends BaseCommand[
          GetIdentityProviderConfigRequest,
          GetIdentityProviderConfigResponse,
          IdentityProviderConfig,
        ] {

      override protected def submitRequest(
          service: IdentityProviderConfigServiceStub,
          request: GetIdentityProviderConfigRequest,
      ): Future[GetIdentityProviderConfigResponse] =
        service.getIdentityProviderConfig(request)

      override protected def createRequest(): Either[String, GetIdentityProviderConfigRequest] =
        Right(
          GetIdentityProviderConfigRequest(identityProviderId.toRequestString)
        )

      override protected def handleResponse(
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

      override protected def submitRequest(
          service: IdentityProviderConfigServiceStub,
          request: ListIdentityProviderConfigsRequest,
      ): Future[ListIdentityProviderConfigsResponse] =
        service.listIdentityProviderConfigs(request)

      override protected def createRequest(): Either[String, ListIdentityProviderConfigsRequest] =
        Right(
          ListIdentityProviderConfigsRequest()
        )

      override protected def handleResponse(
          response: ListIdentityProviderConfigsResponse
      ): Either[String, Seq[IdentityProviderConfig]] =
        Right(response.identityProviderConfigs)
    }

  }

  object UpdateService {

    sealed trait UpdateWrapper {
      def updateId: String
      def isUnassignment = this match {
        case _: UnassignedWrapper => true
        case _ => false
      }
      def createEvents: Iterator[CreatedEvent] =
        this match {
          case UpdateService.TransactionWrapper(t) => t.events.iterator.map(_.getCreated)
          case u: UpdateService.AssignedWrapper => u.events.iterator.flatMap(_.createdEvent)
          case _: UpdateService.UnassignedWrapper => Iterator.empty
          case _: UpdateService.TopologyTransactionWrapper => Iterator.empty
        }

      def synchronizerId: String
    }
    final case class TransactionWrapper(transaction: Transaction) extends UpdateWrapper {
      override def updateId: String = transaction.updateId

      override def synchronizerId: String = transaction.synchronizerId
    }
    final case class TopologyTransactionWrapper(topologyTransaction: TopologyTransaction)
        extends UpdateWrapper {
      override def updateId: String = topologyTransaction.updateId

      override def synchronizerId: String = topologyTransaction.synchronizerId
    }
    sealed trait ReassignmentWrapper extends UpdateWrapper {
      override def updateId: String = reassignment.updateId

      def reassignment: Reassignment
    }

    object ReassignmentWrapper {
      def apply(reassignment: Reassignment): ReassignmentWrapper =
        reassignment.events match {
          case ReassignmentEvent(ReassignmentEvent.Event.Assigned(head)) +: tail =>
            AssignedWrapper(
              reassignment,
              head +: validateRest(head, tail, _.event.assigned),
            )
          case ReassignmentEvent(ReassignmentEvent.Event.Unassigned(head)) +: tail =>
            UnassignedWrapper(
              reassignment,
              head +: validateRest(head, tail, _.event.unassigned),
            )
          case _ =>
            throw new IllegalStateException(
              s"Invalid reassignment events: ${reassignment.events}"
            )
        }

      // Fields shared by AssignedEvent and UnassignedEvent that must be invariant with a batch.
      private type ValidatableEvent = {
        val source: String; val target: String; val reassignmentId: String
      }

      import scala.language.reflectiveCalls
      private def validateRest[E <: ValidatableEvent](
          first: E,
          rest: Seq[ReassignmentEvent],
          getEvent: ReassignmentEvent => Option[E],
      ): Seq[E] = rest match {
        case hd +: tl =>
          getEvent(hd) match {
            case Some(e) =>
              if (
                e.reassignmentId != first.reassignmentId ||
                e.source != first.source ||
                e.target != first.target
              ) throw new IllegalStateException(s"Invalid event batch elements: $first vs $e")
              e +: validateRest(first, tl, getEvent)
            case None => throw new IllegalStateException(s"Inconsistent event type: $hd")
          }
        case _ => Seq.empty
      }
    }

    sealed trait EmptyOrAssignedWrapper {
      def updateId: String

      def reassignment: Reassignment

      def synchronizerId: String

      final def assignedWrapper: AssignedWrapper = this match {
        case w: AssignedWrapper => w
        case _: EmptyReassignmentWrapper =>
          throw new IllegalStateException("Reassignment was empty")
      }
    }
    object EmptyOrAssignedWrapper {
      def apply(
          reassignment: Reassignment,
          synchronizerId: SynchronizerId,
      ): EmptyOrAssignedWrapper =
        if (reassignment.events.isEmpty)
          EmptyReassignmentWrapper(reassignment, synchronizerId.toProtoPrimitive)
        else
          ReassignmentWrapper(reassignment) match {
            case a: AssignedWrapper => a
            case invalid =>
              throw new IllegalStateException(s"Invalid reassignment wrapper: $invalid")
          }
    }

    sealed trait EmptyOrUnassignedWrapper {
      def updateId: String

      def reassignment: Reassignment

      def synchronizerId: String

      final def unassignedWrapper: UnassignedWrapper = this match {
        case w: UnassignedWrapper => w
        case _: EmptyReassignmentWrapper =>
          throw new IllegalStateException("Reassignment was empty")
      }
    }
    object EmptyOrUnassignedWrapper {
      def apply(
          reassignment: Reassignment,
          synchronizerId: SynchronizerId,
      ): EmptyOrUnassignedWrapper =
        if (reassignment.events.isEmpty)
          EmptyReassignmentWrapper(reassignment, synchronizerId.toProtoPrimitive)
        else
          ReassignmentWrapper(reassignment) match {
            case u: UnassignedWrapper => u
            case invalid =>
              throw new IllegalStateException(s"Invalid reassignment wrapper: $invalid")
          }
    }

    final case class AssignedWrapper(
        reassignment: Reassignment,
        events: Seq[AssignedEvent],
    ) extends ReassignmentWrapper
        with EmptyOrAssignedWrapper {
      private val head =
        events.headOption.getOrElse(throw new IllegalStateException("empty events"))
      override def synchronizerId = target
      def source: String = head.source
      def target: String = head.target
      def reassignmentId: String = head.reassignmentId
    }

    final case class UnassignedWrapper(
        reassignment: Reassignment,
        events: Seq[UnassignedEvent],
    ) extends ReassignmentWrapper
        with EmptyOrUnassignedWrapper {
      private val head =
        events.headOption.getOrElse(throw new IllegalStateException("empty events"))
      override def synchronizerId = source
      def source: String = head.source
      def target: String = head.target
      def reassignmentId: String = head.reassignmentId
    }

    final case class EmptyReassignmentWrapper(
        reassignment: Reassignment,
        override val synchronizerId: String,
    ) extends EmptyOrAssignedWrapper
        with EmptyOrUnassignedWrapper {
      override def updateId: String = reassignment.updateId
    }

    trait BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = UpdateServiceStub

      override def createService(channel: ManagedChannel): UpdateServiceStub =
        UpdateServiceGrpc.stub(channel)
    }

    final case class SubscribeUpdates(
        override val observer: StreamObserver[UpdateWrapper],
        beginExclusive: Long,
        endInclusive: Option[Long],
        updateFormat: UpdateFormat,
    )(override implicit val loggingContext: ErrorLoggingContext)
        extends BaseCommand[GetUpdatesRequest, AutoCloseable, AutoCloseable]
        with SubscribeBase[GetUpdatesRequest, GetUpdatesResponse, UpdateWrapper] {
      override def doRequest(
          service: UpdateServiceStub,
          request: GetUpdatesRequest,
          rawObserver: StreamObserver[GetUpdatesResponse],
      ): Unit =
        service.getUpdates(request, rawObserver)

      override def extractResults(response: GetUpdatesResponse): IterableOnce[UpdateWrapper] =
        response.update.transaction
          .map[UpdateWrapper](TransactionWrapper.apply)
          .orElse(response.update.reassignment.map(ReassignmentWrapper(_)))
          .orElse(response.update.topologyTransaction.map(TopologyTransactionWrapper(_)))

      override def createRequest(): Either[String, GetUpdatesRequest] = Right {
        GetUpdatesRequest(
          beginExclusive = beginExclusive,
          endInclusive = endInclusive,
          updateFormat = Some(updateFormat),
          filter = None,
          verbose = false,
        )
      }

    }

    final case class GetUpdateById(id: String, updateFormat: UpdateFormat)(implicit
        ec: ExecutionContext
    ) extends BaseCommand[GetUpdateByIdRequest, Option[GetUpdateResponse], Option[UpdateWrapper]]
        with PrettyPrinting {
      override protected def createRequest(): Either[String, GetUpdateByIdRequest] = Right {
        GetUpdateByIdRequest(
          updateId = id,
          updateFormat = Some(updateFormat),
        )
      }

      override protected def submitRequest(
          service: UpdateServiceStub,
          request: GetUpdateByIdRequest,
      ): Future[Option[GetUpdateResponse]] =
        // The Ledger API will throw an error if it can't find an update by ID.
        // However, as Canton is distributed, an update ID might show up later, so we don't treat this as
        // an error and change it to a None
        service.getUpdateById(request).map(Some(_)).recover {
          case e: StatusRuntimeException if e.getStatus.getCode == Status.Code.NOT_FOUND =>
            None
        }

      override protected def handleResponse(
          response: Option[GetUpdateResponse]
      ): Either[String, Option[UpdateWrapper]] =
        Right(extractUpdate(response))

      override protected def pretty: Pretty[GetUpdateById] =
        prettyOfClass(
          param("id", _.id.unquoted),
          param("updateFormat", _.updateFormat.toString.unquoted),
        )
    }

    final case class GetUpdateByOffset(offset: Long, updateFormat: UpdateFormat)(implicit
        ec: ExecutionContext
    ) extends BaseCommand[GetUpdateByOffsetRequest, Option[GetUpdateResponse], Option[
          UpdateWrapper
        ]]
        with PrettyPrinting {
      override protected def createRequest(): Either[String, GetUpdateByOffsetRequest] = Right {
        GetUpdateByOffsetRequest(
          offset = offset,
          updateFormat = Some(updateFormat),
        )
      }

      override protected def submitRequest(
          service: UpdateServiceStub,
          request: GetUpdateByOffsetRequest,
      ): Future[Option[GetUpdateResponse]] =
        // The Ledger API will throw an error if it can't find an update by ID.
        // However, as Canton is distributed, an update ID might show up later, so we don't treat this as
        // an error and change it to a None
        service.getUpdateByOffset(request).map(Some(_)).recover {
          case e: StatusRuntimeException if e.getStatus.getCode == Status.Code.NOT_FOUND =>
            None
        }

      override protected def handleResponse(
          response: Option[GetUpdateResponse]
      ): Either[String, Option[UpdateWrapper]] =
        Right(extractUpdate(response))

      override protected def pretty: Pretty[GetUpdateByOffset] =
        prettyOfClass(
          param("offset", _.offset),
          param("updateFormat", _.updateFormat.toString.unquoted),
        )
    }

    private def extractUpdate(response: Option[GetUpdateResponse]): Option[UpdateWrapper] = {
      val updateO = response.map(_.update)
      updateO
        .flatMap(_.transaction)
        .map[UpdateWrapper](TransactionWrapper.apply)
        .orElse(updateO.flatMap(_.reassignment).map(ReassignmentWrapper(_)))
        .orElse(updateO.flatMap(_.topologyTransaction).map(TopologyTransactionWrapper(_)))
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
    def synchronizerId: Option[SynchronizerId]
    def userId: String
    def packageIdSelectionPreference: Seq[LfPackageId]

    protected def mkCommand: Commands = Commands(
      workflowId = workflowId,
      userId = userId,
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
            offset.fold(0L)(_.unwrap)
          )
      },
      minLedgerTimeRel = None,
      minLedgerTimeAbs = minLedgerTimeAbs.map(ProtoConverter.InstantConverter.toProtoPrimitive),
      submissionId = submissionId,
      disclosedContracts = disclosedContracts,
      synchronizerId = synchronizerId.map(_.toProtoPrimitive).getOrElse(""),
      packageIdSelectionPreference = packageIdSelectionPreference.map(_.toString),
      prefetchContractKeys = Nil,
    )

    override protected def pretty: Pretty[this.type] =
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
        override val synchronizerId: Option[SynchronizerId],
        override val userId: String,
        override val packageIdSelectionPreference: Seq[LfPackageId],
    ) extends SubmitCommand
        with BaseCommand[SubmitRequest, SubmitResponse, Unit] {
      override protected def createRequest(): Either[String, SubmitRequest] =
        try {
          Right(SubmitRequest(commands = Some(mkCommand)))
        } catch {
          case t: Throwable =>
            Left(t.getMessage)
        }

      override protected def submitRequest(
          service: CommandSubmissionServiceStub,
          request: SubmitRequest,
      ): Future[SubmitResponse] =
        service.submit(request)

      override protected def handleResponse(response: SubmitResponse): Either[String, Unit] =
        Either.unit
    }

    final case class SubmitAssignCommand(
        workflowId: String,
        userId: String,
        commandId: String,
        submitter: LfPartyId,
        submissionId: String,
        reassignmentId: String,
        source: SynchronizerId,
        target: SynchronizerId,
    ) extends BaseCommand[SubmitReassignmentRequest, SubmitReassignmentResponse, Unit] {
      override protected def createRequest(): Either[String, SubmitReassignmentRequest] = Right(
        SubmitReassignmentRequest(
          Some(
            ReassignmentCommands(
              workflowId = workflowId,
              userId = userId,
              commandId = commandId,
              submitter = submitter.toString,
              commands = Seq(
                ReassignmentCommand(
                  ReassignmentCommand.Command.AssignCommand(
                    AssignCommand(
                      reassignmentId = reassignmentId,
                      source = source.toProtoPrimitive,
                      target = target.toProtoPrimitive,
                    )
                  )
                )
              ),
              submissionId = submissionId,
            )
          )
        )
      )

      override protected def submitRequest(
          service: CommandSubmissionServiceStub,
          request: SubmitReassignmentRequest,
      ): Future[SubmitReassignmentResponse] =
        service.submitReassignment(request)

      override protected def handleResponse(
          response: SubmitReassignmentResponse
      ): Either[String, Unit] =
        Either.unit
    }

    final case class SubmitUnassignCommand(
        workflowId: String,
        userId: String,
        commandId: String,
        submitter: LfPartyId,
        submissionId: String,
        contractIds: Seq[LfContractId],
        source: SynchronizerId,
        target: SynchronizerId,
    ) extends BaseCommand[SubmitReassignmentRequest, SubmitReassignmentResponse, Unit] {
      override protected def createRequest(): Either[String, SubmitReassignmentRequest] = Right(
        SubmitReassignmentRequest(
          Some(
            ReassignmentCommands(
              workflowId = workflowId,
              userId = userId,
              commandId = commandId,
              submitter = submitter.toString,
              commands = contractIds.map(contractId =>
                ReassignmentCommand(
                  ReassignmentCommand.Command.UnassignCommand(
                    UnassignCommand(
                      contractId = contractId.coid.toString,
                      source = source.toProtoPrimitive,
                      target = target.toProtoPrimitive,
                    )
                  )
                )
              ),
              submissionId = submissionId,
            )
          )
        )
      )

      override protected def submitRequest(
          service: CommandSubmissionServiceStub,
          request: SubmitReassignmentRequest,
      ): Future[SubmitReassignmentResponse] =
        service.submitReassignment(request)

      override protected def handleResponse(
          response: SubmitReassignmentResponse
      ): Either[String, Unit] =
        Either.unit
    }
  }

  object InteractiveSubmissionService {
    trait BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = InteractiveSubmissionServiceStub
      override def createService(channel: ManagedChannel): InteractiveSubmissionServiceStub =
        InteractiveSubmissionServiceGrpc.stub(channel)
    }

    final case class PrepareCommand(
        actAs: Seq[LfPartyId],
        readAs: Seq[LfPartyId],
        commands: Seq[Command],
        commandId: String,
        minLedgerTimeAbs: Option[Instant],
        disclosedContracts: Seq[DisclosedContract],
        synchronizerId: Option[SynchronizerId],
        userId: String,
        packageIdSelectionPreference: Seq[LfPackageId],
        verboseHashing: Boolean,
        prefetchContractKeys: Seq[PrefetchContractKey],
    ) extends BaseCommand[
          PrepareSubmissionRequest,
          PrepareSubmissionResponse,
          PrepareSubmissionResponse,
        ] {

      override protected def createRequest(): Either[String, PrepareSubmissionRequest] =
        Right(
          PrepareSubmissionRequest(
            userId = userId,
            commandId = commandId,
            commands = commands,
            minLedgerTime = minLedgerTimeAbs
              .map(ProtoConverter.InstantConverter.toProtoPrimitive)
              .map(MinLedgerTime.Time.MinLedgerTimeAbs.apply)
              .map(MinLedgerTime(_)),
            actAs = actAs,
            readAs = readAs,
            disclosedContracts = disclosedContracts,
            synchronizerId = synchronizerId.map(_.toProtoPrimitive).getOrElse(""),
            packageIdSelectionPreference = packageIdSelectionPreference,
            verboseHashing = verboseHashing,
            prefetchContractKeys = prefetchContractKeys,
          )
        )

      override protected def submitRequest(
          service: InteractiveSubmissionServiceStub,
          request: PrepareSubmissionRequest,
      ): Future[PrepareSubmissionResponse] =
        service.prepareSubmission(request)

      override protected def handleResponse(
          response: PrepareSubmissionResponse
      ): Either[String, PrepareSubmissionResponse] =
        Right(response)

      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class ExecuteCommand(
        preparedTransaction: PreparedTransaction,
        transactionSignatures: Map[PartyId, Seq[Signature]],
        submissionId: String,
        userId: String,
        minLedgerTimeAbs: Option[Instant],
        deduplicationPeriod: Option[DeduplicationPeriod],
        hashingSchemeVersion: HashingSchemeVersion,
    ) extends BaseCommand[
          ExecuteSubmissionRequest,
          ExecuteSubmissionResponse,
          ExecuteSubmissionResponse,
        ] {

      import com.digitalasset.canton.crypto.LedgerApiCryptoConversions.*
      import io.scalaland.chimney.dsl.*
      import com.daml.ledger.api.v2.interactive.interactive_submission_service as iss

      private def makePartySignatures: PartySignatures = PartySignatures(
        transactionSignatures.map { case (party, signatures) =>
          SinglePartySignatures(
            party = party.toProtoPrimitive,
            signatures = signatures.map(_.toProtoV30.transformInto[iss.Signature]),
          )
        }.toSeq
      )

      private[commands] def serializeDeduplicationPeriod(
          deduplicationPeriod: Option[DeduplicationPeriod]
      ) = deduplicationPeriod.fold(
        ExecuteSubmissionRequest.DeduplicationPeriod.Empty: ExecuteSubmissionRequest.DeduplicationPeriod
      ) {
        case DeduplicationPeriod.DeduplicationDuration(duration) =>
          ExecuteSubmissionRequest.DeduplicationPeriod.DeduplicationDuration(
            ProtoConverter.DurationConverter.toProtoPrimitive(duration)
          )
        case DeduplicationPeriod.DeduplicationOffset(offset) =>
          ExecuteSubmissionRequest.DeduplicationPeriod.DeduplicationOffset(
            offset.fold(0L)(_.unwrap)
          )
      }

      override protected def createRequest(): Either[String, ExecuteSubmissionRequest] =
        Right(
          ExecuteSubmissionRequest(
            preparedTransaction = Some(preparedTransaction),
            partySignatures = Some(makePartySignatures),
            submissionId = submissionId,
            userId = userId,
            deduplicationPeriod = serializeDeduplicationPeriod(deduplicationPeriod),
            hashingSchemeVersion = hashingSchemeVersion,
            minLedgerTime = None,
          )
        )

      override protected def submitRequest(
          service: InteractiveSubmissionServiceStub,
          request: ExecuteSubmissionRequest,
      ): Future[ExecuteSubmissionResponse] =
        service.executeSubmission(request)

      override protected def handleResponse(
          response: ExecuteSubmissionResponse
      ): Either[String, ExecuteSubmissionResponse] =
        Right(response)

      override def timeoutType: TimeoutType = DefaultUnboundedTimeout
    }

    final case class ExecuteAndWaitCommand(
        preparedTransaction: PreparedTransaction,
        transactionSignatures: Map[PartyId, Seq[Signature]],
        submissionId: String,
        userId: String,
        minLedgerTimeAbs: Option[Instant],
        deduplicationPeriod: Option[DeduplicationPeriod],
        hashingSchemeVersion: HashingSchemeVersion,
    ) extends BaseCommand[
          ExecuteSubmissionAndWaitRequest,
          ExecuteSubmissionAndWaitResponse,
          ExecuteSubmissionAndWaitResponse,
        ] {

      override protected def createRequest(): Either[String, ExecuteSubmissionAndWaitRequest] =
        ExecuteCommand(
          preparedTransaction = preparedTransaction,
          transactionSignatures = transactionSignatures,
          submissionId = submissionId,
          userId = userId,
          deduplicationPeriod = deduplicationPeriod,
          hashingSchemeVersion = hashingSchemeVersion,
          minLedgerTimeAbs = minLedgerTimeAbs,
        ).createRequestInternal()
          .map(_.transformInto[ExecuteSubmissionAndWaitRequest])

      override protected def submitRequest(
          service: InteractiveSubmissionServiceStub,
          request: ExecuteSubmissionAndWaitRequest,
      ): Future[ExecuteSubmissionAndWaitResponse] =
        service.executeSubmissionAndWait(request)

      override protected def handleResponse(
          response: ExecuteSubmissionAndWaitResponse
      ): Either[String, ExecuteSubmissionAndWaitResponse] =
        Right(response)

      override def timeoutType: TimeoutType = DefaultUnboundedTimeout
    }

    final case class ExecuteAndWaitForTransactionCommand(
        preparedTransaction: PreparedTransaction,
        transactionSignatures: Map[PartyId, Seq[Signature]],
        submissionId: String,
        userId: String,
        minLedgerTimeAbs: Option[Instant],
        deduplicationPeriod: Option[DeduplicationPeriod],
        hashingSchemeVersion: HashingSchemeVersion,
        transactionFormat: Option[TransactionFormat],
    ) extends BaseCommand[
          ExecuteSubmissionAndWaitForTransactionRequest,
          ExecuteSubmissionAndWaitForTransactionResponse,
          ExecuteSubmissionAndWaitForTransactionResponse,
        ] {

      override protected def createRequest()
          : Either[String, ExecuteSubmissionAndWaitForTransactionRequest] =
        ExecuteCommand(
          preparedTransaction = preparedTransaction,
          transactionSignatures = transactionSignatures,
          submissionId = submissionId,
          userId = userId,
          deduplicationPeriod = deduplicationPeriod,
          hashingSchemeVersion = hashingSchemeVersion,
          minLedgerTimeAbs = minLedgerTimeAbs,
        ).createRequestInternal()
          .map(
            _.into[ExecuteSubmissionAndWaitForTransactionRequest]
              .withFieldConst(_.transactionFormat, transactionFormat)
              .transform
          )

      override protected def submitRequest(
          service: InteractiveSubmissionServiceStub,
          request: ExecuteSubmissionAndWaitForTransactionRequest,
      ): Future[ExecuteSubmissionAndWaitForTransactionResponse] =
        service.executeSubmissionAndWaitForTransaction(request)

      override protected def handleResponse(
          response: ExecuteSubmissionAndWaitForTransactionResponse
      ): Either[String, ExecuteSubmissionAndWaitForTransactionResponse] =
        Right(response)

      override def timeoutType: TimeoutType = DefaultUnboundedTimeout
    }

    final case class PreferredPackageVersion(
        parties: Set[LfPartyId],
        packageName: LfPackageName,
        synchronizerIdO: Option[SynchronizerId],
        vettingValidAt: Option[CantonTimestamp],
    ) extends BaseCommand[
          GetPreferredPackageVersionRequest,
          GetPreferredPackageVersionResponse,
          Option[PackagePreference],
        ] {

      override protected def submitRequest(
          service: InteractiveSubmissionServiceStub,
          request: GetPreferredPackageVersionRequest,
      ): Future[GetPreferredPackageVersionResponse] =
        service.getPreferredPackageVersion(request)

      override protected def createRequest(): Either[String, GetPreferredPackageVersionRequest] =
        Right(
          GetPreferredPackageVersionRequest(
            parties = parties.toSeq,
            packageName = packageName,
            synchronizerId = synchronizerIdO.map(_.toProtoPrimitive).getOrElse(""),
            vettingValidAt = vettingValidAt.map(_.toProtoTimestamp),
          )
        )

      override protected def handleResponse(
          response: GetPreferredPackageVersionResponse
      ): Either[String, Option[PackagePreference]] = Right(response.packagePreference)
    }

    final case class PreferredPackages(
        packageVettingRequirements: Map[LfPackageName, Set[LfPartyId]],
        synchronizerIdO: Option[SynchronizerId],
        vettingValidAt: Option[CantonTimestamp],
    ) extends BaseCommand[
          GetPreferredPackagesRequest,
          GetPreferredPackagesResponse,
          GetPreferredPackagesResponse,
        ] {

      override protected def submitRequest(
          service: InteractiveSubmissionServiceStub,
          request: GetPreferredPackagesRequest,
      ): Future[GetPreferredPackagesResponse] =
        service.getPreferredPackages(request)

      override protected def createRequest(): Either[String, GetPreferredPackagesRequest] =
        Right(
          GetPreferredPackagesRequest(
            packageVettingRequirements =
              packageVettingRequirements.view.map { case (packageName, parties) =>
                PackageVettingRequirement(packageName = packageName, parties = parties.toSeq)
              }.toSeq,
            synchronizerId = synchronizerIdO.map(_.toProtoPrimitive).getOrElse(""),
            vettingValidAt = vettingValidAt.map(_.toProtoTimestamp),
          )
        )

      override protected def handleResponse(
          response: GetPreferredPackagesResponse
      ): Either[String, GetPreferredPackagesResponse] = Right(response)
    }
  }

  object CommandService {
    trait BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = CommandServiceStub
      override def createService(channel: ManagedChannel): CommandServiceStub =
        CommandServiceGrpc.stub(channel)
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
        override val synchronizerId: Option[SynchronizerId],
        override val userId: String,
        override val packageIdSelectionPreference: Seq[LfPackageId],
        transactionShape: TransactionShape,
        includeCreatedEventBlob: Boolean,
    ) extends SubmitCommand
        with BaseCommand[
          SubmitAndWaitForTransactionRequest,
          SubmitAndWaitForTransactionResponse,
          Transaction,
        ] {

      override protected def createRequest(): Either[String, SubmitAndWaitForTransactionRequest] =
        try {
          Right(
            SubmitAndWaitForTransactionRequest(
              commands = Some(mkCommand),
              transactionFormat = Some(
                TransactionFormat(
                  eventFormat = Some(
                    EventFormat(
                      filtersByParty = actAs
                        .map(
                          _ -> Filters(
                            Seq(
                              CumulativeFilter(
                                IdentifierFilter.WildcardFilter(
                                  WildcardFilter(includeCreatedEventBlob = includeCreatedEventBlob)
                                )
                              )
                            )
                          )
                        )
                        .toMap,
                      filtersForAnyParty = None,
                      verbose = true,
                    )
                  ),
                  transactionShape = transactionShape,
                )
              ),
            )
          )
        } catch {
          case t: Throwable =>
            Left(t.getMessage)
        }

      override protected def submitRequest(
          service: CommandServiceStub,
          request: SubmitAndWaitForTransactionRequest,
      ): Future[SubmitAndWaitForTransactionResponse] =
        service.submitAndWaitForTransaction(request)

      override protected def handleResponse(
          response: SubmitAndWaitForTransactionResponse
      ): Either[String, Transaction] =
        response.transaction.toRight("Received response without any transaction")

      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class SubmitAndWaitAssign(
        workflowId: String,
        userId: String,
        commandId: String,
        submitter: LfPartyId,
        submissionId: String,
        reassignmentId: String,
        source: SynchronizerId,
        target: SynchronizerId,
        eventFormat: Option[EventFormat],
    ) extends BaseCommand[
          SubmitAndWaitForReassignmentRequest,
          SubmitAndWaitForReassignmentResponse,
          EmptyOrAssignedWrapper,
        ] {
      override protected def createRequest(): Either[String, SubmitAndWaitForReassignmentRequest] =
        Right(
          SubmitAndWaitForReassignmentRequest(
            reassignmentCommands = Some(
              ReassignmentCommands(
                workflowId = workflowId,
                userId = userId,
                commandId = commandId,
                submitter = submitter.toString,
                commands = Seq(
                  ReassignmentCommand(
                    ReassignmentCommand.Command.AssignCommand(
                      AssignCommand(
                        reassignmentId = reassignmentId,
                        source = source.toProtoPrimitive,
                        target = target.toProtoPrimitive,
                      )
                    )
                  )
                ),
                submissionId = submissionId,
              )
            ),
            eventFormat = eventFormat,
          )
        )

      override protected def submitRequest(
          service: CommandServiceStub,
          request: SubmitAndWaitForReassignmentRequest,
      ): Future[SubmitAndWaitForReassignmentResponse] =
        service.submitAndWaitForReassignment(request)

      override protected def handleResponse(
          response: SubmitAndWaitForReassignmentResponse
      ): Either[String, EmptyOrAssignedWrapper] =
        response.reassignment
          .toRight("Received response without any reassignment")
          .map(EmptyOrAssignedWrapper(_, target))
    }

    final case class SubmitAndWaitUnassign(
        workflowId: String,
        userId: String,
        commandId: String,
        submitter: LfPartyId,
        submissionId: String,
        contractIds: Seq[LfContractId],
        source: SynchronizerId,
        target: SynchronizerId,
        eventFormat: Option[EventFormat],
    ) extends BaseCommand[
          SubmitAndWaitForReassignmentRequest,
          SubmitAndWaitForReassignmentResponse,
          EmptyOrUnassignedWrapper,
        ] {
      override protected def createRequest(): Either[String, SubmitAndWaitForReassignmentRequest] =
        Right(
          SubmitAndWaitForReassignmentRequest(
            Some(
              ReassignmentCommands(
                workflowId = workflowId,
                userId = userId,
                commandId = commandId,
                submitter = submitter.toString,
                commands = contractIds.map(contractId =>
                  ReassignmentCommand(
                    ReassignmentCommand.Command.UnassignCommand(
                      UnassignCommand(
                        contractId = contractId.coid.toString,
                        source = source.toProtoPrimitive,
                        target = target.toProtoPrimitive,
                      )
                    )
                  )
                ),
                submissionId = submissionId,
              )
            ),
            eventFormat = eventFormat,
          )
        )

      override protected def submitRequest(
          service: CommandServiceStub,
          request: SubmitAndWaitForReassignmentRequest,
      ): Future[SubmitAndWaitForReassignmentResponse] =
        service.submitAndWaitForReassignment(request)

      override protected def handleResponse(
          response: SubmitAndWaitForReassignmentResponse
      ): Either[String, EmptyOrUnassignedWrapper] =
        response.reassignment
          .toRight("Received response without any reassignment")
          .map(EmptyOrUnassignedWrapper(_, source))
    }

  }

  object StateService {
    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = StateServiceStub

      override def createService(channel: ManagedChannel): StateServiceStub =
        StateServiceGrpc.stub(channel)
    }

    final case class LedgerEnd()
        extends BaseCommand[GetLedgerEndRequest, GetLedgerEndResponse, Long] {

      override protected def createRequest(): Either[String, GetLedgerEndRequest] =
        Right(GetLedgerEndRequest())

      override protected def submitRequest(
          service: StateServiceStub,
          request: GetLedgerEndRequest,
      ): Future[GetLedgerEndResponse] =
        service.getLedgerEnd(request)

      override protected def handleResponse(
          response: GetLedgerEndResponse
      ): Either[String, Long] =
        Right(response.offset)
    }

    final case class GetConnectedSynchronizers(partyId: LfPartyId)
        extends BaseCommand[
          GetConnectedSynchronizersRequest,
          GetConnectedSynchronizersResponse,
          GetConnectedSynchronizersResponse,
        ] {

      override protected def createRequest(): Either[String, GetConnectedSynchronizersRequest] =
        Right(
          GetConnectedSynchronizersRequest(
            partyId.toString,
            participantId = "",
            identityProviderId = "",
          )
        )

      override protected def submitRequest(
          service: StateServiceStub,
          request: GetConnectedSynchronizersRequest,
      ): Future[GetConnectedSynchronizersResponse] =
        service.getConnectedSynchronizers(request)

      override protected def handleResponse(
          response: GetConnectedSynchronizersResponse
      ): Either[String, GetConnectedSynchronizersResponse] =
        Right(response)
    }

    final case class GetActiveContracts(
        observer: StreamObserver[GetActiveContractsResponse],
        parties: Set[LfPartyId],
        limit: PositiveInt,
        templateFilter: Seq[TemplateId] = Seq.empty,
        activeAtOffset: Long,
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

      override protected def createRequest(): Either[String, GetActiveContractsRequest] = {
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
            filter = None,
            verbose = false,
            activeAtOffset = activeAtOffset,
            eventFormat = Some(EventFormat(parties.map((_, filter)).toMap, None, verbose)),
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

  object CommandCompletionService {
    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = CommandCompletionServiceStub

      override def createService(channel: ManagedChannel): CommandCompletionServiceStub =
        CommandCompletionServiceGrpc.stub(channel)
    }

    final case class CompletionRequest(
        partyId: LfPartyId,
        beginOffsetExclusive: Long,
        expectedCompletions: Int,
        timeout: java.time.Duration,
        userId: String,
    )(filter: Completion => Boolean, scheduler: ScheduledExecutorService)
        extends BaseCommand[
          CompletionStreamRequest,
          Seq[Completion],
          Seq[Completion],
        ] {

      override protected def createRequest(): Either[String, CompletionStreamRequest] =
        Right(
          CompletionStreamRequest(
            userId = userId,
            parties = Seq(partyId),
            beginExclusive = beginOffsetExclusive,
          )
        )

      override protected def submitRequest(
          service: CommandCompletionServiceStub,
          request: CompletionStreamRequest,
      ): Future[Seq[Completion]] = {
        import scala.jdk.DurationConverters.*
        GrpcAdminCommand
          .streamedResponse[CompletionStreamRequest, CompletionStreamResponse, Completion](
            service.completionStream,
            response => response.completionResponse.completion.toList.filter(filter),
            request,
            expectedCompletions,
            timeout.toScala,
            scheduler,
          )
      }

      override protected def handleResponse(
          response: Seq[Completion]
      ): Either[String, Seq[Completion]] =
        Right(response)

      override def timeoutType: TimeoutType = ServerEnforcedTimeout
    }

    final case class Subscribe(
        observer: StreamObserver[Completion],
        parties: Seq[String],
        offset: Long,
        userId: String,
    )(implicit loggingContext: ErrorLoggingContext)
        extends BaseCommand[CompletionStreamRequest, AutoCloseable, AutoCloseable] {
      // The subscription should never be cut short because of a gRPC timeout
      override def timeoutType: TimeoutType = ServerEnforcedTimeout

      override protected def createRequest(): Either[String, CompletionStreamRequest] = Right {
        CompletionStreamRequest(
          userId = userId,
          parties = parties,
          beginExclusive = offset,
        )
      }

      override protected def submitRequest(
          service: CommandCompletionServiceStub,
          request: CompletionStreamRequest,
      ): Future[AutoCloseable] = {
        val rawObserver = new ForwardingStreamObserver[CompletionStreamResponse, Completion](
          observer,
          response => response.completionResponse.completion.toList,
        )
        val context = Context.current().withCancellation()
        context.run(() => service.completionStream(request, rawObserver))
        Future.successful(context)
      }

      override protected def handleResponse(
          response: AutoCloseable
      ): Either[String, AutoCloseable] = Right(
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

      override protected def submitRequest(
          service: TimeServiceStub,
          request: GetTimeRequest,
      ): Future[GetTimeResponse] =
        service.getTime(request)

      /** Create the request from configured options
        */
      override protected def createRequest(): Either[String, GetTimeRequest] = Right(
        GetTimeRequest()
      )

      /** Handle the response the service has provided
        */
      override protected def handleResponse(
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

      override protected def submitRequest(
          service: TimeServiceStub,
          request: SetTimeRequest,
      ): Future[Empty] =
        service.setTime(request)

      override protected def createRequest(): Either[String, SetTimeRequest] =
        Right(
          SetTimeRequest(
            currentTime = Some(currentTime.toProtoTimestamp),
            newTime = Some(newTime.toProtoTimestamp),
          )
        )

      /** Handle the response the service has provided
        */
      override protected def handleResponse(response: Empty): Either[String, Unit] = Either.unit

    }

  }

  object QueryService {

    abstract class BaseCommand[Req, Res] extends GrpcAdminCommand[Req, Res, Res] {
      override type Svc = EventQueryServiceStub

      override def createService(channel: ManagedChannel): EventQueryServiceStub =
        EventQueryServiceGrpc.stub(channel)

      override protected def handleResponse(response: Res): Either[String, Res] = Right(response)
    }

    final case class GetEventsByContractId(
        contractId: String,
        requestingParties: Seq[String],
    ) extends BaseCommand[
          GetEventsByContractIdRequest,
          GetEventsByContractIdResponse,
        ] {

      override protected def createRequest(): Either[String, GetEventsByContractIdRequest] = Right(
        GetEventsByContractIdRequest(
          contractId = contractId,
          requestingParties = Seq.empty,
          eventFormat = Some(
            EventFormat(
              filtersByParty = requestingParties.map(_ -> Filters(Nil)).toMap,
              filtersForAnyParty = None,
              verbose = true,
            )
          ),
        )
      )

      override protected def submitRequest(
          service: EventQueryServiceStub,
          request: GetEventsByContractIdRequest,
      ): Future[GetEventsByContractIdResponse] = service.getEventsByContractId(request)

    }
  }
}
