// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import com.daml.ledger.api.v1.admin.identity_provider_config_service.IdentityProviderConfigServiceGrpc.IdentityProviderConfigServiceStub
import com.daml.ledger.api.v1.admin.identity_provider_config_service.*
import com.daml.ledger.api.v1.admin.metering_report_service.MeteringReportServiceGrpc.MeteringReportServiceStub
import com.daml.ledger.api.v1.admin.metering_report_service.{
  GetMeteringReportRequest,
  GetMeteringReportResponse,
  MeteringReportServiceGrpc,
}
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementServiceStub
import com.daml.ledger.api.v1.admin.package_management_service.*
import com.daml.ledger.api.v1.admin.participant_pruning_service.ParticipantPruningServiceGrpc.ParticipantPruningServiceStub
import com.daml.ledger.api.v1.admin.participant_pruning_service.*
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementServiceStub
import com.daml.ledger.api.v1.admin.party_management_service.*
import com.daml.ledger.api.v1.admin.user_management_service.UserManagementServiceGrpc.UserManagementServiceStub
import com.daml.ledger.api.v1.admin.user_management_service.{
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
  UpdateUserIdentityProviderRequest,
  UpdateUserIdentityProviderResponse,
  UpdateUserRequest,
  UpdateUserResponse,
  User,
  UserManagementServiceGrpc,
}
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.api.v2.testing.time_service.TimeServiceGrpc.TimeServiceStub
import com.daml.ledger.api.v2.testing.time_service.{GetTimeRequest, SetTimeRequest, TimeServiceGrpc}
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.admin.api.client.data.{
  LedgerApiUser,
  LedgerMeteringReport,
  ListLedgerApiUsersResult,
  UserRights,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.{IdentityProviderId, JwksUrl}
import com.digitalasset.canton.ledger.client.services.admin.IdentityProviderConfigClient
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.BinaryFileUtil
import com.google.protobuf.empty.Empty
import com.google.protobuf.field_mask.FieldMask
import io.grpc.*

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
          UpdatePartyIdentityProviderRequest,
          UpdatePartyIdentityProviderResponse,
          Unit,
        ] {

      override def submitRequest(
          service: PartyManagementServiceStub,
          request: UpdatePartyIdentityProviderRequest,
      ): Future[UpdatePartyIdentityProviderResponse] =
        service.updatePartyIdentityProviderId(request)

      override def createRequest(): Either[String, UpdatePartyIdentityProviderRequest] = Right(
        UpdatePartyIdentityProviderRequest(
          party = party.toProtoPrimitive,
          sourceIdentityProviderId = sourceIdentityProviderId,
          targetIdentityProviderId = targetIdentityProviderId,
        )
      )

      override def handleResponse(
          response: UpdatePartyIdentityProviderResponse
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

      protected def getRights: Seq[UserRight] = {
        actAs.toSeq.map(x => UserRight().withCanActAs(UserRight.CanActAs(x))) ++
          readAs.toSeq.map(x => UserRight().withCanReadAs(UserRight.CanReadAs(x))) ++
          (if (participantAdmin) Seq(UserRight().withParticipantAdmin(UserRight.ParticipantAdmin()))
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
          UpdateUserIdentityProviderRequest,
          UpdateUserIdentityProviderResponse,
          Unit,
        ] {

      override def submitRequest(
          service: UserManagementServiceStub,
          request: UpdateUserIdentityProviderRequest,
      ): Future[UpdateUserIdentityProviderResponse] =
        service.updateUserIdentityProviderId(request)

      override def createRequest(): Either[String, UpdateUserIdentityProviderRequest] = Right(
        UpdateUserIdentityProviderRequest(
          userId = id,
          sourceIdentityProviderId = sourceIdentityProviderId,
          targetIdentityProviderId = targetIdentityProviderId,
        )
      )

      override def handleResponse(
          response: UpdateUserIdentityProviderResponse
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

  object Time {
    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = TimeServiceStub

      override def createService(channel: ManagedChannel): TimeServiceStub =
        TimeServiceGrpc.stub(channel)
    }

    final case class Get(timeout: FiniteDuration)(scheduler: ScheduledExecutorService)(implicit
        executionContext: ExecutionContext
    ) extends BaseCommand[
          GetTimeRequest,
          Either[String, CantonTimestamp],
          CantonTimestamp,
        ] {

      override def submitRequest(
          service: TimeServiceStub,
          request: GetTimeRequest,
      ): Future[Either[String, CantonTimestamp]] =
        service.getTime(request).map {
          _.currentTime
            .toRight("Empty timestamp received from ledger Api server")
            .flatMap(CantonTimestamp.fromProtoTimestamp(_).leftMap(_.message))
        }

      /** Create the request from configured options
        */
      override def createRequest(): Either[String, GetTimeRequest] = Right(GetTimeRequest())

      /** Handle the response the service has provided
        */
      override def handleResponse(
          response: Either[String, CantonTimestamp]
      ): Either[String, CantonTimestamp] = response
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
      override def handleResponse(response: Empty): Either[String, Unit] = Either.unit

    }

  }
}
