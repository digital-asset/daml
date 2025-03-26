// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.admin.user_management_service.*
import com.digitalasset.canton.auth.{Authorizer, RequiredClaim}
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.RequiredClaims
import com.digitalasset.canton.ledger.api.auth.services.UserManagementServiceAuthorization.userReaderClaims
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import io.grpc.ServerServiceDefinition
import scalapb.lenses.Lens

import scala.concurrent.{ExecutionContext, Future}

final class UserManagementServiceAuthorization(
    protected val service: UserManagementServiceGrpc.UserManagementService with AutoCloseable,
    private val authorizer: Authorizer,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends UserManagementServiceGrpc.UserManagementService
    with ProxyCloseable
    with GrpcApiService
    with NamedLogging {

  // Only ParticipantAdmin is allowed to grant ParticipantAdmin right
  private def containsParticipantAdmin(rights: Seq[Right]): Boolean =
    rights.contains(Right(Right.Kind.ParticipantAdmin(Right.ParticipantAdmin())))

  override def createUser(request: CreateUserRequest): Future[CreateUserResponse] =
    authorizer.rpc(service.createUser)(
      RequiredClaims.idpAdminClaimsAndMatchingRequestIdpId(
        identityProviderIdL = Lens.unit[CreateUserRequest].user.identityProviderId,
        mustBeParticipantAdmin = containsParticipantAdmin(request.rights),
      )*
    )(request)

  override def getUser(request: GetUserRequest): Future[GetUserResponse] =
    authorizer.rpc(service.getUser)(
      userReaderClaims(
        userIdL = Lens.unit[GetUserRequest].userId,
        identityProviderIdL = Lens.unit[GetUserRequest].identityProviderId,
      )*
    )(request)

  override def deleteUser(request: DeleteUserRequest): Future[DeleteUserResponse] =
    authorizer.rpc(service.deleteUser)(
      RequiredClaims.idpAdminClaimsAndMatchingRequestIdpId(
        Lens.unit[DeleteUserRequest].identityProviderId
      )*
    )(request)

  override def listUsers(request: ListUsersRequest): Future[ListUsersResponse] =
    authorizer.rpc(service.listUsers)(
      RequiredClaims.idpAdminClaimsAndMatchingRequestIdpId(
        Lens.unit[ListUsersRequest].identityProviderId
      )*
    )(request)

  override def grantUserRights(request: GrantUserRightsRequest): Future[GrantUserRightsResponse] =
    authorizer.rpc(service.grantUserRights)(
      RequiredClaims.idpAdminClaimsAndMatchingRequestIdpId(
        identityProviderIdL = Lens.unit[GrantUserRightsRequest].identityProviderId,
        mustBeParticipantAdmin = containsParticipantAdmin(request.rights),
      )*
    )(request)

  override def revokeUserRights(
      request: RevokeUserRightsRequest
  ): Future[RevokeUserRightsResponse] =
    authorizer.rpc(service.revokeUserRights)(
      RequiredClaims.idpAdminClaimsAndMatchingRequestIdpId(
        identityProviderIdL = Lens.unit[RevokeUserRightsRequest].identityProviderId,
        mustBeParticipantAdmin = containsParticipantAdmin(request.rights),
      )*
    )(request)

  override def listUserRights(request: ListUserRightsRequest): Future[ListUserRightsResponse] =
    authorizer.rpc(service.listUserRights)(
      userReaderClaims(
        userIdL = Lens.unit[ListUserRightsRequest].userId,
        identityProviderIdL = Lens.unit[ListUserRightsRequest].identityProviderId,
      )*
    )(request)

  override def updateUser(request: UpdateUserRequest): Future[UpdateUserResponse] =
    request.user match {
      case Some(_) =>
        authorizer.rpc(service.updateUser)(
          RequiredClaims.idpAdminClaimsAndMatchingRequestIdpId(
            Lens.unit[UpdateUserRequest].user.identityProviderId
          )*
        )(request)
      case None =>
        authorizer.rpc(service.updateUser)(RequiredClaim.AdminOrIdpAdmin())(request)
    }

  override def updateUserIdentityProviderId(
      request: UpdateUserIdentityProviderIdRequest
  ): Future[UpdateUserIdentityProviderIdResponse] =
    authorizer.rpc(service.updateUserIdentityProviderId)(RequiredClaim.Admin())(request)

  override def bindService(): ServerServiceDefinition =
    UserManagementServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()
}

object UserManagementServiceAuthorization {
  def userReaderClaims[Req](
      userIdL: Lens[Req, String],
      identityProviderIdL: Lens[Req, String],
  ): List[RequiredClaim[Req]] = List(
    RequiredClaim.MatchUserIdForUserManagement(userIdL),
    RequiredClaim.MatchIdentityProviderId(identityProviderIdL),
  )

}
