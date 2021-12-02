package com.daml.ledger.api.auth.services

import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.auth._
import com.daml.ledger.api.v1.admin.user_management_service._
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ProxyCloseable
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

private[daml] final class UserManagementServiceAuthorization(
         protected val service: UserManagementServiceGrpc.UserManagementService with AutoCloseable,
         private val authorizer: Authorizer,
       )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
  extends UserManagementServiceGrpc.UserManagementService
    with ProxyCloseable
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)

  private implicit val errorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  override def createUser(request: CreateUserRequest): Future[User] =
    authorizer.requireAdminClaims(service.createUser)(request)

  override def getUser(request: GetUserRequest): Future[User] = {
    if (request.userId.isEmpty) {
      // Request user-id is empty => serve the user from the authenticated claims
      authorizer.withClaims(claims =>
        claims.applicationId match {
          case None =>
            Future.failed(
              LedgerApiErrors.AuthorizationChecks.PermissionDenied.Reject("user-id not set in authenticated claims").asGrpcError)
          case Some(userId) =>
            if (claims.isStandardJwtToken)
              service.getUser(request.copy(userId = userId))
            else {
              // Custom JWT token: decode the user from the token
              // FIXME: make this more idiomatic
              val actAsParties = claims.claims.collect({
                case ClaimActAsParty(p) => p
              }).toSet
              val allParties = claims.claims.collect({
                case ClaimReadAsParty(p) => p
                case ClaimActAsParty(p) => p
              }).toSet

              val user =
                if (allParties.size == 1)
                  // Set a primary party if there's exactly one party for readAs and actAs
                  User(userId, allParties.head)
                else
                  if (actAsParties.size == 1) {
                    // Also set primary party if there's exactly one actAs right
                    User(userId, actAsParties.head)
                  } else
                    User(userId)

              Future.successful(user)
            }
        }
      )
    } else
      authorizer.requireAdminClaims(service.getUser)(request)
  }

  override def deleteUser(request: DeleteUserRequest): Future[DeleteUserResponse] =
    authorizer.requireAdminClaims(service.deleteUser)(request)

  override def listUsers(request: ListUsersRequest): Future[ListUsersResponse] =
    authorizer.requireAdminClaims(service.listUsers)(request)

  override def grantUserRights(request: GrantUserRightsRequest): Future[GrantUserRightsResponse] =
    authorizer.requireAdminClaims(service.grantUserRights)(request)

  override def revokeUserRights(request: RevokeUserRightsRequest): Future[RevokeUserRightsResponse] =
    authorizer.requireAdminClaims(service.revokeUserRights)(request)

  override def listUserRights(request: ListUserRightsRequest): Future[ListUserRightsResponse] = {
    if (request.userId.isEmpty) {
      // Request user-id is empty => use the one from the authenticated claims
      authorizer.withClaims(claims =>
        if (claims.isStandardJwtToken)
          claims.applicationId match {
            case Some(userId) => service.listUserRights(request.copy(userId = userId))
            case None =>
              Future.failed(
                LedgerApiErrors.AuthorizationChecks.PermissionDenied.Reject("user-id not set in authenticated claims").asGrpcError)
          }
        else {
          // Custom JWT token: deliver the decoded rights.
          val claimToRight: Claim => Option[Right] = {
            case ClaimActAsAnyParty => Some(Right(Right.Kind.CanActAsAnyParty(Right.CanActAsAnyParty())))
            case ClaimActAsParty(p) => Some(Right(Right.Kind.CanActAs(Right.CanActAs(p))))
            case ClaimReadAsParty(p) => Some(Right(Right.Kind.CanReadAs(Right.CanReadAs(p))))
            case ClaimAdmin => Some(Right(Right.Kind.ParticipantAdmin(Right.ParticipantAdmin())))
            case ClaimPublic => None
          }

          val rights = claims.claims.collect(claimToRight.unlift) // FIXME: is this the idiomatic way while keeping the pattern matching completeness checks?

          Future.successful(ListUserRightsResponse(rights))
        }
      )
    } else
      authorizer.requireAdminClaims(service.listUserRights)(request)
  }

  override def bindService(): ServerServiceDefinition =
    UserManagementServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()
}
