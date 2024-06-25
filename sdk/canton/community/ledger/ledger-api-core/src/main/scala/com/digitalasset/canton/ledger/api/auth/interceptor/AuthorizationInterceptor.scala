// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.interceptor

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.UserId
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.auth.*
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.{IdentityProviderId, User, UserRight}
import com.digitalasset.canton.ledger.api.validation.ValidationErrors
import com.digitalasset.canton.ledger.error.groups.AuthorizationChecksErrors
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import io.grpc.*

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.{Failure, Success, Try}

/** This interceptor uses the given [[AuthService]] to get [[com.digitalasset.canton.ledger.api.auth.ClaimSet.Claims]] for the current request,
  * and then stores them in the current [[io.grpc.Context]].
  *
  * @param userManagementStoreO - use None if user management is disabled
  */
final case class AuthorizationInterceptor(
    authService: AuthService,
    userManagementStoreO: Option[UserManagementStore],
    identityProviderAwareAuthService: IdentityProviderAwareAuthService,
    telemetry: Telemetry,
    loggerFactory: NamedLoggerFactory,
    implicit val ec: ExecutionContext,
) extends ServerInterceptor
    with NamedLogging {

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      nextListener: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {
    // Note: Context uses ThreadLocal storage, we need to capture it outside of the async block below.
    // Contexts are immutable and safe to pass around.
    val prevCtx = Context.current

    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)
    implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)

    // The method interceptCall() must return a Listener.
    // The target listener is created by calling `Contexts.interceptCall()`.
    // However, this is only done after we have asynchronously received the claims.
    // Therefore, we need to return a listener that buffers all messages until the target listener is available.
    new AsyncForwardingListener[ReqT] {
      private def closeWithError(error: StatusRuntimeException) = {
        call.close(error.getStatus, error.getTrailers)
        new ServerCall.Listener[Nothing]() {}
      }

      authService
        .decodeMetadata(headers)(loggingContextWithTrace.traceContext)
        .asScala
        .flatMap(fallbackToIdpAuthService(headers, _))
        .flatMap(resolveAuthenticatedUserRights)
        .onComplete {
          case Failure(error: StatusRuntimeException) =>
            closeWithError(error)
          case Failure(exception: Throwable) =>
            val error = AuthorizationChecksErrors.InternalAuthorizationError
              .Reject(
                message = "Failed to get claims from request metadata",
                throwable = exception,
              )
              .asGrpcError
            closeWithError(error)
          case Success(claimSet) =>
            val nextCtx = prevCtx.withValue(AuthorizationInterceptor.contextKeyClaimSet, claimSet)
            // Contexts.interceptCall() creates a listener that wraps all methods of `nextListener`
            // such that `Context.current` returns `nextCtx`.
            val nextListenerWithContext =
              Contexts.interceptCall(nextCtx, call, headers, nextListener)
            setNextListener(nextListenerWithContext)
            nextListenerWithContext
        }
    }
  }

  private def fallbackToIdpAuthService(headers: Metadata, claimSet: ClaimSet)(implicit
      loggingContext: LoggingContextWithTrace
  ) =
    if (claimSet == ClaimSet.Unauthenticated)
      identityProviderAwareAuthService.decodeMetadata(headers)
    else
      Future.successful(claimSet)

  private[this] def resolveAuthenticatedUserRights(
      claimSet: ClaimSet
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ErrorLoggingContext,
  ): Future[ClaimSet] =
    claimSet match {
      case ClaimSet.AuthenticatedUser(identityProviderId, userIdStr, participantId, expiration) =>
        for {
          userManagementStore <- getUserManagementStore(userManagementStoreO)
          userId <- getUserId(userIdStr)
          user <- verifyUserIsActive(userManagementStore, userId, identityProviderId)
          _ <- verifyUserIsWithinIdentityProvider(
            identityProviderId,
            user,
          )
          userRightsResult <- userManagementStore.listUserRights(userId, identityProviderId)
          claimsSet <- userRightsResult match {
            case Left(msg) =>
              Future.failed(
                AuthorizationChecksErrors.PermissionDenied
                  .Reject(
                    s"Could not resolve rights for user '$userId' due to '$msg'"
                  )
                  .asGrpcError
              )
            case Right(userRights: Set[UserRight]) =>
              Future.successful(
                ClaimSet.Claims(
                  claims = AuthorizationInterceptor.convertUserRightsToClaims(userRights),
                  participantId = participantId,
                  applicationId = Some(userId),
                  expiration = expiration,
                  resolvedFromUser = true,
                  identityProviderId = identityProviderId,
                )
              )
          }
        } yield {
          claimsSet
        }
      case _ => Future.successful(claimSet)
    }

  private def verifyUserIsWithinIdentityProvider(
      identityProviderId: IdentityProviderId,
      user: User,
  )(implicit errorLoggingContext: ErrorLoggingContext): Future[Unit] =
    if (user.identityProviderId != identityProviderId) {
      Future.failed(
        AuthorizationChecksErrors.PermissionDenied
          .Reject(
            s"User is assigned to another identity provider"
          )
          .asGrpcError
      )
    } else Future.unit

  private def verifyUserIsActive(
      userManagementStore: UserManagementStore,
      userId: UserId,
      identityProviderId: IdentityProviderId,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ErrorLoggingContext,
  ): Future[User] =
    for {
      userResult <- userManagementStore.getUser(id = userId, identityProviderId)
      value <- userResult match {
        case Left(msg) =>
          Future.failed(
            AuthorizationChecksErrors.PermissionDenied
              .Reject(
                s"Could not resolve is_deactivated status for user '$userId' and identity_provider_id '$identityProviderId' due to '$msg'"
              )
              .asGrpcError
          )
        case Right(user: domain.User) =>
          if (user.isDeactivated) {
            Future.failed(
              AuthorizationChecksErrors.PermissionDenied
                .Reject(
                  s"User $userId is deactivated"
                )
                .asGrpcError
            )
          } else {
            Future.successful(user)
          }
      }
    } yield value

  private[this] def getUserManagementStore(
      userManagementStoreO: Option[UserManagementStore]
  )(implicit errorLoggingContext: ErrorLoggingContext): Future[UserManagementStore] =
    userManagementStoreO match {
      case None =>
        Future.failed(
          AuthorizationChecksErrors.Unauthenticated
            .UserBasedAuthenticationIsDisabled()
            .asGrpcError
        )
      case Some(userManagementStore) =>
        Future.successful(userManagementStore)
    }

  private[this] def getUserId(
      userIdStr: String
  )(implicit errorLoggingContext: ErrorLoggingContext): Future[Ref.UserId] =
    Ref.UserId.fromString(userIdStr) match {
      case Left(err) =>
        Future.failed(
          ValidationErrors.invalidArgument(s"token $err")
        )
      case Right(userId) =>
        Future.successful(userId)
    }

}

object AuthorizationInterceptor {

  private[auth] val contextKeyClaimSet = Context.key[ClaimSet]("AuthServiceDecodedClaim")

  def extractClaimSetFromContext(): Try[ClaimSet] = {
    val claimSet = contextKeyClaimSet.get()
    if (claimSet == null)
      Failure(
        new RuntimeException(
          "Thread local context unexpectedly does not store authorization claims. Perhaps a Future was used in some intermediate computation and changed the executing thread?"
        )
      )
    else
      Success(claimSet)
  }

  def convertUserRightsToClaims(userRights: Set[UserRight]): Seq[Claim] = {
    userRights.view.map(userRightToClaim).toList.prepended(ClaimPublic)
  }

  private[this] def userRightToClaim(r: UserRight): Claim = r match {
    case UserRight.CanActAs(p) => ClaimActAsParty(Ref.Party.assertFromString(p))
    case UserRight.CanReadAs(p) => ClaimReadAsParty(Ref.Party.assertFromString(p))
    case UserRight.IdentityProviderAdmin => ClaimIdentityProviderAdmin
    case UserRight.ParticipantAdmin => ClaimAdmin
    case UserRight.CanReadAsAnyParty => ClaimReadAsAnyParty
  }
}
