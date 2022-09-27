// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.interceptor

import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.ledger.api.auth._
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.UserRight
import com.daml.ledger.api.validation.ValidationErrors
import com.daml.ledger.participant.state.index.v2.UserManagementStore
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.UserId
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import io.grpc._

import scala.jdk.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** This interceptor uses the given [[AuthService]] to get [[Claims]] for the current request,
  * and then stores them in the current [[Context]].
  *
  * @param userManagementStoreO - use None if user management is disabled
  */
final class AuthorizationInterceptor(
    authService: AuthService,
    userManagementStoreO: Option[UserManagementStore],
    implicit val ec: ExecutionContext,
)(implicit loggingContext: LoggingContext)
    extends ServerInterceptor {
  private val logger = ContextualizedLogger.get(getClass)
  private val errorLogger = new DamlContextualizedErrorLogger(logger, loggingContext, None)

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      nextListener: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {
    // Note: Context uses ThreadLocal storage, we need to capture it outside of the async block below.
    // Contexts are immutable and safe to pass around.
    val prevCtx = Context.current

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
        .decodeMetadata(headers)
        .asScala
        .flatMap(resolveAuthenticatedUserRights)
        .onComplete {
          case Failure(error: StatusRuntimeException) =>
            closeWithError(error)
          case Failure(exception: Throwable) =>
            val error = LedgerApiErrors.AuthorizationChecks.InternalAuthorizationError
              .Reject(
                message = "Failed to get claims from request metadata",
                throwable = exception,
              )(errorLogger)
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

  private[this] def resolveAuthenticatedUserRights(claimSet: ClaimSet): Future[ClaimSet] =
    claimSet match {
      case ClaimSet.AuthenticatedUser(userIdStr, participantId, expiration) =>
        for {
          userManagementStore <- getUserManagementStore(userManagementStoreO)
          userId <- getUserId(userIdStr)
          _ <- verifyUserIsActive(userManagementStore, userId)
          userRightsResult <- userManagementStore.listUserRights(userId)
          claimsSet <- userRightsResult match {
            case Left(msg) =>
              Future.failed(
                LedgerApiErrors.AuthorizationChecks.PermissionDenied
                  .Reject(
                    s"Could not resolve rights for user '$userId' due to '$msg'"
                  )(errorLogger)
                  .asGrpcError
              )
            case Right(userRights: Set[UserRight]) =>
              Future.successful(
                ClaimSet.Claims(
                  claims = AuthorizationInterceptor.convertUserRightsToClaims(userRights),
                  ledgerId = None,
                  participantId = participantId,
                  applicationId = Some(userId),
                  expiration = expiration,
                  resolvedFromUser = true,
                )
              )
          }
        } yield {
          claimsSet
        }
      case _ => Future.successful(claimSet)
    }

  private def verifyUserIsActive(
      userManagementStore: UserManagementStore,
      userId: UserId,
  ): Future[Unit] =
    for {
      userResult <- userManagementStore.getUser(id = userId)
      _ <- userResult match {
        case Left(msg) =>
          Future.failed(
            LedgerApiErrors.AuthorizationChecks.PermissionDenied
              .Reject(
                s"Could not resolve is_deactivated status for user '$userId' due to '$msg'"
              )(errorLogger)
              .asGrpcError
          )
        case Right(user: domain.User) =>
          if (user.isDeactivated) {
            Future.failed(
              LedgerApiErrors.AuthorizationChecks.PermissionDenied
                .Reject(
                  s"User $userId is deactivated"
                )(errorLogger)
                .asGrpcError
            )
          } else {
            Future.successful(())
          }
      }
    } yield ()

  private[this] def getUserManagementStore(
      userManagementStoreO: Option[UserManagementStore]
  ): Future[UserManagementStore] =
    userManagementStoreO match {
      case None =>
        Future.failed(
          LedgerApiErrors.AuthorizationChecks.Unauthenticated
            .UserBasedAuthenticationIsDisabled()(errorLogger)
            .asGrpcError
        )
      case Some(userManagementStore) =>
        Future.successful(userManagementStore)
    }

  private[this] def getUserId(userIdStr: String): Future[Ref.UserId] =
    Ref.UserId.fromString(userIdStr) match {
      case Left(err) =>
        Future.failed(
          ValidationErrors.invalidArgument(s"token $err")(errorLogger)
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

  def apply(
      authService: AuthService,
      userManagementStoreO: Option[UserManagementStore],
      ec: ExecutionContext,
  ): AuthorizationInterceptor =
    LoggingContext.newLoggingContext { implicit loggingContext: LoggingContext =>
      new AuthorizationInterceptor(authService, userManagementStoreO, ec)
    }

  def convertUserRightsToClaims(userRights: Set[UserRight]): Seq[Claim] = {
    userRights.view.map(userRightToClaim).toList.prepended(ClaimPublic)
  }

  private[this] def userRightToClaim(r: UserRight): Claim = r match {
    case UserRight.CanActAs(p) => ClaimActAsParty(Ref.Party.assertFromString(p))
    case UserRight.CanReadAs(p) => ClaimReadAsParty(Ref.Party.assertFromString(p))
    case UserRight.ParticipantAdmin => ClaimAdmin
  }
}
