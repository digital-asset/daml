// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.interceptor

import com.daml.error.{DamlContextualizedErrorLogger, ErrorCodesVersionSwitcher}
import com.daml.ledger.api.auth._
import com.daml.ledger.api.domain.{ApplicationId, UserRight}
import com.daml.ledger.participant.state.index.v2.UserManagementService
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.server.api.validation.ErrorFactories
import io.grpc._

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** This interceptor uses the given [[AuthService]] to get [[Claims]] for the current request,
  * and then stores them in the current [[Context]].
  */
final class AuthorizationInterceptor(
    protected val authService: AuthService, // FIXME: figure out why a protected val is required here
    userManagementService: UserManagementService,
    errorCodesVersionSwitcher: ErrorCodesVersionSwitcher,
)(implicit loggingContext: LoggingContext, ec: ExecutionContext)
    extends ServerInterceptor {
  private val logger = ContextualizedLogger.get(getClass)
  private val errorLogger = new DamlContextualizedErrorLogger(logger, loggingContext, None)
  private val errorFactories = ErrorFactories(errorCodesVersionSwitcher)

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
      FutureConverters
        .toScala(authService.decodeMetadata(headers))
        .flatMap(resolveUserRights)
        .onComplete {
          case Failure(exception) =>
            val error = errorFactories.internalAuthenticationError(
              securitySafeMessage = "Failed to get claims from request metadata",
              exception = exception,
            )(errorLogger)
            call.close(error.getStatus, error.getTrailers)
            new ServerCall.Listener[Nothing]() {}
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

  private [this] def resolveUserRights(claimSet : ClaimSet): Future[ClaimSet] = {
    claimSet match {
      case ClaimSet.Claims(claims, ledgerId, participantId, Some(applicationId), expiration) if claims.isEmpty =>   // FIXME: use the Claims.isStandardJwtToken function here
        // no claims embedded in the token and applicationId given ==> lookup user rights
        // FIXME: validation of userId
        getUserClaims(ApplicationId(Ref.LedgerString.assertFromString(applicationId))).map({
          case None => { // FIXME: understand why are these braces considered redundant by IntelliJ?
            logger.warn(s"Authorization error: cannot resolve rights for unknown user '$applicationId'.")
            ClaimSet.Unauthenticated
          }
          case Some(userClaims) =>
            ClaimSet.Claims(
              claims = userClaims.prepended(ClaimPublic),
              ledgerId = ledgerId,
              participantId = participantId,
              applicationId = Some(applicationId),
              expiration = expiration,
            )
        })
      case _ => Future.successful(claimSet)
    }
  }

  private[this] def userRightToClaim(r: UserRight): Claim = r match {
    case UserRight.CanActAs(p) => ClaimActAsParty(Ref.Party.assertFromString(p))
    case UserRight.CanReadAs(p) => ClaimReadAsParty(Ref.Party.assertFromString(p))
    case UserRight.ParticipantAdmin => ClaimAdmin
  }

  // FIXME: inline this function into the above
  private[this] def getUserClaims(userId : ApplicationId): Future[Option[List[Claim]]] = {
    userManagementService.listUserRights(userId)
      .map {
        // FIXME: figure out the idiomatic way to do that
        case Left(_) => None
        case Right(x) => Some(x.view.map(userRightToClaim).toList)
      }
  }
}

object AuthorizationInterceptor {

  private[auth] val contextKeyClaimSet = Context.key[ClaimSet]("AuthServiceDecodedClaim")

  def extractClaimSetFromContext(): Option[ClaimSet] =
    Option(contextKeyClaimSet.get())

  def apply(
      authService: AuthService,
      userManagementService: UserManagementService,
      errorCodesStatusSwitcher: ErrorCodesVersionSwitcher,
  )(implicit ec: ExecutionContext): AuthorizationInterceptor =
    LoggingContext.newLoggingContext { implicit loggingContext: LoggingContext =>
      new AuthorizationInterceptor(authService, userManagementService, errorCodesStatusSwitcher)
    }
}
