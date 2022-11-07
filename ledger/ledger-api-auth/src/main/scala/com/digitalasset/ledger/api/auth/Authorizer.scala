// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import java.time.Instant

import akka.actor.Scheduler
import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.jwt.JwtTimestampLeeway
import com.daml.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.api.validation.ValidationErrors
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.localstore.api.UserManagementStore
import io.grpc.StatusRuntimeException
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import scalapb.lenses.Lens

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** A simple helper that allows services to use authorization claims
  * that have been stored by [[AuthorizationInterceptor]].
  */
final class Authorizer(
    now: () => Instant,
    ledgerId: String,
    participantId: String,
    userManagementStore: UserManagementStore,
    ec: ExecutionContext,
    userRightsCheckIntervalInSeconds: Int,
    akkaScheduler: Scheduler,
    jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
)(implicit loggingContext: LoggingContext) {
  private val logger = ContextualizedLogger.get(this.getClass)
  private implicit val errorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  /** Validates all properties of claims that do not depend on the request,
    * such as expiration time or ledger ID.
    */
  private def valid(claims: ClaimSet.Claims): Either[AuthorizationError, Unit] =
    for {
      _ <- claims.notExpired(now(), jwtTimestampLeeway)
      _ <- claims.validForLedger(ledgerId)
      _ <- claims.validForParticipant(participantId)
    } yield {
      ()
    }

  def requirePublicClaimsOnStream[Req, Res](
      call: (Req, StreamObserver[Res]) => Unit
  ): (Req, StreamObserver[Res]) => Unit =
    authorize(call) { claims =>
      for {
        _ <- valid(claims)
        _ <- claims.isPublic
      } yield {
        ()
      }
    }

  def requirePublicClaims[Req, Res](call: Req => Future[Res]): Req => Future[Res] =
    authorize(call) { claims =>
      for {
        _ <- valid(claims)
        _ <- claims.isPublic
      } yield {
        ()
      }
    }

  def requireAdminClaims[Req, Res](call: Req => Future[Res]): Req => Future[Res] =
    authorize(call) { claims =>
      for {
        _ <- valid(claims)
        _ <- claims.isAdmin
      } yield {
        ()
      }
    }

  def requireAdminOrIDPAdminClaims[Req, Res](call: Req => Future[Res]): Req => Future[Res] =
    authorize(call) { claims =>
      for {
        _ <- valid(claims)
        _ <- claims.isAdminOrIDPAdmin
      } yield {
        ()
      }
    }

  def requireIDPContext[Req, Res](
      identityProviderId: String,
      call: Req => Future[Res],
  )(addIdentityProvider: (String, Req) => Req): Req => Future[Res] =
    authorizeWithReq(call) { (claims, req) =>
      val potentiallyResolvedId = resolve(identityProviderId, claims)
      authorizationErrorAsGrpc(potentiallyResolvedId).map(id => addIdentityProvider(id, req))
    }

  private def resolve(
      identityProviderId: String,
      claims: ClaimSet.Claims,
  ): Either[AuthorizationError, String] = {
    val requestIdentityProviderId = Option(identityProviderId).filter(_.nonEmpty)
    if (claims.claims.contains(ClaimAdmin)) {
      // admin does not have any check, just idpId from the request is used
      Right(identityProviderId)
    } else if (!claims.resolvedFromUser) {
      // admin should always provide identityProviderId
      Right(identityProviderId)
    } else if (
      claims.resolvedFromUser && requestIdentityProviderId.isDefined && requestIdentityProviderId == claims.identityProviderId
    ) {
      // the user has provided idp_id, and it matches his issuer in the token
      Right(identityProviderId)
    } else if (claims.resolvedFromUser && requestIdentityProviderId.isEmpty)
      Right(claims.identityProviderId.getOrElse(""))
    else
      Left(
        AuthorizationError.MissingAdminClaim
      ) // TODO DPP-1299 Is it really admin claim here? or what?
  }

  private[this] def requireForAll[T](
      xs: IterableOnce[T],
      f: T => Either[AuthorizationError, Unit],
  ): Either[AuthorizationError, Unit] = {
    xs.iterator.foldLeft[Either[AuthorizationError, Unit]](Right(()))((acc, x) =>
      acc.flatMap(_ => f(x))
    )
  }

  /** Wraps a streaming call to verify whether some Claims authorize to read as all parties
    * of the given set. Authorization is always granted for an empty collection of parties.
    */
  def requireReadClaimsForAllPartiesOnStream[Req, Res](
      parties: Iterable[String],
      call: (Req, StreamObserver[Res]) => Unit,
  ): (Req, StreamObserver[Res]) => Unit =
    authorize(call) { claims =>
      for {
        _ <- valid(claims)
        _ <- requireForAll(parties, party => claims.canReadAs(party))
      } yield {
        ()
      }
    }

  def requireReadClaimsForAllPartiesOnStreamWithApplicationId[Req, Res](
      parties: Iterable[String],
      applicationIdL: Lens[Req, String],
      call: (Req, StreamObserver[Res]) => Unit,
  ): (Req, StreamObserver[Res]) => Unit =
    authorizeWithReq(call) { (claims, req) =>
      val reqApplicationId = applicationIdL.get(req)
      for {
        _ <- authorizationErrorAsGrpc(valid(claims))
        _ <- authorizationErrorAsGrpc(requireForAll(parties, party => claims.canReadAs(party)))
        defaultedApplicationId <- defaultApplicationId(reqApplicationId, claims)
        _ <- authorizationErrorAsGrpc(claims.validForApplication(defaultedApplicationId))
      } yield applicationIdL.set(defaultedApplicationId)(req)
    }

  /** Wraps a single call to verify whether some Claims authorize to read as all parties
    * of the given set. Authorization is always granted for an empty collection of parties.
    */
  def requireReadClaimsForAllParties[Req, Res](
      parties: Iterable[String],
      call: Req => Future[Res],
  ): Req => Future[Res] =
    authorize(call) { claims =>
      for {
        _ <- valid(claims)
        _ <- requireForAll(parties, party => claims.canReadAs(party))
      } yield {
        ()
      }
    }

  def requireActAndReadClaimsForParties[Req, Res](
      actAs: Set[String],
      readAs: Set[String],
      applicationIdL: Lens[Req, String],
      call: Req => Future[Res],
  ): Req => Future[Res] =
    authorizeWithReq(call) { (claims, req) =>
      val reqApplicationId = applicationIdL.get(req)
      for {
        _ <- authorizationErrorAsGrpc(valid(claims))
        _ <- authorizationErrorAsGrpc(
          actAs.foldRight[Either[AuthorizationError, Unit]](Right(()))((p, acc) =>
            acc.flatMap(_ => claims.canActAs(p))
          )
        )
        _ <- authorizationErrorAsGrpc(
          readAs.foldRight[Either[AuthorizationError, Unit]](Right(()))((p, acc) =>
            acc.flatMap(_ => claims.canReadAs(p))
          )
        )
        defaultedApplicationId <- defaultApplicationId(reqApplicationId, claims)
        _ <- authorizationErrorAsGrpc(claims.validForApplication(defaultedApplicationId))
      } yield applicationIdL.set(defaultedApplicationId)(req)
    }

  /** Checks whether the current Claims authorize to read data for all parties mentioned in the given transaction filter */
  def requireReadClaimsForTransactionFilterOnStream[Req, Res](
      filter: Option[TransactionFilter],
      call: (Req, StreamObserver[Res]) => Unit,
  ): (Req, StreamObserver[Res]) => Unit =
    requireReadClaimsForAllPartiesOnStream(
      filter.map(_.filtersByParty).fold(Set.empty[String])(_.keySet),
      call,
    )

  def authenticatedUserId(): Try[Option[String]] =
    authenticatedClaimsFromContext()
      .flatMap(claims =>
        if (claims.resolvedFromUser)
          claims.applicationId match {
            case Some(applicationId) => Success(Some(applicationId))
            case None =>
              Failure(
                LedgerApiErrors.AuthorizationChecks.InternalAuthorizationError
                  .Reject(
                    "unexpectedly the user-id is not set in the authenticated claims",
                    new RuntimeException(),
                  )
                  .asGrpcError
              )
          }
        else
          Success(None)
      )

  /** Compute the application-id for a request, defaulting to the one in the claims in
    * case the request does not specify an application-id.
    */
  private def defaultApplicationId(
      reqApplicationId: String,
      claims: ClaimSet.Claims,
  ): Either[StatusRuntimeException, String] =
    if (reqApplicationId.isEmpty)
      claims.applicationId match {
        case Some(applicationId) if applicationId.nonEmpty => Right(applicationId)
        case _ =>
          Left(
            ValidationErrors.invalidArgument(
              "Cannot default application_id field because claims do not specify an application-id or user-id. Is authentication turned on?"
            )
          )
      }
    else Right(reqApplicationId)

  private def authorizationErrorAsGrpc[T](
      errOrV: Either[AuthorizationError, T]
  ): Either[StatusRuntimeException, T] =
    errOrV.fold(
      err =>
        Left(LedgerApiErrors.AuthorizationChecks.PermissionDenied.Reject(err.reason).asGrpcError),
      Right(_),
    )

  private def assertServerCall[A](observer: StreamObserver[A]): ServerCallStreamObserver[A] =
    observer match {
      case _: ServerCallStreamObserver[_] =>
        observer.asInstanceOf[ServerCallStreamObserver[A]]
      case _ =>
        throw new IllegalArgumentException(
          s"The wrapped stream MUST be a ${classOf[ServerCallStreamObserver[_]].getName}"
        )
    }

  private def ongoingAuthorization[Res](
      observer: ServerCallStreamObserver[Res],
      claims: ClaimSet.Claims,
  ) = OngoingAuthorizationObserver[Res](
    observer = observer,
    originalClaims = claims,
    nowF = now,
    userManagementStore = userManagementStore,
    userRightsCheckIntervalInSeconds = userRightsCheckIntervalInSeconds,
    akkaScheduler = akkaScheduler,
    jwtTimestampLeeway = jwtTimestampLeeway,
  )(loggingContext, ec)

  /** Directly access the authenticated claims from the thread-local context.
    *
    * Prefer to use the more specialized methods of [[Authorizer]] instead of this
    * method to avoid skipping required authorization checks.
    */
  private def authenticatedClaimsFromContext(): Try[ClaimSet.Claims] =
    AuthorizationInterceptor
      .extractClaimSetFromContext()
      .flatMap({
        case ClaimSet.Unauthenticated =>
          Failure(
            LedgerApiErrors.AuthorizationChecks.Unauthenticated
              .MissingJwtToken()
              .asGrpcError
          )
        case authenticatedUser: ClaimSet.AuthenticatedUser =>
          Failure(
            LedgerApiErrors.AuthorizationChecks.InternalAuthorizationError
              .Reject(
                s"Unexpected unresolved authenticated user claim",
                new RuntimeException(
                  s"Unexpected unresolved authenticated user claim for user '${authenticatedUser.userId}"
                ),
              )
              .asGrpcError
          )
        case claims: ClaimSet.Claims => Success(claims)
      })

  private def authorizeWithReq[Req, Res](call: (Req, ServerCallStreamObserver[Res]) => Unit)(
      authorized: (ClaimSet.Claims, Req) => Either[StatusRuntimeException, Req]
  ): (Req, StreamObserver[Res]) => Unit = (request, observer) => {
    val serverCallStreamObserver = assertServerCall(observer)
    authenticatedClaimsFromContext()
      .fold(
        ex => {
          observer.onError(ex)
        },
        claims =>
          authorized(claims, request) match {
            case Right(modifiedRequest) =>
              call(
                modifiedRequest,
                if (claims.expiration.isDefined || claims.resolvedFromUser)
                  ongoingAuthorization(serverCallStreamObserver, claims)
                else
                  serverCallStreamObserver,
              )
            case Left(ex) =>
              observer.onError(ex)
          },
      )
  }

  private def authorize[Req, Res](call: (Req, ServerCallStreamObserver[Res]) => Unit)(
      authorized: ClaimSet.Claims => Either[AuthorizationError, Unit]
  ): (Req, StreamObserver[Res]) => Unit =
    authorizeWithReq(call)((claims, req) =>
      authorizationErrorAsGrpc(authorized(claims)).map(_ => req)
    )

  private[auth] def authorizeWithReq[Req, Res](call: Req => Future[Res])(
      authorized: (ClaimSet.Claims, Req) => Either[StatusRuntimeException, Req]
  ): Req => Future[Res] = request =>
    authenticatedClaimsFromContext() match {
      case Failure(ex) => Future.failed(ex)
      case Success(claims) =>
        authorized(claims, request) match {
          case Right(modifiedReq) => call(modifiedReq)
          case Left(ex) =>
            Future.failed(ex)
        }
    }

  private[auth] def authorize[Req, Res](call: Req => Future[Res])(
      authorized: ClaimSet.Claims => Either[AuthorizationError, Unit]
  ): Req => Future[Res] =
    authorizeWithReq(call)((claims, req) =>
      authorizationErrorAsGrpc(authorized(claims)).map(_ => req)
    )

}
