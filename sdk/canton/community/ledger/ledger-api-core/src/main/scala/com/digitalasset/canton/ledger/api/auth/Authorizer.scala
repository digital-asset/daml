// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import com.daml.jwt.JwtTimestampLeeway
import com.daml.ledger.api.v2.transaction_filter.Filters
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.digitalasset.canton.ledger.api.domain.IdentityProviderId
import com.digitalasset.canton.ledger.api.validation.ValidationErrors
import com.digitalasset.canton.ledger.error.groups.{
  AuthorizationChecksErrors,
  RequestValidationErrors,
}
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.{TelemetryTracing, TraceContext}
import io.grpc.StatusRuntimeException
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import org.apache.pekko.actor.Scheduler
import scalapb.lenses.Lens

import java.time.{Duration, Instant}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** A simple helper that allows services to use authorization claims
  * that have been stored by [[com.digitalasset.canton.ledger.api.auth.interceptor.AuthorizationInterceptor]].
  */
final class Authorizer(
    now: () => Instant,
    participantId: String,
    userManagementStore: UserManagementStore,
    ec: ExecutionContext,
    userRightsCheckIntervalInSeconds: Int,
    pekkoScheduler: Scheduler,
    jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
    tokenExpiryGracePeriodForStreams: Option[Duration] = None,
    protected val telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging
    with TelemetryTracing {

  /** Validates all properties of claims that do not depend on the request,
    * such as expiration time or ledger ID.
    */
  private def valid(claims: ClaimSet.Claims): Either[AuthorizationError, Unit] =
    for {
      _ <- claims.notExpired(
        now(),
        jwtTimestampLeeway,
        None,
      ) // Don't use the grace period for the initial check
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
    authorize(call) { (claims, req) =>
      for {
        _ <- valid(claims)
        _ <- claims.isPublic
      } yield req
    }

  def requireAdminClaims[Req, Res](call: Req => Future[Res]): Req => Future[Res] =
    authorize(call) { (claims, req) =>
      for {
        _ <- valid(claims)
        _ <- claims.isAdmin
      } yield req
    }

  def requireIdpAdminClaimsAndMatchingRequestIdpId[Req, Res](
      identityProviderIdL: Lens[Req, String],
      call: Req => Future[Res],
  ): Req => Future[Res] =
    requireIdpAdminClaimsAndMatchingRequestIdpId(identityProviderIdL, false, call)

  def requireIdpAdminClaimsAndMatchingRequestIdpId[Req, Res](
      identityProviderIdL: Lens[Req, String],
      mustBeParticipantAdmin: Boolean,
      call: Req => Future[Res],
  )(req: Req): Future[Res] =
    authorize(call) { (claims, req) =>
      for {
        _ <- valid(claims)
        _ <- if (mustBeParticipantAdmin) claims.isAdmin else claims.isAdminOrIDPAdmin
        modifiedRequest = implyIdentityProviderIdFromClaims(identityProviderIdL, claims, req)
        requestIdentityProviderId <- requireIdentityProviderId(
          identityProviderIdL.get(modifiedRequest)
        )
        _ <- validateRequestIdentityProviderId(requestIdentityProviderId, claims)
      } yield modifiedRequest
    }(req)

  private def implyIdentityProviderIdFromClaims[Req](
      identityProviderIdL: Lens[Req, String],
      claims: ClaimSet.Claims,
      req: Req,
  ): Req = {
    if (identityProviderIdL.get(req) == "" && !claims.claims.contains(ClaimAdmin)) {
      val impliedIdpId = identityProviderIdFromClaims.fold("")(_.toRequestString)
      identityProviderIdL.set(impliedIdpId)(req)
    } else req
  }

  def requireMatchingRequestIdpId[Req, Res](
      identityProviderIdL: Lens[Req, String],
      call: Req => Future[Res],
  ): Req => Future[Res] =
    authorize(call) { (claims, req) =>
      for {
        _ <- valid(claims)
        modifiedRequest = implyIdentityProviderIdFromClaims(identityProviderIdL, claims, req)
        requestIdentityProviderId <- requireIdentityProviderId(
          identityProviderIdL.get(modifiedRequest)
        )
        _ <- validateRequestIdentityProviderId(requestIdentityProviderId, claims)
      } yield modifiedRequest
    }

  def requireIdpAdminClaims[Req, Res](
      call: Req => Future[Res]
  ): Req => Future[Res] =
    authorize(call) { (claims, req) =>
      for {
        _ <- valid(claims)
        _ <- claims.isAdminOrIDPAdmin
      } yield req
    }

  private def requireIdentityProviderId(
      identityProviderId: String
  ): Either[AuthorizationError, IdentityProviderId] =
    IdentityProviderId.fromString(identityProviderId).left.map { reason =>
      AuthorizationError.InvalidField("identity_provider_id", reason)
    }

  private def validateRequestIdentityProviderId(
      requestIdentityProviderId: IdentityProviderId,
      claims: ClaimSet.Claims,
  ): Either[AuthorizationError, Unit] = claims.identityProviderId match {
    case id: IdentityProviderId.Id if requestIdentityProviderId != id =>
      // Claim is valid only for the specific Identity Provider,
      // and identity_provider_id in the request matches the one provided in the claim.
      Left(AuthorizationError.InvalidIdentityProviderId(requestIdentityProviderId))
    case _ =>
      Right(())
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
      readAsAnyParty: Boolean,
      call: (Req, StreamObserver[Res]) => Unit,
  ): (Req, StreamObserver[Res]) => Unit =
    authorize(call) { claims =>
      for {
        _ <- valid(claims)
        _ <- if (readAsAnyParty) claims.canReadAsAnyParty else Right(())
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
        _ <-
          if (claims.claims.contains(ClaimReadAsAnyParty))
            Right(())
          else authorizationErrorAsGrpc(claims.validForApplication(defaultedApplicationId))
      } yield applicationIdL.set(defaultedApplicationId)(req)
    }

  /** Wraps a single call to verify whether some Claims authorize to read as all parties
    * of the given set. Authorization is always granted for an empty collection of parties.
    */
  def requireReadClaimsForAllParties[Req, Res](
      parties: Iterable[String],
      call: Req => Future[Res],
  ): Req => Future[Res] =
    authorize(call) { (claims, req) =>
      for {
        _ <- valid(claims)
        _ <- requireForAll(parties, party => claims.canReadAs(party))
      } yield req
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
      filter: Option[Map[String, Filters]],
      readAsAnyParty: Boolean,
      call: (Req, StreamObserver[Res]) => Unit,
  ): (Req, StreamObserver[Res]) => Unit =
    requireReadClaimsForAllPartiesOnStream(
      filter.fold(Set.empty[String])(_.keySet),
      readAsAnyParty,
      call,
    )

  def identityProviderIdFromClaims: Option[IdentityProviderId] =
    authenticatedClaimsFromContext().map(_.identityProviderId).toOption

  def authenticatedUserId(): Try[Option[String]] =
    authenticatedClaimsFromContext()
      .flatMap(claims =>
        if (claims.resolvedFromUser)
          claims.applicationId match {
            case Some(applicationId) => Success(Some(applicationId))
            case None =>
              Failure(
                AuthorizationChecksErrors.InternalAuthorizationError
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
      {
        case AuthorizationError.InvalidField(fieldName, reason) =>
          Left(
            RequestValidationErrors.InvalidField
              .Reject(fieldName = fieldName, message = reason)
              .asGrpcError
          )
        case err =>
          Left(
            AuthorizationChecksErrors.PermissionDenied.Reject(err.reason).asGrpcError
          )
      },
      Right(_),
    )

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
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
    pekkoScheduler = pekkoScheduler,
    jwtTimestampLeeway = jwtTimestampLeeway,
    tokenExpiryGracePeriodForStreams = tokenExpiryGracePeriodForStreams,
    loggerFactory = loggerFactory,
  )(ec, TraceContext.empty)

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
            AuthorizationChecksErrors.Unauthenticated
              .MissingJwtToken()
              .asGrpcError
          )
        case authenticatedUser: ClaimSet.AuthenticatedUser =>
          Failure(
            AuthorizationChecksErrors.InternalAuthorizationError
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
      authorized: (ClaimSet.Claims, Req) => Either[AuthorizationError, Req]
  ): Req => Future[Res] =
    authorizeWithReq(call)((claims, req) => authorizationErrorAsGrpc(authorized(claims, req)))

}
