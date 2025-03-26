// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import cats.syntax.either.*
import com.daml.jwt.JwtTimestampLeeway
import com.daml.tracing.Telemetry
import com.digitalasset.canton.LfLedgerString
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TelemetryTracing
import io.grpc.StatusRuntimeException
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import scalapb.lenses.Lens

import java.time.Instant
import scala.annotation.tailrec
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/** A simple helper that allows services to use authorization claims that have been stored by
  * [[AuthorizationInterceptor]].
  */
final class Authorizer(
    now: () => Instant,
    participantId: String,
    ongoingAuthorizationFactory: OngoingAuthorizationFactory = NoOpOngoingAuthorizationFactory(),
    jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
    protected val telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging
    with TelemetryTracing {

  /** Validates all properties of claims that do not depend on the request, such as expiration time
    * or ledger ID.
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

  private def implyIdentityProviderIdFromClaims[Req](
      identityProviderIdL: Lens[Req, String],
      claims: ClaimSet.Claims,
      req: Req,
  ): Req =
    if (identityProviderIdL.get(req) == "" && !claims.claims.contains(ClaimAdmin)) {
      val impliedIdpId = claims.identityProviderId.getOrElse(ClaimSet.DefaultIdentityProviderId)
      identityProviderIdL.set(impliedIdpId)(req)
    } else req

  private def sanitizeIdentityProviderId(
      identityProviderId: String
  ): Either[AuthorizationError, Option[LfLedgerString]] =
    (Some(identityProviderId).filter(_.nonEmpty) match {
      case Some(id) => LfLedgerString.fromString(id).map(Some(_))
      case None => Right(None)
    }).left.map { reason =>
      AuthorizationError.InvalidField("identity_provider_id", reason)
    }

  private def validateRequestIdentityProviderId(
      requestIdentityProviderId: Option[LfLedgerString],
      claims: ClaimSet.Claims,
  ): Either[AuthorizationError, Unit] = claims.identityProviderId match {
    case Some(_) if requestIdentityProviderId != claims.identityProviderId =>
      // Claim is valid only for the specific Identity Provider,
      // and identity_provider_id in the request matches the one provided in the claim.
      Left(
        AuthorizationError.InvalidIdentityProviderId(requestIdentityProviderId.getOrElse("<empty>"))
      )
    case _ =>
      Either.unit
  }

  private def authenticatedUserId(
      claims: ClaimSet.Claims
  ): Either[AuthorizationError, Option[String]] =
    if (claims.resolvedFromUser)
      claims.userId match {
        case Some(userId) => Right(Some(userId))
        case None =>
          Left(
            AuthorizationError.InternalAuthorizationError(
              "unexpectedly the user-id is not set in the authenticated claims",
              new RuntimeException(),
            )
          )
      }
    else
      Right(None)

  /** Compute the user-id for a request, defaulting to the one in the claims in case the request
    * does not specify an user-id.
    */
  private def defaultUserId(
      reqUserId: String,
      claims: ClaimSet.Claims,
  ): Either[AuthorizationError, String] =
    if (reqUserId.isEmpty)
      claims.userId match {
        case Some(userId) if userId.nonEmpty => Right(userId)
        case _ =>
          Left(
            AuthorizationError.MissingUserId(
              "Cannot default user_id field because claims do not specify an user-id. Is authentication turned on?"
            )
          )
      }
    else Right(reqUserId)

  private def authorizationErrorAsGrpc[T](
      errOrV: Either[AuthorizationError, T]
  ): Either[StatusRuntimeException, T] =
    errOrV.fold(
      {
        case AuthorizationError.InvalidField(fieldName, reason) =>
          Left(
            AuthorizationChecksErrors.InvalidToken
              .InvalidField(fieldName = fieldName, message = reason)
              .asGrpcError
          )

        case AuthorizationError.MissingUserId(reason) =>
          Left(
            AuthorizationChecksErrors.InvalidToken
              .MissingUserId(reason)
              .asGrpcError
          )

        case AuthorizationError.InternalAuthorizationError(reason, throwable) =>
          Left(
            AuthorizationChecksErrors.InternalAuthorizationError
              .Reject(reason, throwable)
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

  /** Directly access the authenticated claims from the thread-local context.
    *
    * Prefer to use the more specialized methods of [[Authorizer]] instead of this method to avoid
    * skipping required authorization checks.
    */
  private def authenticatedClaimsFromContext(): Try[ClaimSet.Claims] =
    AuthorizationInterceptor
      .extractClaimSetFromContext()
      .flatMap {
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
      }

  private def defaultToAuthenticatedUser(
      claims: ClaimSet.Claims,
      userId: String,
  ): Either[AuthorizationError, Option[String]] =
    authenticatedUserId(claims).flatMap {
      case Some(authUserId) if userId.isEmpty || userId == authUserId =>
        // We include the case where the request userId is equal to the authenticated userId in the defaulting.
        Right(Some(authUserId))

      case None if userId.isEmpty =>
        // This case can be hit both when running without authentication and when using custom Daml tokens.
        Left(
          AuthorizationError.MissingUserId(
            "requests with an empty user-id are only supported if there is an authenticated user"
          )
        )

      case _ => Right(None)
    }

  private def authorizeRequiredClaim[Req](
      requiredClaim: RequiredClaim[Req],
      claims: ClaimSet.Claims,
      req: Req,
  ): Either[AuthorizationError, Req] =
    requiredClaim match {
      case RequiredClaim.Public() => claims.isPublic.map(_ => req)

      case RequiredClaim.ReadAs(party) => claims.canReadAs(party).map(_ => req)

      case RequiredClaim.ReadAsAnyParty() => claims.canReadAsAnyParty.map(_ => req)

      case RequiredClaim.ActAs(party) => claims.canActAs(party).map(_ => req)

      case RequiredClaim.MatchIdentityProviderId(_) =>
        val identityProviderIdL = requiredClaim.requestStringL
        val modifiedRequest = implyIdentityProviderIdFromClaims(identityProviderIdL, claims, req)
        sanitizeIdentityProviderId(identityProviderIdL.get(modifiedRequest))
          .flatMap(validateRequestIdentityProviderId(_, claims))
          .map(_ => modifiedRequest)

      case RequiredClaim.MatchUserId(_, skipUserIdValidationForAnyPartyReaders) =>
        val userIdL = requiredClaim.requestStringL
        defaultUserId(userIdL.get(req), claims).flatMap(defaultedUserId =>
          Option
            .when(
              skipUserIdValidationForAnyPartyReaders && claims.claims.contains(
                ClaimReadAsAnyParty
              )
            )(Either.unit)
            .getOrElse(claims.validForUser(defaultedUserId))
            .map(_ => userIdL.set(defaultedUserId)(req))
        )

      case RequiredClaim.MatchUserIdForUserManagement(_) =>
        val userIdL = requiredClaim.requestStringL
        defaultToAuthenticatedUser(
          claims = claims,
          userId = userIdL.get(req),
        ).flatMap {
          case Some(userId) => Right(userIdL.set(userId)(req))
          case None => claims.isAdminOrIDPAdmin.map(_ => req)
        }

      case RequiredClaim.Admin() => claims.isAdmin.map(_ => req)

      case RequiredClaim.AdminOrIdpAdmin() => claims.isAdminOrIDPAdmin.map(_ => req)
    }

  @tailrec
  private def authorizedIteration[Req](
      requiredClaims: List[RequiredClaim[Req]],
      claims: ClaimSet.Claims,
      req: Req,
  ): Either[AuthorizationError, Req] =
    requiredClaims match {
      case Nil => Right(req)
      case requiredClaim :: rest =>
        authorizeRequiredClaim(requiredClaim, claims, req) match {
          case Left(err) => Left(err)
          case Right(newReq) => authorizedIteration(rest, claims, newReq)
        }
    }

  private def authorized[Req](
      requiredClaims: List[RequiredClaim[Req]],
      claims: ClaimSet.Claims,
      req: Req,
  ): Either[StatusRuntimeException, Req] =
    authorizationErrorAsGrpc(
      valid(claims).flatMap(_ => authorizedIteration(requiredClaims, claims, req))
    )

  def stream[Req, Res](
      call: (Req, StreamObserver[Res]) => Unit
  )(
      requiredClaims: RequiredClaim[Req]*
  )(req: Req, responseObserver: StreamObserver[Res]): Unit = {
    val serverCallStreamObserver = assertServerCall(responseObserver)
    authenticatedClaimsFromContext() match {
      case Failure(ex) => responseObserver.onError(ex)
      case Success(claims) =>
        authorized(requiredClaims.toList, claims, req) match {
          case Right(modifiedRequest) =>
            call(
              modifiedRequest,
              if (claims.expiration.isDefined || claims.resolvedFromUser)
                ongoingAuthorizationFactory(serverCallStreamObserver, claims)
              else
                serverCallStreamObserver,
            )
          case Left(ex) =>
            responseObserver.onError(ex)
        }
    }
  }

  def rpc[Req, Res](
      call: Req => Future[Res]
  )(
      requiredClaims: RequiredClaim[Req]*
  )(req: Req): Future[Res] =
    authenticatedClaimsFromContext() match {
      case Failure(ex) => Future.failed(ex)
      case Success(claims) =>
        authorized(requiredClaims.toList, claims, req) match {
          case Right(modifiedReq) => call(modifiedReq)
          case Left(ex) =>
            Future.failed(ex)
        }
    }
}

sealed trait RequiredClaim[Req] extends Product with Serializable {
  def requestStringL: Lens[Req, String] = throw new UnsupportedOperationException()
}

object RequiredClaim {
  final case class Public[Req]() extends RequiredClaim[Req]
  final case class ReadAs[Req](party: String) extends RequiredClaim[Req]
  final case class ReadAsAnyParty[Req]() extends RequiredClaim[Req]
  final case class ActAs[Req](party: String) extends RequiredClaim[Req]
  final case class MatchIdentityProviderId[Req](override val requestStringL: Lens[Req, String])
      extends RequiredClaim[Req]
  // TODO(i24590) consolidate these two
  final case class MatchUserId[Req](
      override val requestStringL: Lens[Req, String],
      skipUserIdValidationForAnyPartyReaders: Boolean = false,
  ) extends RequiredClaim[Req]
  final case class MatchUserIdForUserManagement[Req](override val requestStringL: Lens[Req, String])
      extends RequiredClaim[Req]
  final case class Admin[Req]() extends RequiredClaim[Req]
  final case class AdminOrIdpAdmin[Req]() extends RequiredClaim[Req]
}
