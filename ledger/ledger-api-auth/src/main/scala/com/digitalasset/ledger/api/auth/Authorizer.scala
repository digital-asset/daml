// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import java.time.Instant

import com.daml.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.logging.ContextualizedLogger
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.platform.server.api.validation.ErrorFactories.permissionDenied
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}

import scala.concurrent.Future

sealed abstract class AuthorizationResult {
  def flatMap(f: Unit => AuthorizationResult): AuthorizationResult = this match {
    case Authorized => f(())
    case e: NotAuthorized => e
  }
  def map(f: Unit => Unit): AuthorizationResult = this match {
    case Authorized => Authorized
    case e: NotAuthorized => e
  }
  def isAuthorized: Boolean = this match {
    case Authorized => true
    case NotAuthorized(_) => false
  }
}
case object Authorized extends AuthorizationResult
final case class NotAuthorized(error: String) extends AuthorizationResult

/** A simple helper that allows services to use authorization claims
  * that have been stored by [[AuthorizationInterceptor]].
  */
final class Authorizer(now: () => Instant, ledgerId: String, participantId: String) {

  private val logger = ContextualizedLogger.get(this.getClass)

  /** Validates all properties of claims that do not depend on the request,
    * such as expiration time or ledger ID. */
  private def valid(claims: Claims): AuthorizationResult =
    for {
      _ <- if (claims.notExpired(now())) Authorized else NotAuthorized("Claims expired")
      _ <- if (claims.validForLedger(ledgerId)) Authorized
      else NotAuthorized(s"Claims not valid for ledgerId $ledgerId")
      _ <- if (claims.validForParticipant(participantId)) Authorized
      else NotAuthorized(s"Claims not valid for participantId $participantId")
    } yield {
      ()
    }

  def requirePublicClaimsOnStream[Req, Res](
      call: (Req, StreamObserver[Res]) => Unit): (Req, StreamObserver[Res]) => Unit =
    authorize(call) { claims =>
      for {
        _ <- valid(claims)
        _ <- if (claims.isPublic) Authorized else NotAuthorized("Public claim missing")
      } yield {
        ()
      }
    }

  def requirePublicClaims[Req, Res](call: Req => Future[Res]): Req => Future[Res] =
    authorize(call) { claims =>
      for {
        _ <- valid(claims)
        _ <- if (claims.isPublic) Authorized else NotAuthorized("Public claim missing")
      } yield {
        ()
      }
    }

  def requireAdminClaims[Req, Res](call: Req => Future[Res]): Req => Future[Res] =
    authorize(call) { claims =>
      for {
        _ <- valid(claims)
        _ <- if (claims.isAdmin) Authorized else NotAuthorized("Admin claim missing.")
      } yield {
        ()
      }
    }

  /** Wraps a streaming call to verify whether some Claims authorize to read as all parties
    * of the given set. Authorization is always granted for an empty collection of parties.
    */
  def requireReadClaimsForAllPartiesOnStream[Req, Res](
      parties: Iterable[String],
      applicationId: Option[String],
      call: (Req, StreamObserver[Res]) => Unit): (Req, StreamObserver[Res]) => Unit =
    authorize(call) { claims =>
      for {
        _ <- valid(claims)
        _ <- if (parties.forall(claims.canReadAs)) Authorized
        else
          NotAuthorized(
            s"Claims do not grant read rights for all of the following parties: $parties")
        _ <- if (applicationId.forall(claims.validForApplication)) Authorized
        else NotAuthorized(s"Claims not valid for applicationId ${applicationId.getOrElse("")}")
      } yield {
        ()
      }
    }

  /** Wraps a single call to verify whether some Claims authorize to read as all parties
    * of the given set. Authorization is always granted for an empty collection of parties.
    */
  def requireReadClaimsForAllParties[Req, Res](
      parties: Iterable[String],
      call: Req => Future[Res]): Req => Future[Res] =
    authorize(call) { claims =>
      for {
        _ <- valid(claims)
        _ <- if (parties.forall(claims.canReadAs)) Authorized
        else
          NotAuthorized(
            s"Claims do not grant read rights for all of the following parties: $parties")
      } yield {
        ()
      }
    }

  /** Checks whether the current Claims authorize to act as the given party, if any.
    * Note: A missing party or applicationId does NOT result in an authorization error.
    */
  def requireActClaimsForParty[Req, Res](
      party: Option[String],
      applicationId: Option[String],
      call: Req => Future[Res]): Req => Future[Res] =
    authorize(call) { claims =>
      for {
        _ <- valid(claims)
        _ <- if (party.forall(claims.canActAs)) Authorized
        else NotAuthorized(s"Claims do not grant act rights for party $party")
        _ <- if (applicationId.forall(claims.validForApplication)) Authorized
        else NotAuthorized(s"Claims not valid for applicationId ${applicationId.getOrElse("")}")
      } yield {
        ()
      }
    }

  /** Checks whether the current Claims authorize to read data for all parties mentioned in the given transaction filter */
  def requireReadClaimsForTransactionFilterOnStream[Req, Res](
      filter: Option[TransactionFilter],
      call: (Req, StreamObserver[Res]) => Unit): (Req, StreamObserver[Res]) => Unit =
    requireReadClaimsForAllPartiesOnStream(
      filter.map(_.filtersByParty).fold(Set.empty[String])(_.keySet),
      applicationId = None,
      call)

  private def assertServerCall[A](observer: StreamObserver[A]): ServerCallStreamObserver[A] =
    observer match {
      case _: ServerCallStreamObserver[_] =>
        observer.asInstanceOf[ServerCallStreamObserver[A]]
      case _ =>
        throw new IllegalArgumentException(
          s"The wrapped stream MUST be a ${classOf[ServerCallStreamObserver[_]].getName}")
    }

  private def ongoingAuthorization[Res](scso: ServerCallStreamObserver[Res], claims: Claims) =
    new OngoingAuthorizationObserver[Res](
      scso,
      claims,
      _.notExpired(now()), {
        newLoggingContext("Claims" -> claims.toString) { implicit logCtx =>
          logger.error(
            "Permission denied. Reason: The given claims have expired after the result stream has started.")
        }
        permissionDenied()
      }
    )

  private def authorize[Req, Res](call: (Req, ServerCallStreamObserver[Res]) => Unit)(
      authorized: Claims => AuthorizationResult,
  ): (Req, StreamObserver[Res]) => Unit =
    (request, observer) => {
      val scso = assertServerCall(observer)
      AuthorizationInterceptor
        .extractClaimsFromContext()
        .fold(
          observer.onError(_),
          claims =>
            authorized(claims) match {
              case Authorized =>
                call(
                  request,
                  if (claims.expiration.isDefined)
                    ongoingAuthorization(scso, claims)
                  else
                    scso
                )
              case NotAuthorized(reason) =>
                newLoggingContext("Request" -> request.toString, "Claims" -> claims.toString) {
                  implicit logCtx =>
                    logger.error(s"Permission denied. Reason: $reason.")
                }
                observer.onError(permissionDenied())
          }
        )
    }

  private def authorize[Req, Res](call: Req => Future[Res])(
      authorized: Claims => AuthorizationResult,
  ): Req => Future[Res] =
    request =>
      AuthorizationInterceptor
        .extractClaimsFromContext()
        .fold(
          Future.failed,
          claims =>
            authorized(claims) match {
              case Authorized => call(request)
              case NotAuthorized(reason) =>
                newLoggingContext("Request" -> request.toString, "Claims" -> claims.toString) {
                  implicit logCtx =>
                    logger.error(s"Permission denied. Reason: $reason.")
                }
                Future.failed(permissionDenied())
          }
      )

}
