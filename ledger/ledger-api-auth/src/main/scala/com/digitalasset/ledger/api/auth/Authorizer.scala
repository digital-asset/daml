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

/** A simple helper that allows services to use authorization claims
  * that have been stored by [[AuthorizationInterceptor]].
  */
final class Authorizer(now: () => Instant, ledgerId: String, participantId: String) {

  private val logger = ContextualizedLogger.get(this.getClass)

  /** Validates all properties of claims that do not depend on the request,
    * such as expiration time or ledger ID. */
  private def valid(claims: Claims): Either[AuthorizationError, Unit] =
    for {
      _ <- claims.notExpired(now())
      _ <- claims.validForLedger(ledgerId)
      _ <- claims.validForParticipant(participantId)
    } yield {
      ()
    }

  def requirePublicClaimsOnStream[Req, Res](
      call: (Req, StreamObserver[Res]) => Unit): (Req, StreamObserver[Res]) => Unit =
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
        _ <- parties.foldLeft[Either[AuthorizationError, Unit]](Right(()))((acc, e) =>
          acc.flatMap(_ => claims.canReadAs(e)))
        _ <- applicationId.fold[Either[AuthorizationError, Unit]](Right(()))(id =>
          claims.validForApplication(id))
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
        _ <- parties.foldLeft[Either[AuthorizationError, Unit]](Right(()))((acc, e) =>
          acc.flatMap(_ => claims.canReadAs(e)))
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
        _ <- party.fold[Either[AuthorizationError, Unit]](Right(()))(p => claims.canActAs(p))
        _ <- applicationId.fold[Either[AuthorizationError, Unit]](Right(()))(id =>
          claims.validForApplication(id))
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
      _.notExpired(now()),
      authorizationError => {
        // Note: only put the claims in the context, as the request can be huge
        newLoggingContext("Claims" -> claims.toString) { implicit logCtx =>
          logger.warn(s"Permission denied. Reason: ${authorizationError.reason}.")
        }
        permissionDenied()
      }
    )

  private def authorize[Req, Res](call: (Req, ServerCallStreamObserver[Res]) => Unit)(
      authorized: Claims => Either[AuthorizationError, Unit],
  ): (Req, StreamObserver[Res]) => Unit =
    (request, observer) => {
      val scso = assertServerCall(observer)
      AuthorizationInterceptor
        .extractClaimsFromContext()
        .fold(
          observer.onError(_),
          claims =>
            authorized(claims) match {
              case Right(_) =>
                call(
                  request,
                  if (claims.expiration.isDefined)
                    ongoingAuthorization(scso, claims)
                  else
                    scso
                )
              case Left(authorizationError) =>
                // Note: only put the claims in the context, as the request can be huge
                newLoggingContext("Claims" -> claims.toString) { implicit logCtx =>
                  logger.warn(s"Permission denied. Reason: ${authorizationError.reason}.")
                }
                observer.onError(permissionDenied())
          }
        )
    }

  private def authorize[Req, Res](call: Req => Future[Res])(
      authorized: Claims => Either[AuthorizationError, Unit],
  ): Req => Future[Res] =
    request =>
      AuthorizationInterceptor
        .extractClaimsFromContext()
        .fold(
          Future.failed,
          claims =>
            authorized(claims) match {
              case Right(_) => call(request)
              case Left(authorizationError) =>
                // Note: only put the claims in the context, as the request can be huge
                newLoggingContext("Claims" -> claims.toString) { implicit logCtx =>
                  logger.warn(s"Permission denied. Reason: ${authorizationError.reason}.")
                }
                Future.failed(permissionDenied())
          }
      )

}
