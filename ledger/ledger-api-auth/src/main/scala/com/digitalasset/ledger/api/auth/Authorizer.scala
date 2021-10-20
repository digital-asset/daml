// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import com.daml.error.{
  ContextualizedErrorLogger,
  DamlContextualizedErrorLogger,
  ErrorCodesVersionSwitcher,
}
import com.daml.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.server.api.validation.ErrorFactories
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}

import java.time.Instant
import scala.collection.compat._
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/** A simple helper that allows services to use authorization claims
  * that have been stored by [[AuthorizationInterceptor]].
  */
final class Authorizer(
    now: () => Instant,
    ledgerId: String,
    participantId: String,
    errorCodesVersionSwitcher: ErrorCodesVersionSwitcher,
)(implicit loggingContext: LoggingContext) {
  private val logger = ContextualizedLogger.get(this.getClass)
  private val errorFactories = ErrorFactories(errorCodesVersionSwitcher)
  private implicit val errorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  /** Validates all properties of claims that do not depend on the request,
    * such as expiration time or ledger ID.
    */
  private def valid(claims: ClaimSet.Claims): Either[AuthorizationError, Unit] =
    for {
      _ <- claims.notExpired(now())
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
      applicationId: Option[String],
      call: (Req, StreamObserver[Res]) => Unit,
  ): (Req, StreamObserver[Res]) => Unit =
    authorize(call) { claims =>
      for {
        _ <- valid(claims)
        _ <- requireForAll(parties, party => claims.canReadAs(party))
        _ <- applicationId.map(claims.validForApplication).getOrElse(Right(()))
      } yield {
        ()
      }
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

  /** Checks whether the current Claims authorize to act as the given party, if any.
    * Note: A missing party or applicationId does NOT result in an authorization error.
    */
  def requireActClaimsForParty[Req, Res](
      party: Option[String],
      applicationId: Option[String],
      call: Req => Future[Res],
  ): Req => Future[Res] =
    authorize(call) { claims =>
      for {
        _ <- valid(claims)
        _ <- party.map(claims.canActAs).getOrElse(Right(()))
        _ <- applicationId.map(claims.validForApplication).getOrElse(Right(()))
      } yield {
        ()
      }
    }

  def requireActAndReadClaimsForParties[Req, Res](
      actAs: Set[String],
      readAs: Set[String],
      applicationId: Option[String],
      call: Req => Future[Res],
  ): Req => Future[Res] =
    authorize(call) { claims =>
      for {
        _ <- valid(claims)
        _ <- actAs.foldRight[Either[AuthorizationError, Unit]](Right(()))((p, acc) =>
          acc.flatMap(_ => claims.canActAs(p))
        )
        _ <- readAs.foldRight[Either[AuthorizationError, Unit]](Right(()))((p, acc) =>
          acc.flatMap(_ => claims.canReadAs(p))
        )
        _ <- applicationId.map(claims.validForApplication).getOrElse(Right(()))
      } yield {
        ()
      }
    }

  /** Checks whether the current Claims authorize to read data for all parties mentioned in the given transaction filter */
  def requireReadClaimsForTransactionFilterOnStream[Req, Res](
      filter: Option[TransactionFilter],
      call: (Req, StreamObserver[Res]) => Unit,
  ): (Req, StreamObserver[Res]) => Unit =
    requireReadClaimsForAllPartiesOnStream(
      filter.map(_.filtersByParty).fold(Set.empty[String])(_.keySet),
      applicationId = None,
      call,
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
      scso: ServerCallStreamObserver[Res],
      claims: ClaimSet.Claims,
  ) = new OngoingAuthorizationObserver[Res](
    scso,
    claims,
    _.notExpired(now()),
    authorizationError => {
      errorFactories.permissionDenied(authorizationError.reason)
    },
  )

  private def authenticatedClaimsFromContext(): Try[ClaimSet.Claims] =
    AuthorizationInterceptor
      .extractClaimSetFromContext()
      .fold[Try[ClaimSet.Claims]](Failure(errorFactories.unauthenticatedMissingJwtToken())) {
        case ClaimSet.Unauthenticated =>
          Failure(errorFactories.unauthenticatedMissingJwtToken())
        case claims: ClaimSet.Claims => Success(claims)
      }

  private def authorize[Req, Res](call: (Req, ServerCallStreamObserver[Res]) => Unit)(
      authorized: ClaimSet.Claims => Either[AuthorizationError, Unit]
  ): (Req, StreamObserver[Res]) => Unit = (request, observer) => {
    val scso = assertServerCall(observer)
    authenticatedClaimsFromContext()
      .fold(
        ex => {
          // TODO error codes: Remove once fully relying on self-service error codes with logging on creation
          logger.debug(
            s"No authenticated claims found in the request context. Returning UNAUTHENTICATED"
          )
          observer.onError(ex)
        },
        claims =>
          authorized(claims) match {
            case Right(_) =>
              call(
                request,
                if (claims.expiration.isDefined)
                  ongoingAuthorization(scso, claims)
                else
                  scso,
              )
            case Left(authorizationError) =>
              observer.onError(
                errorFactories.permissionDenied(authorizationError.reason)
              )
          },
      )
  }

  private[auth] def authorize[Req, Res](call: Req => Future[Res])(
      authorized: ClaimSet.Claims => Either[AuthorizationError, Unit]
  ): Req => Future[Res] = request =>
    authenticatedClaimsFromContext()
      .fold(
        ex => {
          // TODO error codes: Remove once fully relying on self-service error codes with logging on creation
          logger.debug(
            s"No authenticated claims found in the request context. Returning UNAUTHENTICATED"
          )
          Future.failed(ex)
        },
        claims =>
          authorized(claims) match {
            case Right(_) => call(request)
            case Left(authorizationError) =>
              Future.failed(
                errorFactories.permissionDenied(authorizationError.reason)
              )
          },
      )
}

object Authorizer {
  def apply(
      now: () => Instant,
      ledgerId: String,
      participantId: String,
      errorCodesVersionSwitcher: ErrorCodesVersionSwitcher,
  ): Authorizer =
    LoggingContext.newLoggingContext { loggingContext =>
      new Authorizer(now, ledgerId, participantId, errorCodesVersionSwitcher)(loggingContext)
    }
}
