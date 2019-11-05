// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.auth

import java.time.Instant

import com.digitalasset.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.digitalasset.ledger.api.v1.transaction_filter.TransactionFilter
import com.digitalasset.platform.server.api.validation.ErrorFactories.permissionDenied
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}

import scala.concurrent.Future

object Authorizer {

  private def exception = permissionDenied("You are not authorized to use this resource")

}

/** A simple helper that allows services to use authorization claims
  * that have been stored by [[AuthorizationInterceptor]].
  */
final class Authorizer(now: () => Instant) {

  def requirePublicClaimsOnStream[Req, Res](
      call: (Req, StreamObserver[Res]) => Unit): (Req, StreamObserver[Res]) => Unit =
    wrapStream(c => c.notExpired(now()) && c.isPublic, call)

  def requirePublicClaims[Req, Res](call: Req => Future[Res]): Req => Future[Res] =
    wrapSingleCall(c => c.notExpired(now()) && c.isPublic, call)

  def requireAdminClaimsOnStream[Req, Res](
      call: (Req, StreamObserver[Res]) => Unit): (Req, StreamObserver[Res]) => Unit =
    wrapStream(c => c.notExpired(now()) && c.isAdmin, call)

  def requireAdminClaims[Req, Res](call: Req => Future[Res]): Req => Future[Res] =
    wrapSingleCall(c => c.notExpired(now()) && c.isAdmin, call)

  def requireNotExpiredOnStream[Req, Res](
      call: (Req, StreamObserver[Res]) => Unit): (Req, StreamObserver[Res]) => Unit =
    wrapStream(_.notExpired(now()), call)

  def requireNotExpired[Req, Res](call: Req => Future[Res]): Req => Future[Res] =
    wrapSingleCall(_.notExpired(now()), call)

  /** Wraps a streaming call to verify whether some Claims authorize to act as all parties
    * of the given set. Authorization is always granted for an empty collection of parties.
    */
  def requireClaimsForAllPartiesOnStream[Req, Res](
      parties: Iterable[String],
      call: (Req, StreamObserver[Res]) => Unit): (Req, StreamObserver[Res]) => Unit =
    wrapStream(c => c.notExpired(now()) && parties.forall(p => c.canActAs(p)), call)

  /** Wraps a single call to verify whether some Claims authorize to act as all parties
    * of the given set. Authorization is always granted for an empty collection of parties.
    */
  def requireClaimsForAllParties[Req, Res](
      parties: Iterable[String],
      call: Req => Future[Res]): Req => Future[Res] =
    wrapSingleCall(c => c.notExpired(now()) && parties.forall(p => c.canActAs(p)), call)

  /** Checks whether the current Claims authorize to act as the given party, if any.
    * Note: An missing party does NOT result in an authorization error.
    */
  def requireClaimsForPartyOnStream[Req, Res](
      party: Option[String],
      call: (Req, StreamObserver[Res]) => Unit): (Req, StreamObserver[Res]) => Unit =
    requireClaimsForAllPartiesOnStream(party.toList, call)

  /** Checks whether the current Claims authorize to act as the given party, if any.
    * Note: A missing party does NOT result in an authorization error.
    */
  def requireClaimsForParty[Req, Res](
      party: Option[String],
      call: Req => Future[Res]): Req => Future[Res] =
    requireClaimsForAllParties(party.toList, call)

  /** Checks whether the current Claims authorize to read data for all parties mentioned in the given transaction filter */
  def requireClaimsForTransactionFilterOnStream[Req, Res](
      filter: Option[TransactionFilter],
      call: (Req, StreamObserver[Res]) => Unit): (Req, StreamObserver[Res]) => Unit =
    requireClaimsForAllPartiesOnStream(
      filter.map(_.filtersByParty).fold(Set.empty[String])(_.keySet),
      call)

  /** Checks whether the current Claims authorize to read data for all parties mentioned in the given transaction filter */
  def requireClaimsForTransactionFilter[Req, Res](
      filter: Option[TransactionFilter],
      call: Req => Future[Res]): Req => Future[Res] =
    requireClaimsForAllParties(filter.map(_.filtersByParty).fold(Set.empty[String])(_.keySet), call)

  private def assertServerCall[A](observer: StreamObserver[A]): ServerCallStreamObserver[A] =
    observer match {
      case _: ServerCallStreamObserver[_] =>
        observer.asInstanceOf[ServerCallStreamObserver[A]]
      case _ =>
        throw new IllegalArgumentException(
          s"The wrapped stream MUST be a ${classOf[ServerCallStreamObserver[_]].getName}")
    }

  private def ongoingAuthorization[Res](scso: ServerCallStreamObserver[Res], claims: Claims) =
    new OngoingAuthorizationObserver[Res](scso, claims, _.notExpired(now()), Authorizer.exception)

  private def wrapStream[Req, Res](
      authorized: Claims => Boolean,
      call: (Req, ServerCallStreamObserver[Res]) => Unit): (Req, StreamObserver[Res]) => Unit =
    (request, observer) => {
      val scso = assertServerCall(observer)
      AuthorizationInterceptor
        .extractClaimsFromContext()
        .fold(
          observer.onError(_),
          claims =>
            if (authorized(claims))
              call(
                request,
                if (claims.expiration.isDefined)
                  ongoingAuthorization(scso, claims)
                else
                  scso
              )
            else observer.onError(Authorizer.exception)
        )
    }

  private def wrapSingleCall[Req, Res](
      authorized: Claims => Boolean,
      call: Req => Future[Res]): Req => Future[Res] =
    request =>
      AuthorizationInterceptor
        .extractClaimsFromContext()
        .fold(
          Future.failed,
          claims =>
            if (authorized(claims)) call(request)
            else Future.failed(Authorizer.exception)
      )

}
