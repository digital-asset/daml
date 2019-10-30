// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.auth

import java.time.Clock

import com.digitalasset.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.digitalasset.ledger.api.v1.transaction_filter.TransactionFilter
import com.digitalasset.platform.server.api.validation.ErrorFactories.permissionDenied
import io.grpc.stub.StreamObserver

import scala.concurrent.Future

object Authorizer {

  private def authError = permissionDenied("You are not authorized to use this resource")

}

/** A simple helper that allows services to use authorization claims
  * that have been stored by [[AuthorizationInterceptor]].
  */
final class Authorizer(clock: Clock) {

  import Authorizer.authError

  def requirePublicClaimsOnStream[Req, Res](
      call: (Req, StreamObserver[Res]) => Unit): (Req, StreamObserver[Res]) => Unit =
    wrapStream(_.isPublic(clock), call)

  def requirePublicClaims[Req, Res](call: Req => Future[Res]): Req => Future[Res] =
    wrapSingleCall(_.isPublic(clock), call)

  def requireAdminClaimsOnStream[Req, Res](
      call: (Req, StreamObserver[Res]) => Unit): (Req, StreamObserver[Res]) => Unit =
    wrapStream(_.isAdmin(clock), call)

  def requireAdminClaims[Req, Res](call: Req => Future[Res]): Req => Future[Res] =
    wrapSingleCall(_.isAdmin(clock), call)

  def requireNotExpiredOnStream[Req, Res](
      call: (Req, StreamObserver[Res]) => Unit): (Req, StreamObserver[Res]) => Unit =
    wrapStream(_.notExpired(clock), call)

  def requireNotExpired[Req, Res](call: Req => Future[Res]): Req => Future[Res] =
    wrapSingleCall(_.notExpired(clock), call)

  /** Wraps a streaming call to verify whether some Claims authorize to act as all parties
    * of the given set. Authorization is always granted for an empty collection of parties.
    */
  def requireClaimsForAllPartiesOnStream[Req, Res](
      parties: Iterable[String],
      call: (Req, StreamObserver[Res]) => Unit): (Req, StreamObserver[Res]) => Unit =
    wrapStream(claims => parties.forall(p => claims.canActAs(p, clock)), call)

  /** Wraps a single call to verify whether some Claims authorize to act as all parties
    * of the given set. Authorization is always granted for an empty collection of parties.
    */
  def requireClaimsForAllParties[Req, Res](
      parties: Iterable[String],
      call: Req => Future[Res]): Req => Future[Res] =
    wrapSingleCall(claims => parties.forall(p => claims.canActAs(p, clock)), call)

  /** Checks whether the current Claims authorize to act as the given party, if any.
    * Note: An missing party does NOT result in an authorization error.
    */
  def requireClaimsForPartyOnStream[Req, Res](
      party: Option[String],
      call: (Req, StreamObserver[Res]) => Unit): (Req, StreamObserver[Res]) => Unit =
    requireClaimsForAllPartiesOnStream(party.toList, call)

  /** Checks whether the current Claims authorize to act as the given party, if any.
    * Note: An missing party does NOT result in an authorization error.
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

  private def wrapStream[Req, Res](
      authorized: Claims => Boolean,
      call: (Req, StreamObserver[Res]) => Unit): (Req, StreamObserver[Res]) => Unit =
    (request, observer) => {
      AuthorizationInterceptor
        .extractClaimsFromContext()
        .fold(
          observer.onError(_),
          claims =>
            if (authorized(claims))
              call(
                request,
                if (claims.expiration.isDefined)
                  new OngoingAuthorizationObserver(observer, claims, _.notExpired(clock), authError)
                else observer
              )
            else observer.onError(authError)
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
            else Future.failed(authError)
      )

}
