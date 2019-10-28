// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.authorization

import com.digitalasset.ledger.api.auth.Claims
import com.digitalasset.ledger.api.v1.transaction_filter.TransactionFilter
import com.digitalasset.platform.server.api.validation.ErrorFactories._
import io.grpc.{Context, StatusRuntimeException}
import org.slf4j.{Logger, LoggerFactory}

sealed abstract class Authorization
case object Authorized extends Authorization
final case class NotAuthorized(reason: String = "You are not authorized to use this resource")
    extends Authorization

/** A simple helper that allows services to use authorization claims
  * that have been stored by [[AuthorizationInterceptor]].
  */
object ApiServiceAuthorization {

  private[this] val logger: Logger = LoggerFactory.getLogger(ApiServiceAuthorization.getClass)

  /** Checks whether the given claims give access to the request.
    *
    * @param check Given a set of [[Claims]], returns whether the current method is authorized.
    *              Typically, the caller compares the request parameters with the given claims.
    *
    * @return   A [[StatusRuntimeException]] with code `PERMISSION_DENIED` if the request is not authorized,
    *           or Unit if the request is authorized.
    */
  def requireClaims(check: Claims => Authorization): Either[StatusRuntimeException, Unit] = {
    val claims = AuthorizationInterceptor.contextKeyClaim.get()
    if (claims == null) {
      // The current context does not contain any claims.
      // Most likely causes:
      // - The API server does not use the [[AuthorizationInterceptor]] which is responsible for creating the context
      // - This function is called from a thread different from the one used to handle the gRPC call,
      //   such as during a `Future.map`. See also [[ApiServiceAuthorization.withContext]].
      Left(internal("Cannot retrieve claims from context."))
    } else {
      check(claims) match {
        case Authorized =>
          Right(())
        case NotAuthorized(reason) =>
          // Be careful with logging errors in order to not leak secrets through log messages
          // logger.warn(s"Rejecting authorization with claims $claims")
          Left(permissionDenied(reason))
      }
    }
  }

  /** Executes the given block with the given gRPC context as the current context.
    *
    * Usage:
    * ```
    * // Capture the gRPC context in a gRPC thread
    * val ctx = Context.current
    *
    * // Function bar uses ApiServiceAuthorization.requireClaims,
    * // but is called from a non-gRPC thread.
    * // Wrap it in withContext, so that the gRPC context is available in bar()
    * val asyncResult: Future[T] = foo()
    *   .flatMap(ApiServiceAuthorization.withContext(ctx)(bar))
    * ```
    */
  def withContext[T](context: Context)(block: => T): T = {
    val previous = context.attach()
    try block
    finally context.detach(previous)
  }

  def requirePublicClaims(): Either[StatusRuntimeException, Unit] = {
    requireClaims(claims => if (claims.isPublic) Authorized else NotAuthorized())
  }

  def requireAdminClaims(): Either[StatusRuntimeException, Unit] = {
    requireClaims(claims => if (claims.isAdmin) Authorized else NotAuthorized())
  }

  /** Checks whether the current Claims authorize to act as all parties of the given set.
    * Note: An empty set does NOT result in an authorization error.
    */
  def requireClaimsForAllParties(parties: Set[String]): Either[StatusRuntimeException, Unit] = {
    requireClaims(
      claims => if (parties.forall(p => claims.canActAs(p))) Authorized else NotAuthorized())
  }

  /** Checks whether the current Claims authorize to act as the given party, if any.
    * Note: An missing party does NOT result in an authorization error.
    */
  def requireClaimsForParty(party: Option[String]): Either[StatusRuntimeException, Unit] = {
    requireClaims(
      claims => if (party.forall(p => claims.canActAs(p))) Authorized else NotAuthorized())
  }

  /** Checks whether the current Claims authorize to read data for all parties mentioned in the given transaction filter */
  def requireClaimsForTransactionFilter(
      filter: Option[TransactionFilter]): Either[StatusRuntimeException, Unit] = {
    val filterPartyNames = filter
      .map(_.filtersByParty.keySet)
      .getOrElse(Set.empty)
    requireClaimsForAllParties(filterPartyNames)
  }
}
