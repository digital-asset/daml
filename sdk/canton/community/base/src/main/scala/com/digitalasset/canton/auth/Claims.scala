// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.daml.jwt.JwtTimestampLeeway
import com.digitalasset.canton.LfLedgerString
import com.digitalasset.daml.lf.data.Ref

import java.time.{Duration, Instant}

/** A claim is a single statement about what an authenticated user can do with the ledger API.
  *
  * Note: this ADT is expected to evolve in the future by adding new cases for more fine grained claims.
  * The existing cases should be treated as immutable in order to guarantee backwards compatibility for
  * [[AuthService]] implementations.
  */
sealed abstract class Claim

/** Authorized to use all admin services.
  * Does not authorize to use non-admin services.
  */
case object ClaimAdmin extends Claim

/** Authorized to use admin services for the configured identity provider.
  * Does not authorize to use non-admin services.
  */
case object ClaimIdentityProviderAdmin extends Claim

/** Authorized to use all "public" services, i.e.,
  * those that do not require admin rights and do not depend on any Daml party.
  * Examples include the VersionService or the PackageService.
  */
case object ClaimPublic extends Claim

/** Authorized to act as any party, including:
  * - Reading all data for all parties
  * - Creating contract on behalf of any party
  * - Exercising choices on behalf of any party
  */
case object ClaimActAsAnyParty extends Claim

/** Authorized to act as the given party, including:
  * - Reading all data for the given party
  * - Creating contracts on behalf of the given party
  * - Exercising choices on behalf of the given party
  */
final case class ClaimActAsParty(name: Ref.Party) extends Claim

/** Authorized to read all data for the given party.
  *
  * Does NOT authorize to issue commands.
  */
final case class ClaimReadAsParty(name: Ref.Party) extends Claim

/** Authorized to read all data as any party on the participant.
  *
  * Does NOT authorize to issue commands.
  */
case object ClaimReadAsAnyParty extends Claim

sealed trait ClaimSet

object ClaimSet {

  val DefaultIdentityProviderId: String = ""

  object Unauthenticated extends ClaimSet

  /** [[Claims]] define what actions an authenticated user can perform on the Ledger API.
    *
    * They also optionally specify an expiration epoch time that statically specifies the
    * time on or after which the token will no longer be considered valid by the Ledger API.
    *
    * The precise authorization rules are documented in "//docs/source/app-dev/authorization.rst".
    * Please use that file when writing or reviewing tests; and keep it up to date when adding new endpoints.
    *
    * @param claims         List of [[Claim]]s describing the authorization this object describes.
    * @param participantId  If set, the claims will only be valid on the given participant identifier.
    * @param applicationId  If set, the claims will only be valid on the given application identifier.
    * @param expiration     If set, the claims will cease to be valid at the given time.
    * @param resolvedFromUser  If set, then the claims were resolved from a user in the user management service.
    * @param identityProviderId  If set, the claims will only be valid on the given Identity Provider configuration.
    */
  final case class Claims(
      claims: Seq[Claim],
      participantId: Option[String],
      applicationId: Option[String],
      expiration: Option[Instant],
      identityProviderId: Option[LfLedgerString],
      resolvedFromUser: Boolean,
  ) extends ClaimSet {

    def validForParticipant(id: String): Either[AuthorizationError, Unit] =
      participantId match {
        case Some(p) if p != id => Left(AuthorizationError.InvalidParticipant(p, id))
        case _ => Right(())
      }

    def validForApplication(id: String): Either[AuthorizationError, Unit] =
      applicationId match {
        case Some(a) if a != id => Left(AuthorizationError.InvalidApplication(a, id))
        case _ => Right(())
      }

    /** Returns false if the expiration timestamp exists and is greater than or equal to the current time */
    def notExpired(
        now: Instant,
        jwtTimestampLeeway: Option[JwtTimestampLeeway],
        tokenExpiryGracePeriodForStreams: Option[Duration] = None,
    ): Either[AuthorizationError, Unit] = {
      val subtrahends =
        jwtTimestampLeeway
          .flatMap(l => l.expiresAt.orElse(l.default))
          .map(Duration.ofSeconds)
          .toList ++
          tokenExpiryGracePeriodForStreams.toList
      val relaxedNow = subtrahends.foldLeft(now)((acc, subtrahend) => acc.minus(subtrahend))
      expiration match {
        case Some(e) if !relaxedNow.isBefore(e) => Left(AuthorizationError.Expired(e, now))
        case _ => Right(())
      }
    }

    /** Returns true if the set of claims authorizes the user to use admin services, unless the claims expired */
    def isAdmin: Either[AuthorizationError, Unit] =
      Either.cond(claims.contains(ClaimAdmin), (), AuthorizationError.MissingAdminClaim)

    /** Returns true if the set of claims authorizes the user as an administrator or
      * an identity provider administrator, unless the claims expired
      */
    def isAdminOrIDPAdmin: Either[AuthorizationError, Unit] =
      Either.cond(
        claims.contains(ClaimIdentityProviderAdmin) || claims.contains(ClaimAdmin),
        (),
        AuthorizationError.MissingAdminClaim,
      )

    /** Returns true if the set of claims authorizes the user to use public services, unless the claims expired */
    def isPublic: Either[AuthorizationError, Unit] =
      Either.cond(claims.contains(ClaimPublic), (), AuthorizationError.MissingPublicClaim)

    /** Returns true if the set of claims authorizes the user to act as the given party, unless the claims expired */
    def canActAs(party: String): Either[AuthorizationError, Unit] =
      Either.cond(
        claims.exists {
          case ClaimActAsAnyParty => true
          case ClaimActAsParty(p) if p == party => true
          case _ => false
        },
        (),
        AuthorizationError.MissingActClaim(party),
      )

    /** Returns true if the set of claims authorizes the user to read data for the given party, unless the claims expired */
    def canReadAs(party: String): Either[AuthorizationError, Unit] =
      Either.cond(
        claims.exists {
          case ClaimActAsAnyParty => true
          case ClaimReadAsAnyParty => true
          case ClaimActAsParty(p) if p == party => true
          case ClaimReadAsParty(p) if p == party => true
          case _ => false
        },
        (),
        AuthorizationError.MissingReadClaim(party),
      )

    /** Returns true if the set of claims authorizes the user to read data as any party, unless the claims expired */
    def canReadAsAnyParty: Either[AuthorizationError, Unit] =
      Either.cond(
        claims.exists {
          case ClaimActAsAnyParty => true
          case ClaimReadAsAnyParty => true
          case _ => false
        },
        (),
        AuthorizationError.MissingReadAsAnyPartyClaim,
      )
  }

  /** The representation of a user that was authenticated, but whose [[Claims]] have not yet been resolved. */
  final case class AuthenticatedUser(
      identityProviderId: Option[LfLedgerString],
      userId: String,
      participantId: Option[String],
      expiration: Option[Instant],
  ) extends ClaimSet

  object Claims {

    /** A set of [[Claims]] that does not have any authorization */
    val Empty: Claims = Claims(
      claims = List.empty[Claim],
      participantId = None,
      applicationId = None,
      expiration = None,
      resolvedFromUser = false,
      identityProviderId = None,
    )

    /** A set of [[Claims]] that has all possible authorizations */
    val Wildcard: Claims =
      Empty.copy(claims = List[Claim](ClaimPublic, ClaimAdmin, ClaimActAsAnyParty))

    /** A set of [[Claims]] that has all admin authorizations but doesn't authorize to act as parties */
    val Admin: Claims =
      Empty.copy(claims = List[Claim](ClaimPublic, ClaimAdmin))

  }

}
