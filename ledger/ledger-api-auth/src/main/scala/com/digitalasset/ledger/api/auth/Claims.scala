// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import java.time.Instant

import com.daml.lf.data.Ref

/**
  * A claim is a single statement about what an authenticated user can do with the ledger API.
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

/** Authorized to use all "public" services, i.e.,
  * those that do not require admin rights and do not depend on any DAML party.
  * Examples include the LedgerIdentityService or the PackageService.
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

/**
  * [[Claims]] define what actions an authenticated user can perform on the Ledger API.
  *
  * They also optionally specify an expiration epoch time that statically specifies the
  * time on or after which the token will no longer be considered valid by the Ledger API.
  *
  * The following is a full list of services and the corresponding required claims:
  * +-------------------------------------+----------------------------+------------------------------------------+
  * | Ledger API service                  | Method                     | Access with                              |
  * +-------------------------------------+----------------------------+------------------------------------------+
  * | LedgerIdentityService               | GetLedgerIdentity          | isPublic                                 |
  * | ActiveContractsService              | GetActiveContracts         | for each requested party p: canReadAs(p) |
  * | CommandSubmissionService            | Submit                     | for submitting party p: canActAs(p)      |
  * | CommandCompletionService            | CompletionEnd              | isPublic                                 |
  * | CommandCompletionService            | CompletionStream           | for each requested party p: canReadAs(p) |
  * | CommandService                      | *                          | for submitting party p: canActAs(p)      |
  * | LedgerConfigurationService          | GetLedgerConfiguration     | isPublic                                 |
  * | PackageService                      | *                          | isPublic                                 |
  * | PackageManagementService            | *                          | isAdmin                                  |
  * | PartyManagementService              | *                          | isAdmin                                  |
  * | ResetService                        | *                          | isAdmin                                  |
  * | TimeService                         | GetTime                    | isPublic                                 |
  * | TimeService                         | SetTime                    | isAdmin                                  |
  * | TransactionService                  | LedgerEnd                  | isPublic                                 |
  * | TransactionService                  | *                          | for each requested party p: canReadAs(p) |
  * +-------------------------------------+----------------------------+------------------------------------------+
  *
  * @param claims         List of [[Claim]]s describing the authorization this object describes.
  * @param ledgerId       If set, the claims will only be valid on the given ledger.
  * @param participantId  If set, the claims will only be valid on the given participant.
  * @param expiration     If set, the claims will cease to be valid at the given time.
  */
final case class Claims(
    claims: Seq[Claim],
    ledgerId: Option[String] = None,
    participantId: Option[String] = None,
    expiration: Option[Instant] = None,
) {
  def validForLedger(id: String): Boolean =
    ledgerId.forall(_ == id)

  def validForParticipant(id: String): Boolean =
    participantId.forall(_ == id)

  /** Returns false if the expiration timestamp exists and is greather than or equal to the current time */
  def notExpired(now: Instant): Boolean =
    expiration.forall(now.isBefore)

  /** Returns true if the set of claims authorizes the user to use admin services, unless the claims expired */
  def isAdmin: Boolean =
    claims.contains(ClaimAdmin)

  /** Returns true if the set of claims authorizes the user to use public services, unless the claims expired */
  def isPublic: Boolean =
    claims.contains(ClaimPublic)

  /** Returns true if the set of claims authorizes the user to act as the given party, unless the claims expired */
  def canActAs(party: String): Boolean = {
    claims.exists {
      case ClaimActAsAnyParty => true
      case ClaimActAsParty(p) if p == party => true
      case _ => false
    }
  }

  /** Returns true if the set of claims authorizes the user to read data for the given party, unless the claims expired */
  def canReadAs(party: String): Boolean = {
    claims.exists {
      case ClaimActAsAnyParty => true
      case ClaimActAsParty(p) if p == party => true
      case ClaimReadAsParty(p) if p == party => true
      case _ => false
    }
  }
}

object Claims {

  /** A set of [[Claims]] that does not have any authorization */
  val empty = Claims(List.empty[Claim], expiration = None, ledgerId = None, participantId = None)

  /** A set of [[Claims]] that has all possible authorizations */
  val wildcard = Claims(
    List[Claim](ClaimPublic, ClaimAdmin, ClaimActAsAnyParty),
    expiration = None,
    ledgerId = None,
    participantId = None)

}
