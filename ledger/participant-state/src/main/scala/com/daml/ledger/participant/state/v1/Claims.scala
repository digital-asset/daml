// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import com.digitalasset.daml.lf.data.Ref

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

/**
  * [[Claims]] define what actions an authenticated user can perform on the ledger API.
  *
  *
  * The following is a full list of services and the corresponding required claims:
  * +-------------------------------------+----------------------------+------------------------------------------+
  * | Ledger API service                  | Method                     | Access with                              |
  * +-------------------------------------+----------------------------+------------------------------------------+
  * | LedgerIdentityService               | GetLedgerIdentity          | isPublic                                 |
  * | ActiveContractsService              | GetActiveContracts         | for each requested party p: canActAs(p)  |
  * | CommandSubmissionService            | Submit                     | for submitting party p: canActAs(p)      |
  * | CommandCompletionService            | CompletionEnd              | isPublic                                 |
  * | CommandCompletionService            | CompletionStream           | for each requested party p: canActAs(p)  |
  * | CommandService                      | *                          | for submitting party p: canActAs(p)      |
  * | LedgerConfigurationService          | GetLedgerConfiguration     | isPublic                                 |
  * | PackageService                      | *                          | isPublic                                 |
  * | PackageManagementService            | *                          | isAdmin                                  |
  * | PartyManagementService              | *                          | isAdmin                                  |
  * | ResetService                        | *                          | isAdmin                                  |
  * | TimeService                         | *                          | ???                                      |
  * | TransactionService                  | LedgerEnd                  | isPublic                                 |
  * | TransactionService                  | *                          | for each requested party p: canActAs(p)  |
  * +-------------------------------------+----------------------------+------------------------------------------+
  */
case class Claims(claims: Seq[Claim]) {

  /** Returns true if the set of claims authorizes the user to use admin services */
  def isAdmin: Boolean = claims.exists {
    case ClaimAdmin => true
    case _ => false
  }

  /** Returns true if the set of claims authorizes the user to use public services */
  def isPublic: Boolean = claims.exists {
    case ClaimPublic => true
    case _ => false
  }

  /** Returns true if the set of claims authorizes the user to act as the given party */
  def canActAs(party: String): Boolean = claims.exists {
    case ClaimActAsAnyParty => true
    case ClaimActAsParty(p) => p == party
    case _ => false
  }
}

object Claims {

  /** A set of [[Claims]] that does not have any authorization */
  def empty: Claims = Claims(List.empty[Claim])

  /** A set of [[Claims]] that has all possible authorizations */
  def wildcard: Claims = Claims(List[Claim](ClaimPublic, ClaimAdmin, ClaimActAsAnyParty))
}
