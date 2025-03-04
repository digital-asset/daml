// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import com.daml.ledger.api.v2.transaction_filter.{
  EventFormat,
  TransactionFilter,
  TransactionFormat,
  UpdateFormat,
}
import com.digitalasset.canton.auth.RequiredClaim
import scalapb.lenses.Lens

object RequiredClaims {

  def apply[Req](claims: RequiredClaim[Req]*): List[RequiredClaim[Req]] = claims.toList

  def submissionClaims[Req](
      actAs: Set[String],
      readAs: Set[String],
      applicationIdL: Lens[Req, String],
  ): List[RequiredClaim[Req]] =
    RequiredClaim.MatchApplicationId(applicationIdL)
      :: actAs.view.map(RequiredClaim.ActAs[Req]).toList
      ::: readAs.view.map(RequiredClaim.ReadAs[Req]).toList

  def readAsForAllParties[Req](parties: Iterable[String]): List[RequiredClaim[Req]] =
    parties.view.map(RequiredClaim.ReadAs[Req]).toList

  def transactionFormatClaims[Req](transactionFormat: TransactionFormat): List[RequiredClaim[Req]] =
    transactionFormat.eventFormat.toList.flatMap(RequiredClaims.eventFormatClaims[Req])

  def eventFormatClaims[Req](eventFormat: EventFormat): List[RequiredClaim[Req]] =
    readAsForAllParties[Req](eventFormat.filtersByParty.keys)
      ::: eventFormat.filtersForAnyParty.map(_ => RequiredClaim.ReadAsAnyParty[Req]()).toList

  def updateFormatClaims[Req](updateFormat: UpdateFormat): List[RequiredClaim[Req]] =
    List(
      updateFormat.includeTransactions.toList
        .flatMap(transactionFormatClaims[Req]),
      updateFormat.includeReassignments.toList
        .flatMap(eventFormatClaims[Req]),
      updateFormat.includeTopologyEvents
        .flatMap(_.includeParticipantAuthorizationEvents)
        .toList
        .map(_.parties)
        .flatMap {
          case empty if empty.isEmpty => List(RequiredClaim.ReadAsAnyParty[Req]())
          case nonEmpty => readAsForAllParties[Req](nonEmpty)
        },
    ).flatten.distinct

  def transactionFilterClaims[Req](transactionFilter: TransactionFilter): List[RequiredClaim[Req]] =
    readAsForAllParties[Req](transactionFilter.filtersByParty.keys)
      ::: transactionFilter.filtersForAnyParty.map(_ => RequiredClaim.ReadAsAnyParty[Req]()).toList

  def idpAdminClaimsAndMatchingRequestIdpId[Req](
      identityProviderIdL: Lens[Req, String],
      mustBeParticipantAdmin: Boolean = false,
  ): List[RequiredClaim[Req]] = RequiredClaims(
    if (mustBeParticipantAdmin) RequiredClaim.Admin[Req]()
    else RequiredClaim.AdminOrIdpAdmin[Req](),
    RequiredClaim.MatchIdentityProviderId(identityProviderIdL),
  )
}
