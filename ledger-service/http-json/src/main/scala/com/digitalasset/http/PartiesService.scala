// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api
import scalaz.OneAnd

import scala.collection.breakOut
import scala.concurrent.{ExecutionContext, Future}

class PartiesService(listAllParties: LedgerClientJwt.ListKnownParties)(
    implicit ec: ExecutionContext) {

  // TODO(Leo) memoize this calls or listAllParties()?
  def allParties(jwt: Jwt): Future[List[domain.PartyDetails]] = {
    listAllParties(jwt).map(ps => ps.map(p => domain.PartyDetails.fromLedgerApi(p)))
  }

  // TODO(Leo) memoize this calls or listAllParties()?
  def parties(
      jwt: Jwt,
      identifiers: OneAnd[Set, domain.Party]
  ): Future[(Set[domain.PartyDetails], Set[domain.Party])] = {
    val requested: Set[domain.Party] = identifiers.tail + identifiers.head
    val strIds: Set[String] = domain.Party.unsubst(requested)

    listAllParties(jwt).map { ps =>
      val result: Set[domain.PartyDetails] = collectParties(ps, strIds)
      (result, findUnknownParties(result, requested))
    }
  }

  private def collectParties(
      xs: List[api.domain.PartyDetails],
      requested: Set[String]
  ): Set[domain.PartyDetails] =
    xs.collect {
      case p if requested(p.party) => domain.PartyDetails.fromLedgerApi(p)
    }(breakOut)

  private def findUnknownParties(
      found: Set[domain.PartyDetails],
      requested: Set[domain.Party]
  ): Set[domain.Party] =
    if (found.size == requested.size) Set.empty[domain.Party]
    else requested -- found.map(_.identifier)

}
