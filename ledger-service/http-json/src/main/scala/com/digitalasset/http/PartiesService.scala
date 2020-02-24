// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.ledger.api.domain.PartyDetails

import scala.concurrent.{ExecutionContext, Future}
import com.digitalasset.jwt.domain.Jwt
import scalaz.OneAnd

class PartiesService(listAllParties: Jwt => Future[List[PartyDetails]])(
    implicit ec: ExecutionContext) {

  // TODO(Leo) memoize this calls or listAllParties()?
  def allParties(jwt: Jwt): Future[List[domain.PartyDetails]] = {
    listAllParties(jwt).map(ps => ps.map(p => domain.PartyDetails.fromLedgerApi(p)))
  }

  // TODO(Leo) memoize this calls or listAllParties()?
  def parties(
      jwt: Jwt,
      identifiers: OneAnd[Set, domain.Party]): Future[List[domain.PartyDetails]] = {
    val ids: Set[String] = domain.Party.unsubst(identifiers.tail + identifiers.head)
    listAllParties(jwt).map { ps =>
      ps.collect { case p if ids(p.party) => domain.PartyDetails.fromLedgerApi(p) }
    }
  }
}
