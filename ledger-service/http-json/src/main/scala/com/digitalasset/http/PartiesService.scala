// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.ledger.api.domain.PartyDetails
import scalaz.syntax.equal._
import scalaz.std.string._

import scala.collection.breakOut
import scala.concurrent.{ExecutionContext, Future}

class PartiesService(listAllParties: () => Future[List[PartyDetails]])(
    implicit ec: ExecutionContext) {

  def allParties(): Future[List[domain.PartyDetails]] = {
    listAllParties().map(ps => ps.map(domain.PartyDetails.fromLedgerApi))
  }

  // TODO(Leo) memoize it
  def resolveParty(identifier: String): Future[Option[domain.PartyDetails]] =
    listAllParties().map {
      _.find(identifier === _.party).map(domain.PartyDetails.fromLedgerApi)
    }
}

object PartiesService {
  type ResolveParty = String => Option[domain.PartyDetails]

  type PartyMap = Map[domain.Party, domain.PartyDetails]

  def buildPartiesMap(parties: List[domain.PartyDetails]): PartyMap =
    parties.map(x => (x.identifier, x))(breakOut)
}
