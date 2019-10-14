package com.digitalasset.http

import com.digitalasset.ledger.api.domain.PartyDetails

import scala.concurrent.{ExecutionContext, Future}

class PartiesService(listAllParties: () => Future[List[PartyDetails]])(
    implicit ec: ExecutionContext) {

  def allParties(): Future[List[domain.PartyDetails]] = {
    val f = listAllParties().map(ps => ps.map(p => domain.PartyDetails.fromLedgerApi(p)))
    f.onComplete { xs =>
      println(s"!!!!!xs: $xs")
    }
    f
  }
}
