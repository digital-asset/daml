// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.fixture

import com.daml.ledger.api.auth.client.LedgerCallCredentials
import com.daml.ledger.api.v1.admin.party_management_service.{
  AllocatePartyRequest,
  PartyManagementServiceGrpc,
}

import java.util.concurrent.Executors

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

trait CreatesParties {
  self: SandboxFixture =>

  protected def createParty(token: Option[String])(
      partyId: String
  )(implicit executionContext: ExecutionContext): Future[String] = {
    val req = AllocatePartyRequest(partyId)
    val stub = PartyManagementServiceGrpc.stub(channel)
    token
      .fold(stub)(LedgerCallCredentials.authenticatingStub(stub, _))
      .allocateParty(req)
      .map(_.getPartyDetails.party)
  }

  def createPrerequisiteParties(token: Option[String], prerequisiteParties: List[String]): Unit = {
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(
      Executors.newFixedThreadPool(2)
    )
    val _ =
      Await.result(
        Future.sequence(prerequisiteParties.map(createParty(token))),
        10.seconds,
      )
  }

}
