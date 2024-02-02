// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import java.util.Optional

import com.daml.ledger.javaapi.data._
import com.daml.ledger.api.v2.state_service.{ActiveContract, GetActiveContractsResponse}
import com.daml.ledger.api.v2.testing.time_service.GetTimeResponse
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse.ContractEntry
import com.google.protobuf.timestamp.Timestamp

import scala.jdk.CollectionConverters._

trait DataLayerHelpers {

  def ledgerServices: LedgerServices

  def genGetActiveContractsResponse: GetActiveContractsResponse = {
    new GetActiveContractsResponse(
      "",
      "workflowId",
      ContractEntry.ActiveContract(
        new ActiveContract(
          createdEvent = TransactionGenerator.createdEventGen.sample.map(_._1.value),
          domainId = "someDomain",
          reassignmentCounter = 0,
        )
      ),
    )
  }

  def genGetTimeResponse: GetTimeResponse = {
    new GetTimeResponse(Some(Timestamp(1L, 2)))
  }

  def genCommands(commands: List[Command], party: Option[String] = None): SubmitCommandsRequest = {
    new SubmitCommandsRequest(
      "workflowId",
      "applicationId",
      "commandId",
      party.getOrElse("party"),
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      commands.asJava,
    )
  }

  val filterNothing: FiltersByPartyV2 = new FiltersByPartyV2(Map[String, Filter]().asJava)

  def filterFor(party: String): FiltersByPartyV2 =
    new FiltersByPartyV2(
      Map(party -> (InclusiveFilter.ofTemplateIds(Set.empty[Identifier].asJava): Filter)).asJava
    )
}
