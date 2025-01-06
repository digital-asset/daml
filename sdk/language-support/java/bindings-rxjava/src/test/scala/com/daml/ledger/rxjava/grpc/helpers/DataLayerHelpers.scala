// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import com.daml.ledger.javaapi.data._
import com.daml.ledger.api.v2.state_service.{ActiveContract, GetActiveContractsResponse}
import com.daml.ledger.api.v2.testing.time_service.GetTimeResponse
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse.ContractEntry
import com.google.protobuf.timestamp.Timestamp

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

trait DataLayerHelpers {

  def ledgerServices: LedgerServices

  def genGetActiveContractsResponse: GetActiveContractsResponse = {
    new GetActiveContractsResponse(
      "workflowId",
      ContractEntry.ActiveContract(
        new ActiveContract(
          createdEvent = TransactionGenerator.createdEventGen.sample.map(_._1.value),
          synchronizerId = "someSynchronizer",
          reassignmentCounter = 0,
        )
      ),
    )
  }

  def genGetTimeResponse: GetTimeResponse = {
    new GetTimeResponse(Some(Timestamp(1L, 2)))
  }

  def genCommands(
      commands: List[Command],
      synchronizerId: Option[String] = None,
  ): CommandsSubmission = {
    CommandsSubmission
      .create(
        "applicationId",
        "commandId",
        synchronizerId.getOrElse("synchronizerId"),
        commands.asJava,
      )
      .withWorkflowId("workflowId")
  }

  val filterNothing: TransactionFilter =
    new TransactionFilter(Map[String, Filter]().asJava, None.toJava)

  def filterFor(party: String): TransactionFilter =
    new TransactionFilter(
      Map(
        party -> (new CumulativeFilter(
          Map.empty.asJava,
          Map.empty.asJava,
          Some(Filter.Wildcard.HIDE_CREATED_EVENT_BLOB).toJava,
        ): Filter)
      ).asJava,
      None.toJava,
    )
}
