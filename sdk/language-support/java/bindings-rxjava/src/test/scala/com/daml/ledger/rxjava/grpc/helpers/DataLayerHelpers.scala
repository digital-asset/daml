// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import com.daml.ledger.javaapi.data._
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.command_completion_service.CompletionEndResponse
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.Value.Absolute
import com.daml.ledger.api.v1.testing.time_service.GetTimeResponse
import com.google.protobuf.timestamp.Timestamp

import scala.jdk.CollectionConverters._

trait DataLayerHelpers {

  def ledgerServices: LedgerServices

  def genGetActiveContractsResponse: GetActiveContractsResponse = {
    new GetActiveContractsResponse(
      "",
      "workflowId",
      Seq[CreatedEvent](),
    )
  }

  def genGetTimeResponse: GetTimeResponse = {
    new GetTimeResponse(Some(Timestamp(1L, 2)))
  }

  def genCommands(commands: List[Command], party: Option[String] = None): CommandsSubmission =
    CommandsSubmission
      .create("applicationId", "commandId", commands.asJava)
      .withActAs(party.getOrElse("party"))
      .withWorkflowId("workflowId")

  def genLedgerOffset(absVal: String): LedgerOffset =
    new LedgerOffset(Absolute(absVal))

  def genCompletionEndResponse(absVal: String): CompletionEndResponse =
    new CompletionEndResponse(Some(genLedgerOffset(absVal)))

  val filterNothing: FiltersByParty = new FiltersByParty(Map[String, Filter]().asJava)

  def filterFor(party: String): FiltersByParty =
    new FiltersByParty(
      Map(party -> (InclusiveFilter.ofTemplateIds(Set.empty[Identifier].asJava): Filter)).asJava
    )
}
