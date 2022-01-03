// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import java.util.Optional

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
  def genLedgerOffset(absVal: String): LedgerOffset =
    new LedgerOffset(Absolute(absVal))

  def genCompletionEndResponse(absVal: String): CompletionEndResponse =
    new CompletionEndResponse(Some(genLedgerOffset(absVal)))

  val filterNothing: FiltersByParty = new FiltersByParty(Map[String, Filter]().asJava)

  def filterFor(party: String): FiltersByParty =
    new FiltersByParty(
      Map(party -> new InclusiveFilter(Set.empty[Identifier].asJava).asInstanceOf[Filter]).asJava
    )
}
