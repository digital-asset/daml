// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import java.time.Instant

import com.daml.ledger.javaapi.data._
import com.digitalasset.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.ledger.api.v1.command_completion_service.CompletionEndResponse
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.Value.Absolute
import com.digitalasset.ledger.api.v1.testing.time_service.GetTimeResponse
import com.google.protobuf.timestamp.Timestamp

import scala.collection.JavaConverters._

trait DataLayerHelpers {

  def ledgerServices: LedgerServices

  def genGetActiveContractsResponse: GetActiveContractsResponse = {
    new GetActiveContractsResponse(
      "",
      "workflowId",
      Seq[CreatedEvent](),
      None
    )
  }

  def genGetTimeResponse: GetTimeResponse = {
    new GetTimeResponse(Some(Timestamp(1l, 2)))
  }

  def genCommands(commands: List[Command]): SubmitCommandsRequest = {
    new SubmitCommandsRequest(
      "workflowId",
      "applicationId",
      "commandId",
      "party",
      Instant.EPOCH,
      Instant.EPOCH,
      commands.asJava)
  }
  def genLedgerOffset(absVal: String): LedgerOffset =
    new LedgerOffset(Absolute(absVal))

  def genCompletionEndResponse(absVal: String): CompletionEndResponse =
    new CompletionEndResponse(Some(genLedgerOffset(absVal)))

  val filterNothing: FiltersByParty = new FiltersByParty(Map[String, Filter]().asJava)
}
