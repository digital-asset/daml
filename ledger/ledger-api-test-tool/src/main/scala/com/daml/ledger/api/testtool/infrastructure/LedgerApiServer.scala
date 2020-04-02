// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionService

object LedgerApiServer {

  /** temporary work-around to get access to the transaction and command completion service
    * while we haven't merged the test into the upstream code
    */
  def getServices(
      participant: ParticipantTestContext): (CommandCompletionService, TransactionService) = {
    val servicesField = participant.getClass.getDeclaredField("services")
    servicesField.setAccessible(true)
    val actualExecutor = servicesField.get(participant)
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    val ls = actualExecutor.asInstanceOf[LedgerServices]
    (ls.commandCompletion, ls.transaction)
  }

}
