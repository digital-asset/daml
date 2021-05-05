// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import io.grpc.Channel
import com.daml.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc
import com.daml.ledger.api.v1.ledger_identity_service.GetLedgerIdentityRequest
import org.slf4j.LoggerFactory

final class LedgerIdentityService(channel: Channel) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val service = LedgerIdentityServiceGrpc.blockingStub(channel)

  def fetchLedgerId(): String = {
    val response = service.getLedgerIdentity(
      new GetLedgerIdentityRequest()
    )
    logger.info(s"Fetched ledger ID: ${response.ledgerId}")
    response.ledgerId
  }

}
