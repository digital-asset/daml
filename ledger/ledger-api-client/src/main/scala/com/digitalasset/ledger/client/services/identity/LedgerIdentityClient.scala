// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.identity

import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.ledger_identity_service.GetLedgerIdentityRequest
import com.digitalasset.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityService
import com.digitalasset.ledger.client.configuration.LedgerIdRequirement

import scala.concurrent.{ExecutionContext, Future}

final class LedgerIdentityClient(ledgerIdentityService: LedgerIdentityService) {

  /**
    * The ledgerId in use, if the check was successful.
    */
  def satisfies(ledgerIdRequirement: LedgerIdRequirement)(
      implicit ec: ExecutionContext): Future[LedgerId] =
    for {
      ledgerId <- getLedgerId()
    } yield {
      val requirement = ledgerIdRequirement
      require(
        requirement.isAccepted(ledgerId),
        s"Required Ledger ID ${requirement.ledgerId} does not match received Ledger ID $ledgerId"
      )
      LedgerId(ledgerId)
    }

  def getLedgerId(): Future[String] =
    ledgerIdentityService
      .getLedgerIdentity(new GetLedgerIdentityRequest())
      .map(_.ledgerId)(DirectExecutionContext)

}
