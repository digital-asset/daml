// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.identity

import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.ledger_identity_service.GetLedgerIdentityRequest
import com.digitalasset.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityServiceStub
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.configuration.LedgerIdRequirement

import scala.concurrent.{ExecutionContext, Future}

final class LedgerIdentityClient(service: LedgerIdentityServiceStub) {

  /**
    * The ledgerId in use, if the check was successful.
    */
  def satisfies(ledgerIdRequirement: LedgerIdRequirement, token: Option[String] = None)(
      implicit ec: ExecutionContext): Future[LedgerId] =
    for {
      ledgerId <- getLedgerId(token)
    } yield {
      val requirement = ledgerIdRequirement
      require(
        requirement.isAccepted(ledgerId),
        s"Required Ledger ID ${requirement.ledgerId} does not match received Ledger ID $ledgerId"
      )
      LedgerId(ledgerId)
    }

  def getLedgerId(token: Option[String] = None): Future[String] =
    LedgerClient
      .stub(service, token)
      .getLedgerIdentity(new GetLedgerIdentityRequest())
      .map(_.ledgerId)(DirectExecutionContext)

}
