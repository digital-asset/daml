// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.identity

import com.daml.ledger.api.v1.ledger_identity_service.GetLedgerIdentityRequest
import com.daml.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityServiceStub
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.configuration.LedgerIdRequirement

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

final class LedgerIdentityClient(service: LedgerIdentityServiceStub) {

  /** The ledgerId in use, if the check was successful.
    */
  def satisfies(
      ledgerIdRequirement: LedgerIdRequirement,
      token: Option[String] = None,
  )(implicit executionContext: ExecutionContext): Future[LedgerId] =
    for {
      ledgerId <- getLedgerId(token)
    } yield {
      val printable = ledgerIdRequirement.optionalLedgerId.getOrElse("<empty-ledger-id>")
      require(
        ledgerIdRequirement.isAccepted(ledgerId),
        s"Received Ledger ID $ledgerId does not match the expected value $printable",
      )
      LedgerId(ledgerId)
    }

  def getLedgerId(
      token: Option[String] = None
  )(implicit executionContext: ExecutionContext): Future[String] =
    LedgerClient
      .stub(service, token)
      .getLedgerIdentity(new GetLedgerIdentityRequest())
      .map(_.ledgerId): @nowarn(
      "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*"
    )

}
