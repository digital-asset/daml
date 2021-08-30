// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.version

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.version_service.GetLedgerApiVersionRequest
import com.daml.ledger.api.v1.version_service.VersionServiceGrpc.VersionServiceStub
import com.daml.ledger.client.LedgerClient
import scalaz.syntax.tag._

import scala.concurrent.Future

sealed class VersionClientWithoutLedgerId(service: VersionServiceStub) {

  def getApiVersion(
      ledgerIdToUse: LedgerId,
      token: Option[String] = None,
  ): Future[String] =
    LedgerClient
      .stub(service, token)
      .getLedgerApiVersion(
        new GetLedgerApiVersionRequest(ledgerIdToUse.unwrap)
      )
      .map(_.version)(DirectExecutionContext)

}

final class VersionClient(ledgerId: LedgerId, service: VersionServiceStub)
    extends VersionClientWithoutLedgerId(service) {

  override def getApiVersion(
      ledgerIdToUse: LedgerId = ledgerId,
      token: Option[String] = None,
  ): Future[String] = super.getApiVersion(ledgerId, token)

}
