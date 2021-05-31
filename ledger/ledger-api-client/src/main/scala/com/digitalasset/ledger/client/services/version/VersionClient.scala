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

class VersionClient(ledgerId: LedgerId, service: VersionServiceStub) {

  def getApiVersion(token: Option[String] = None): Future[String] =
    LedgerClient
      .stub(service, token)
      .getLedgerApiVersion(new GetLedgerApiVersionRequest(ledgerId.unwrap))
      .map(_.version)(DirectExecutionContext)

}
