// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.version

import com.daml.ledger.api.v1.version_service.VersionServiceGrpc.VersionServiceStub
import com.digitalasset.canton.ledger.api.domain.LedgerId

import scala.concurrent.{ExecutionContext, Future}

final class VersionClient(ledgerId: LedgerId, service: VersionServiceStub) {
  private val it = new withoutledgerid.VersionClient(service)

  def getApiVersion(
      token: Option[String] = None
  )(implicit executionContext: ExecutionContext): Future[String] =
    it.getApiVersion(ledgerId, token)

}
