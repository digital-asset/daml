// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.http.scaladsl.model.Uri
import com.daml.bazeltools.BazelRunfiles
import com.daml.http.json.{DomainJsonDecoder, DomainJsonEncoder}
import com.daml.ledger.client.LedgerClient
import org.scalatest.Suite

import scala.concurrent.{ExecutionContext, Future}

trait HttpFailureTestFixture extends ToxicSandboxFixture { self: Suite =>

  private implicit val ec: ExecutionContext = system.dispatcher

  override def packageFiles =
    List(
      BazelRunfiles.requiredResource("docs/quickstart-model.dar"),
      BazelRunfiles.requiredResource("ledger-service/http-json/Account.dar"))

  protected def allocateParty(client: LedgerClient, displayName: String): Future[domain.Party] =
    client.partyManagementClient
      .allocateParty(None, Some(displayName), None)
      .map(p => domain.Party(p.party))

  protected def withHttpService[A]
    : ((Uri, DomainJsonEncoder, DomainJsonDecoder, LedgerClient) => Future[A]) => Future[A] =
    HttpServiceTestFixture.withHttpService(
      this.getClass.getSimpleName,
      proxiedPort,
      None,
      None,
      wsConfig = Some(Config.DefaultWsConfig))
}
