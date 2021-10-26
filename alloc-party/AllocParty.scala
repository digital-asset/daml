// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import com.daml.lf.data.Ref
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.domain
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ledger.client.LedgerClient
import com.daml.platform.sandboxnext.SandboxNextFixture
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.BeforeAndAfterEach
import org.scalatest.freespec.AsyncFreeSpec
import scala.concurrent.Future
import scalaz.syntax.tag._

final class AllocParty
    extends AsyncFreeSpec
    with AkkaBeforeAndAfterAll
    with BeforeAndAfterEach
    with SuiteResourceManagementAroundAll
    with SandboxNextFixture
    with StrictLogging {

  private val appId = domain.ApplicationId("alloc-party")
  private val clientConfiguration = LedgerClientConfiguration(
    applicationId = appId.unwrap,
    ledgerIdRequirement = LedgerIdRequirement.none,
    commandClient = CommandClientConfiguration.default,
    sslContext = None,
    token = None,
  )

  override def config = super.config.copy(damlPackages = Nil)

  private def allocateN(client: LedgerClient, i: Int) =
    for {
      ps <- List
        .range(0, i)
        // Allocate parties sequentially to avoid timeouts on CI.
        .foldLeft[Future[List[Ref.Party]]](Future.successful(Nil)) { case (acc, i) =>
          for {
            ps <- acc
            _ = logger.info(s"Allocating party $i")
            p <- client.partyManagementClient.allocateParty(None, None).map(_.party)
            _ = logger.info(s"Allocated party $i")
          } yield ps :+ p
        }
    } yield ps
  "abc" in {
    for {
      client <- LedgerClient(channel, clientConfiguration)
      _ <- allocateN(client, 1000)
    } yield succeed
  }
}
