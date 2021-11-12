// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.transaction.TreeEvent
import com.daml.platform.sandboxnext.SandboxNextFixture
import com.daml.platform.sandbox.services.TestCommands
import java.util.UUID
import org.scalatest.matchers.should.Matchers
import org.scalatest.freespec.AsyncFreeSpec
import scala.concurrent.Future

final class SubmitSync
    extends AsyncFreeSpec
    with SandboxNextFixture
    with Matchers
    with SuiteResourceManagementAroundAll
    with TestCommands {

  private val clientConfig = LedgerClientConfiguration(
    applicationId = "myappid",
    ledgerIdRequirement = LedgerIdRequirement.none,
    commandClient = CommandClientConfiguration.default,
    sslContext = None,
    token = None,
  )

  private def tries(n: Int, client: LedgerClient): Future[Unit] =
    if (n <= 0) {
      Future.successful(())
    } else {
      for {
        resp <- client.commandServiceClient.submitAndWaitForTransactionTree(
          SubmitAndWaitRequest(
            buildRequest(
              ledgerId = client.ledgerId,
              commandId = UUID.randomUUID.toString,
              commands = Seq(createWithOperator(templateIds.dummy, "Alice")),
              party = "Alice",
            ).commands
          )
        )
        cid = resp.getTransaction.eventsById.values.head.kind
          .asInstanceOf[TreeEvent.Kind.Created]
          .created
          .get
          .contractId
        _ <- client.commandServiceClient.submitAndWaitForTransactionTree(
          SubmitAndWaitRequest(
            buildRequest(
              ledgerId = client.ledgerId,
              commandId = UUID.randomUUID.toString,
              commands = Seq(exerciseWithUnit(templateIds.dummy, cid, "Archive")),
              party = "Alice",
            ).commands
          )
        )
        r <- tries(n - 1, client)
      } yield r
    }

  "SubmitSync" in {
    for {
      client <- LedgerClient(channel, clientConfig)
      _ <- tries(10000, client)
    } yield succeed
  }
}
