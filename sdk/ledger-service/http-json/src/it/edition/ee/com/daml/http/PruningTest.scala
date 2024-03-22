// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

final class PruningTest
    extends AbstractHttpServiceIntegrationTestFunsCustomToken
    with HttpServicePostgresInt {

  override def staticContentConfig: Option[StaticContentConfig] = None
  override def useTls = HttpServiceTestFixture.UseTls.Tls
  override def wsConfig: Option[WebsocketConfig] = None

  import com.daml.ledger.api.v1.admin.{participant_pruning_service => PruneGrpc}
  "fail querying after pruned offset" in withHttpService { fixture =>
    import scala.concurrent.duration._
    import com.daml.timer.RetryStrategy
    for {
      (alice, aliceHeaders) <- fixture.getUniquePartyAndAuthHeaders("Alice")
      query = jsObject(s"""{"templateIds": ["Iou:Iou"]}""")

      // do query to populate cache
      _ <- searchExpectOk(List(), query, fixture, aliceHeaders)

      // perform more actions on the ledger
      (offsetBeforeArchive, offsetAfterArchive) <- offsetBeforeAfterArchival(
        alice,
        fixture,
        aliceHeaders,
      )

      // prune, at an offset that is later than the last cache refresh.
      _ <- RetryStrategy.constant(20, 1.second) { case (_, _) =>
        for {
          // Add artificial ledger activity to advance the safe prune offset. Repeated on failure.
          _ <- postCreateCommand(iouCreateCommand(alice), fixture, aliceHeaders)
          pruned <- PruneGrpc.ParticipantPruningServiceGrpc
            .stub(fixture.client.channel)
            .prune(
              PruneGrpc.PruneRequest(
                pruneUpTo = domain.Offset unwrap offsetAfterArchive,
                pruneAllDivulgedContracts = true,
              )
            )
        } yield pruned should ===(PruneGrpc.PruneResponse())
      }

      // now query again to ensure it handles PARTICIPANT_PRUNED_DATA_ACCESSED gracefully
      _ <- searchExpectOk(List(), query, fixture, aliceHeaders)
    } yield succeed
  }

}
