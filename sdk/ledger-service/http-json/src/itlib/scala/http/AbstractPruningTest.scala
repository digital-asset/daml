// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

abstract class AbstractPruningTest extends AbstractHttpServiceIntegrationTestFunsCustomToken {

  override def staticContentConfig: Option[StaticContentConfig] = None
  override def useTls = HttpServiceTestFixture.UseTls.Tls
  override def wsConfig: Option[WebsocketConfig] = None

  import AbstractHttpServiceIntegrationTestFuns.TpId
  import com.daml.ledger.api.v1.admin.{participant_pruning_service => PruneGrpc}
  "fail querying after pruned offset" in withHttpService { fixture =>
    import scala.concurrent.duration._
    import com.daml.timer.RetryStrategy
    for {
      (alice, aliceHeaders) <- fixture.getUniquePartyAndAuthHeaders("Alice")
      query = jsObject(s"""{"templateIds": ["${tidString(TpId.Iou.Iou)}"]}""")

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

  "query for visible contract, which was created and archived by another party before your first query" in withHttpService {
    fixture =>
      import HttpServiceTestFixture.archiveCommand
      import com.daml.http.json.JsonProtocol._
      import com.daml.ledger.api.v1.admin.{participant_pruning_service => PruneGrpc}
      import com.daml.timer.RetryStrategy, scala.concurrent.duration._
      import org.apache.pekko.http.scaladsl.model._
      import spray.json._
      for {
        (alice, aliceHeaders) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (bob, bobHeaders) <- fixture.getUniquePartyAndAuthHeaders("Bob")

        // perform some actions on the ledger
        (_, offsetAfterArchive) <- offsetBeforeAfterArchival(
          alice,
          fixture,
          aliceHeaders,
        )
        // prune
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

        // Create contract visible to both Alice and Bob
        contractId <- postCreateCommand(
          iouCreateCommand(
            alice,
            amount = "42.0",
            currency = "HKD",
            observers = Vector(bob),
          ),
          fixture,
          aliceHeaders,
        ) map resultContractId

        // Query by Alice, to populate the contract into cache
        _ <- searchExpectOk(
          List.empty,
          jsObject(s"""{"templateIds": ["${tidString(TpId.Iou.Iou)}"], "query": {"currency": "HKD"}}"""),
          fixture,
          aliceHeaders,
        ).map { acl => acl.size shouldBe 1 }

        // Archive this contract on the ledger. The cache is not yet updated.
        exercise = archiveCommand(domain.EnrichedContractId(Some(TpId.Iou.Iou), contractId))
        exerciseJson: JsValue = encodeExercise(fixture.encoder)(exercise)

        _ <- fixture
          .postJsonRequest(Uri.Path("/v1/exercise"), exerciseJson, aliceHeaders)
          .parseResponse[domain.ExerciseResponse[JsValue]]
          .flatMap(inside(_) { case domain.OkResponse(exercisedResponse, _, StatusCodes.OK) =>
            assertExerciseResponseArchivedContract(exercisedResponse, exercise)
          })

        // Now the *first* query by Bob.
        // This should not get confused by the fact that the archival happened before this query.
        _ <- searchExpectOk(
          List.empty,
          jsObject(s"""{"templateIds": ["${tidString(TpId.Iou.Iou)}"], "query": {"currency": "HKD"}}"""),
          fixture,
          bobHeaders,
        ).map { acl => acl.size shouldBe 0 }

      } yield succeed
  }
}
