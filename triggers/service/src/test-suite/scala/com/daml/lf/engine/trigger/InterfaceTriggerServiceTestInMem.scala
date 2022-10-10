// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.http.scaladsl.model.Uri
import com.daml.bazeltools.BazelRunfiles.requiredResource
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Choice}
import com.daml.ledger.api.v1.commands.{Command, CreateCommand, ExerciseCommand}
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import scalaz.syntax.tag._
import com.google.protobuf.{ByteString => PByteString}

import java.io.File
import java.nio.file.Files

class InterfaceTriggerServiceTestInMem
  extends AbstractTriggerServiceTestHelper
  with TriggerDaoInMemFixture
  with NoAuthFixture {

  override protected val darPath: File = requiredResource("triggers/service/test-model-1.dev.dar")

  it should "DEBUGGY" inClaims withTriggerService(List(dar)) { uri: Uri =>
    for {
      client <- sandboxClient(
        ApiTypes.ApplicationId("exp-app-id"),
        actAs = List(ApiTypes.Party(alice.unwrap)),
      )
      adminClient <- sandboxClient(
        ApiTypes.ApplicationId("exp-app-id"),
        admin = true,
      )
      _ <- adminClient.packageManagementClient.uploadDarFile(
        PByteString.copyFrom(Files.readAllBytes(darPath.toPath))
      )
      _ <- adminClient.partyManagementClient.allocateParty(Some(alice.unwrap), None)

      response <- startTrigger(uri, s"$testPkgId:InterfaceTriggers:trigger", alice, Some(ApplicationId("exp-app-id")))
      startedTriggerId <- parseTriggerId(response)
      _ <- assertTriggerIds(uri, alice, Vector(startedTriggerId))

      _ <- {
        val cmd = Command().withCreate(
          CreateCommand(
            templateId = Some(Identifier(testPkgId, "InterfaceTriggers", "A")),
            createArguments = Some(
              Record(
                fields = Seq(
                  RecordField("owner", Some(Value().withParty(alice.unwrap))),
                  RecordField("tag", Some(Value().withText("DEBUGGY-1"))),
                ),
              )
            ),
          )
        )
        submitCmd(client, alice.unwrap, cmd)
      }

      _ <- {
        val cmd = Command().withCreate(
          CreateCommand(
            templateId = Some(Identifier(testPkgId, "InterfaceTriggers", "A")),
            createArguments = Some(
              Record(
                fields = Seq(
                  RecordField("owner", Some(Value().withParty(alice.unwrap))),
                  RecordField("tag", Some(Value().withText("DEBUGGY-A"))),
                ),
              )
            ),
          )
        )
        submitCmd(client, alice.unwrap, cmd)
      }
      contracts <- getActiveContracts(client, alice, Identifier(testPkgId, "InterfaceTriggers", "A"))
      contractIdA = contracts.last.contractId

      _ <- {
        val cmd = Command().withCreate(
          CreateCommand(
            templateId = Some(Identifier(testPkgId, "InterfaceTriggers", "B")),
            createArguments = Some(
              Record(
                fields = Seq(
                  RecordField("owner", Some(Value().withParty(alice.unwrap))),
                  RecordField("tag", Some(Value().withText("DEBUGGY-B"))),
                ),
              )
            ),
          )
        )
        submitCmd(client, alice.unwrap, cmd)
      }
      contracts <- getActiveContracts(client, alice, Identifier(testPkgId, "InterfaceTriggers", "B"))
      contractIdB = contracts.last.contractId

      _ <- {
        val cmdA = Command().withExercise(
          ExerciseCommand(
            templateId = Some(Identifier(testPkgId, "InterfaceTriggers", "I")),
            contractId = contractIdA,
            choice = Choice("Log").unwrap,
            choiceArgument = Some(Value().withRecord(Record(fields = Seq(RecordField("msg", Some(Value().withText("DEBUGGY-A"))))))),
          )
        )
        submitCmd(client, alice.unwrap, cmdA)
      }

      _ <- {
        val cmdB = Command().withExercise(
          ExerciseCommand(
            templateId = Some(Identifier(testPkgId, "InterfaceTriggers", "I")),
            contractId = contractIdB,
            choice = Choice("Log").unwrap,
            choiceArgument = Some(Value().withRecord(Record(fields = Seq(RecordField("msg", Some(Value().withText("DEBUGGY-B"))))))),
          )
        )
        submitCmd(client, alice.unwrap, cmdB)
      }

      contracts <- getActiveContracts(client, alice, Identifier(testPkgId, "InterfaceTriggers", "A"))
      _ = println(s"DEBUGGY-A: $contracts")
      contracts <- getActiveContracts(client, alice, Identifier(testPkgId, "InterfaceTriggers", "B"))
      _ = println(s"DEBUGGY-B: $contracts")

      response <- stopTrigger(uri, startedTriggerId, alice)
      stoppedTriggerId <- parseTriggerId(response)
      _ <- stoppedTriggerId shouldBe startedTriggerId
    } yield fail()
  }
}
