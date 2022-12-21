// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.test

import akka.stream.scaladsl.Flow
import com.daml.lf.data.Ref._
import com.daml.lf.speedy.SValue
import com.daml.lf.speedy.SValue._
import com.daml.lf.value.Value.ContractId
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.commands.CreateCommand
import com.daml.ledger.api.v1.event.{Event => ApiEvent}
import com.daml.ledger.api.v1.event.Event.Event.Created
import com.daml.ledger.api.v1.transaction.{Transaction => ApiTransaction}
import com.daml.ledger.api.v1.{value => LedgerApi}
import com.daml.ledger.sandbox.SandboxOnXForTest.ParticipantId
import com.daml.lf.engine.trigger.Runner.TriggerContext
import com.daml.platform.services.time.TimeProviderType
import io.grpc.{Status => grpcStatus, StatusRuntimeException}
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.syntax.traverse._

import scala.jdk.CollectionConverters._
import com.daml.lf.engine.trigger.TriggerMsg
import com.daml.util.Ctx

import java.util.UUID
import scala.concurrent.Future

abstract class AbstractFuncTests
    extends AsyncWordSpec
    with AbstractTriggerTest
    with Matchers
    with Inside
    with SuiteResourceManagementAroundAll
    with TryValues {
  self: Suite =>

  this.getClass.getSimpleName can {
    "Failure testing" should {
      val contractPairings = 500

      // TODO https://github.com/digital-asset/daml/pull/15929
      //  enable the test
      s"with $contractPairings contract pairings and always failing submissions" ignore {
        def command(template: String, owner: String, i: Int): CreateCommand =
          CreateCommand(
            templateId = Some(LedgerApi.Identifier(packageId, "Cats", template)),
            createArguments = Some(
              LedgerApi.Record(fields =
                Seq(
                  LedgerApi.RecordField("owner", Some(LedgerApi.Value().withParty(owner))),
                  template match {
                    case "TestControl" =>
                      LedgerApi.RecordField("size", Some(LedgerApi.Value().withInt64(i.toLong)))
                    case _ =>
                      LedgerApi.RecordField("isin", Some(LedgerApi.Value().withInt64(i.toLong)))
                  },
                )
              )
            ),
          )

        def cat(owner: String, i: Int): CreateCommand = command("Cat", owner, i)

        def food(owner: String, i: Int): CreateCommand = command("Food", owner, i)

        def notObserving(
            templateId: LedgerApi.Identifier
        ): TriggerContext[TriggerMsg] => Boolean = {
          case Ctx(
                _,
                TriggerMsg.Transaction(
                  ApiTransaction(_, _, _, _, Seq(ApiEvent(Created(created))), _)
                ),
                _,
              ) if created.getTemplateId == templateId =>
            false

          case _ =>
            true
        }

        // TODO https://github.com/digital-asset/daml/pull/15929
        //  enable the test
        "Process all contract pairings successfully" ignore {
          for {
            client <- ledgerClient()
            party <- allocateParty(client)
            _ <- Future.sequence(
              (0 until contractPairings).map { i =>
                create(client, party, cat(party, i))
              }
            )
            _ <- Future.sequence(
              (0 until contractPairings).map { i =>
                create(client, party, food(party, i))
              }
            )
            runner = getRunner(
              client,
              QualifiedName.assertFromString("Cats:trigger"),
              party,
            )
            (acs, offset) <- runner.queryACS()
            _ <- runner
              .runWithACS(
                acs,
                offset,
                msgFlow = Flow[TriggerContext[TriggerMsg]]
                  // Allow flow to proceed until we observe a CatExample:TestComplete contract being created
                  .takeWhile(
                    notObserving(LedgerApi.Identifier(packageId, "Cats", "TestComplete"))
                  ),
              )
              ._2
          } yield {
            succeed
          }
        }
      }
    }

    "Batch trigger" should {
      val triggerId = QualifiedName.assertFromString("BatchTrigger:test")

      "process and track contract creation, exercise and archive" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, triggerId, party)
          (acs, offset) <- runner.queryACS()
          // 1 for create of T
          // 1 for completion
          // 1 for archive on T
          // 1 for completion
          finalState <- runner
            .runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(4))
            ._2
        } yield {
          inside(finalState) { case SList(commandIds) =>
            commandIds.toSet should have size 2
            // ensure all are UUIDs
            commandIds.map(inside(_) { case SText(s) =>
              SText(UUID.fromString(s).toString)
            }) should ===(commandIds)
          }
        }
      }
    }

    "AcsTests" should {
      val assetId = LedgerApi.Identifier(packageId, "ACS", "Asset")
      val assetMirrorId = LedgerApi.Identifier(packageId, "ACS", "AssetMirror")
      def asset(party: String): CreateCommand =
        CreateCommand(
          templateId = Some(assetId),
          createArguments = Some(
            LedgerApi.Record(fields =
              Seq(LedgerApi.RecordField("issuer", Some(LedgerApi.Value().withParty(party))))
            )
          ),
        )

      final case class AssetResult(
          successfulCompletions: Long,
          failedCompletions: Long,
          activeAssets: Set[String],
      )

      def toResult(value: SValue): AssetResult = {
        val fields = value.asInstanceOf[SRecord].values
        AssetResult(
          successfulCompletions = fields.get(1).asInstanceOf[SInt64].value,
          failedCompletions = fields.get(2).asInstanceOf[SInt64].value,
          activeAssets = fields
            .get(0)
            .asInstanceOf[SList]
            .list
            .map(x => x.asInstanceOf[SContractId].value.asInstanceOf[ContractId].coid.toString)
            .toSet,
        )
      }

      "1 create" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, QualifiedName.assertFromString("ACS:test"), party)
          (acs, offset) <- runner.queryACS()
          // 1 for the create from the test
          // 1 for the completion from the test
          // 1 for the create in the trigger
          // 1 for the exercise in the trigger
          // 2 completions for the trigger
          finalStateF = runner
            .runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(6))
            ._2
          contractId <- create(client, party, asset(party))
          result <- finalStateF.map(toResult)
          acs <- queryACS(client, party)
        } yield {
          result.activeAssets shouldBe Set(contractId)
          result.successfulCompletions shouldBe 2
          result.failedCompletions shouldBe 0
          acs(assetMirrorId) should have size 1
        }
      }

      "2 creates" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, QualifiedName.assertFromString("ACS:test"), party)
          (acs, offset) <- runner.queryACS()

          // 2 for the creates from the test
          // 2 completions for the test
          // 2 for the creates in the trigger
          // 2 for the exercises in the trigger
          // 4 completions for the trigger
          finalStateF = runner
            .runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(12))
            ._2

          contractId1 <- create(client, party, asset(party))
          contractId2 <- create(client, party, asset(party))

          result <- finalStateF.map(toResult)
          acs <- queryACS(client, party)
        } yield {
          result.activeAssets shouldBe Set(contractId1, contractId2)
          result.successfulCompletions shouldBe 4
          result.failedCompletions shouldBe 0
          acs(assetMirrorId) should have size 2
        }
      }

      "2 creates and 2 archives" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, QualifiedName.assertFromString("ACS:test"), party)
          (acs, offset) <- runner.queryACS()

          // 2 for the creates from the test
          // 2 for the archives from the test
          // 4 for the completions from the test
          // 2 for the creates in the trigger
          // 2 for the exercises in the trigger
          // 4 for the completions in the trigger
          finalStateF = runner
            .runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(16))
            ._2

          contractId1 <- create(client, party, asset(party))
          contractId2 <- create(client, party, asset(party))
          _ <- archive(client, party, assetId, contractId1)
          _ <- archive(client, party, assetId, contractId2)

          result <- finalStateF.map(toResult)
          acs <- queryACS(client, party)
        } yield {
          result.activeAssets shouldBe empty
          result.successfulCompletions shouldBe 4
          result.failedCompletions shouldBe 0
          acs(assetMirrorId) should have size 2
        }
      }
    }

    "CopyTests" should {
      val triggerId = QualifiedName.assertFromString("CopyTrigger:copyTrigger")
      val originalId = LedgerApi.Identifier(packageId, "CopyTrigger", "Original")
      val copyId = LedgerApi.Identifier(packageId, "CopyTrigger", "Copy")
      val subscriberId = LedgerApi.Identifier(packageId, "CopyTrigger", "Subscriber")
      def original(owner: String, name: String): CreateCommand =
        CreateCommand(
          templateId = Some(originalId),
          createArguments = Some(
            LedgerApi.Record(fields =
              Seq(
                LedgerApi.RecordField("owner", Some(LedgerApi.Value().withParty(owner))),
                LedgerApi.RecordField("name", Some(LedgerApi.Value().withText(name))),
                LedgerApi.RecordField("textdata", Some(LedgerApi.Value().withText(""))),
              )
            )
          ),
        )
      def subscriber(subscriber: String, subscribedTo: String): CreateCommand =
        CreateCommand(
          templateId = Some(subscriberId),
          createArguments = Some(
            LedgerApi.Record(fields =
              Seq(
                LedgerApi.RecordField("subscriber", Some(LedgerApi.Value().withParty(subscriber))),
                LedgerApi.RecordField(
                  "subscribedTo",
                  Some(LedgerApi.Value().withParty(subscribedTo)),
                ),
              )
            )
          ),
        )

      "1 original, 0 subscriber" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, triggerId, party)
          (acs, offset) <- runner.queryACS()
          // 1 for create of original
          // 1 for corresponding completion
          finalStateF = runner
            .runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(2))
            ._2
          _ <- create(client, party, original(party, "original0"))
          _ <- finalStateF
          acs <- queryACS(client, party)
        } yield {
          acs(originalId) should have length 1
          acs shouldNot contain key subscriberId
          acs shouldNot contain key copyId
        }
      }

      "1 original, 1 subscriber" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, triggerId, party)
          (acs, offset) <- runner.queryACS()
          // 1 for create of original
          // 1 for create of subscriber
          // 2 for corresponding completions
          // 1 for create of copy
          // 1 for corresponding completion
          finalStateF = runner
            .runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(6))
            ._2
          _ <- create(client, party, original(party, "original0"))
          _ <- create(client, party, subscriber(party, party))
          _ <- finalStateF
          acs <- queryACS(client, party)
        } yield {
          acs(originalId) should have length 1
          acs(subscriberId) should have length 1
          acs(copyId) should have length 1
        }
      }

      "2 original, 1 subscriber" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, triggerId, party)
          (acs, offset) <- runner.queryACS()
          // 2 for create of original
          // 1 for create of subscriber
          // 3 for corresponding completions
          // 2 for create of copy
          finalStateF = runner
            .runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(10))
            ._2
          _ <- create(client, party, original(party, "original0"))
          _ <- create(client, party, original(party, "original1"))
          _ <- create(client, party, subscriber(party, party))
          _ <- finalStateF
          acs <- queryACS(client, party)
        } yield {
          acs(originalId) should have length 2
          acs(subscriberId) should have length 1
          acs(copyId) should have length 2
        }
      }
    }

    "RetryTests" should {
      val triggerId = QualifiedName.assertFromString("Retry:retryTrigger")
      val tId = LedgerApi.Identifier(packageId, "Retry", "T")
      val doneId = LedgerApi.Identifier(packageId, "Retry", "Done")

      "3 retries" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, triggerId, party)
          (acs, offset) <- runner.queryACS()
          // 1 for create of T
          // 1 for completion
          // 3 failed completion for exercises
          // 1 for create of Done
          // 1 for corresponding completion
          _ <- runner.runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(7))._2
          acs <- queryACS(client, party)
        } yield {
          acs(tId) should have length 1
          acs(doneId) should have length 1
        }
      }
    }

    "ExerciseByKeyTest" should {
      val triggerId = QualifiedName.assertFromString("ExerciseByKey:exerciseByKeyTrigger")
      val tId = LedgerApi.Identifier(packageId, "ExerciseByKey", "T")
      val tPrimeId = LedgerApi.Identifier(packageId, "ExerciseByKey", "T_")

      "1 exerciseByKey" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, triggerId, party)
          (acs, offset) <- runner.queryACS()
          // 1 for create of T
          // 1 for completion
          // 1 for exerciseByKey
          // 1 for corresponding completion
          _ <- runner.runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(4))._2
          acs <- queryACS(client, party)
        } yield {
          acs(tId) should have length 1
          acs(tPrimeId) should have length 1
        }
      }
    }

    "CreateAndExercise" should {
      val triggerId = QualifiedName.assertFromString("CreateAndExercise:createAndExerciseTrigger")
      val tId = LedgerApi.Identifier(packageId, "CreateAndExercise", "T")
      val uId = LedgerApi.Identifier(packageId, "CreateAndExercise", "U")

      "createAndExercise" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, triggerId, party)
          (acs, offset) <- runner.queryACS()
          // 1 for create and exercise
          // 1 for completion
          _ <- runner.runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(2))._2
          acs <- queryACS(client, party)
        } yield {
          acs(tId) should have length 1
          acs(uId) should have length 1
        }
      }
    }

    "MaxMessageSizeTests" should {
      val triggerId =
        QualifiedName.assertFromString("MaxInboundMessageTest:maxInboundMessageSizeTrigger")

      "fail" in {
        for {
          client <- ledgerClient(
            // Sufficiently low that the transaction is larger than the max inbound message size
            maxInboundMessageSize = 100
          )
          party <- allocateParty(client)
          runner = getRunner(client, triggerId, party)
          (acs, offset) <- runner.queryACS()
          // 1 for create and exercise
          // 1 for completion
          // 1 for the transaction
          ex <- recoverToExceptionIf[StatusRuntimeException](
            runner.runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(3))._2
          )
        } yield {
          ex.getStatus.getCode shouldBe grpcStatus.Code.RESOURCE_EXHAUSTED
        }
      }
    }

    "NumericTests" should {
      val triggerId = QualifiedName.assertFromString("Numeric:test")
      val tId = LedgerApi.Identifier(packageId, "Numeric", "T")

      "numeric" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, triggerId, party)
          (acs, offset) <- runner.queryACS()
          // 1 for create of T
          // 1 for completion
          // 1 for exercise on T
          // 1 for completion
          _ <- runner.runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(4))._2
          acs <- queryACS(client, party)
        } yield {
          val vals = acs(tId).map(_.fields(1).getValue.getNumeric).toSet
          vals shouldBe Set("1.06000000000", "2.06000000000")
        }
      }
    }

    "CommandIdTests" should {
      val triggerId = QualifiedName.assertFromString("CommandId:test")

      "command-id" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, triggerId, party)
          (acs, offset) <- runner.queryACS()
          // 1 for create of T
          // 1 for completion
          // 1 for archive on T
          // 1 for completion
          finalState <- runner
            .runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(4))
            ._2
        } yield {
          inside(finalState) { case SList(commandIds) =>
            commandIds.toSet should have size 2
            // ensure all are UUIDs
            commandIds.map(inside(_) { case SText(s) =>
              SText(UUID.fromString(s).toString)
            }) should ===(commandIds)
          }
        }
      }
    }

    "PendingTests" should {
      val triggerId = QualifiedName.assertFromString("PendingSet:booTrigger")
      val fooId = LedgerApi.Identifier(packageId, "PendingSet", "Foo")
      val booId = LedgerApi.Identifier(packageId, "PendingSet", "Boo")
      val doneId = LedgerApi.Identifier(packageId, "PendingSet", "Done")

      "pending set" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, triggerId, party)
          (acs, offset) <- runner.queryACS()
          // 1 for the creates at startup
          // 1 for the completion from startup
          // 1 for the exercise in the trigger
          // 1 for the completion in the trigger
          _ <- runner.runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(4))._2
          acs <- queryACS(client, party)
        } yield {
          acs(doneId) should have length 1
          acs shouldNot contain key fooId
          acs(booId) should have length 1
        }
      }
    }

    "TemplateFilterTests" should {
      val doneOneId = LedgerApi.Identifier(packageId, "TemplateIdFilter", "DoneOne")
      val doneTwoId = LedgerApi.Identifier(packageId, "TemplateIdFilter", "DoneTwo")
      val oneId = LedgerApi.Identifier(packageId, "TemplateIdFilter", "One")
      val twoId = LedgerApi.Identifier(packageId, "TemplateIdFilter", "Two")

      def one(party: String): CreateCommand =
        CreateCommand(
          templateId = Some(oneId),
          createArguments = Some(
            LedgerApi.Record(
              fields = Seq(LedgerApi.RecordField("p", Some(LedgerApi.Value().withParty(party))))
            )
          ),
        )

      def two(party: String): CreateCommand =
        CreateCommand(
          templateId = Some(twoId),
          createArguments = Some(
            LedgerApi.Record(
              fields = Seq(LedgerApi.RecordField("p", Some(LedgerApi.Value().withParty(party))))
            )
          ),
        )

      "filter to One" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(
            client,
            QualifiedName.assertFromString("TemplateIdFilter:testOne"),
            party,
          )
          _ <- create(client, party, one(party))
          _ <- create(client, party, two(party))
          (acs, offset) <- runner.queryACS()
          // 1 for the create in the trigger
          // 1 for the completion from the trigger
          _ <- runner.runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(2))._2
          acs <- queryACS(client, party)
        } yield {
          acs(doneOneId) should have length 1
          acs shouldNot contain key doneTwoId
        }
      }

      "filter to Two" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(
            client,
            QualifiedName.assertFromString("TemplateIdFilter:testTwo"),
            party,
          )
          _ <- create(client, party, one(party))
          _ <- create(client, party, two(party))
          (acs, offset) <- runner.queryACS()
          // 1 for the create in the trigger
          // 1 for the completion from the trigger
          _ <- runner.runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(2))._2
          acs <- queryACS(client, party)
        } yield {
          acs shouldNot contain key doneOneId
          acs(doneTwoId) should have length 1
        }
      }
    }

    "HeartbeatTests" should {
      val triggerId = QualifiedName.assertFromString("Heartbeat:test")

      "test" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, triggerId, party)
          (acs, offset) <- runner.queryACS()
          // 2 heartbeats
          finalState <- runner
            .runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(2))
            ._2
        } yield {
          finalState shouldBe SInt64(2)
        }
      }
    }

    "TimeTests" should {
      "test" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(client, QualifiedName.assertFromString("Time:test"), party)
          (acs, offset) <- runner.queryACS()
          finalState <- runner
            .runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(4))
            ._2
        } yield {
          finalState match {
            case SRecord(_, _, values) if values.size == 2 =>
              values.get(1) match {
                case SList(items) if items.length == 2 =>
                  val t0 = items.slowApply(0).asInstanceOf[STimestamp].value
                  val t1 = items.slowApply(1).asInstanceOf[STimestamp].value
                  config
                    .participants(ParticipantId)
                    .apiServer
                    .timeProviderType match {
                    case TimeProviderType.WallClock =>
                      // Given the limited resolution it can happen that t0 == t1
                      t0 should be >= t1
                    case TimeProviderType.Static =>
                      t0 shouldBe t1
                  }
                case v => fail(s"Expected list with 2 elements but got $v")
              }
            case _ => fail(s"Expected Tuple2 but got $finalState")
          }
        }
      }
    }

    "readAs" should {
      val visibleToPublicId = LedgerApi.Identifier(packageId, "ReadAs", "VisibleToPublic")
      def visibleToPublic(party: String): CreateCommand =
        CreateCommand(
          templateId = Some(visibleToPublicId),
          createArguments = Some(
            LedgerApi.Record(fields =
              Seq(LedgerApi.RecordField("public", Some(LedgerApi.Value().withParty(party))))
            )
          ),
        )

      "respected by trigger runner" in {
        for {
          client <- ledgerClient()
          public <- allocateParty(client)
          party <- allocateParty(client)
          _ <- create(client, public, visibleToPublic(public))
          runner = getRunner(
            client,
            QualifiedName.assertFromString("ReadAs:test"),
            party,
            Set(public),
          )
          (acs, offset) <- runner.queryACS()
          // 1 for the completion & 1 for the transaction.
          result <- runner
            .runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(2))
            ._2
        } yield {
          inside(toHighLevelResult(result).state) { case SInt64(i) =>
            i shouldBe 3
          }
        }
      }
    }
    "getActAs" should {
      "produce a consistent party" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(
            client,
            QualifiedName.assertFromString("ActAs:test"),
            party,
          )
          (acs, offset) <- runner.queryACS()
          // 1 for the completion & 1 for the transaction.
          result <- runner
            .runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(2))
            ._2
        } yield {
          inside(toHighLevelResult(result).state) { case SRecord(_, _, values) =>
            // Check that both updateState and rule were executed.
            values.asScala shouldBe Seq[SValue](
              SParty(Party.assertFromString(party)),
              SBool(true),
              SBool(true),
            )
          }
        }
      }
    }

    "queryFilter" should {
      "return contracts matching predicates" in {
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          runner = getRunner(
            client,
            QualifiedName.assertFromString("QueryFilter:trigger"),
            party,
          )
          (acs, offset) <- runner.queryACS()
          // 1 for the completion & 1 for the transaction.
          result <- runner
            .runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(2))
            ._2
        } yield {
          inside(toHighLevelResult(result).state) { case SRecord(_, _, values) =>
            values.asScala shouldBe Seq[SValue](
              SInt64(1),
              SInt64(2),
              SBool(true),
            )
          }
        }
      }
    }
  }
}
