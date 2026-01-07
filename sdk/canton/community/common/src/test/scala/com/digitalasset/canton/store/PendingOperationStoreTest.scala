// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protobuf.VersionedMessageV0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.PendingOperation.{
  ConflictingPendingOperationError,
  PendingOperationTriggerType,
}
import com.digitalasset.canton.store.PendingOperationStoreTest.TestPendingOperationMessage.createMessage
import com.digitalasset.canton.store.PendingOperationStoreTest.{
  TestPendingOperationMessage,
  createOp,
  op1,
  op1Modified,
  op2,
  op3,
}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.topology.{DefaultTestIdentities, SynchronizerId}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext}
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.wordspec.{AnyWordSpec, AsyncWordSpec}

import scala.concurrent.Future

trait PendingOperationStoreTest[Op <: HasProtocolVersionedWrapper[Op]]
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown {

  /** A "backdoor" method for tests to insert a malformed operation bypassing the validation in the
    * public `insert` method.
    */
  protected def insertCorruptedData(
      op: PendingOperation[TestPendingOperationMessage],
      store: Option[PendingOperationStore[TestPendingOperationMessage]] = None,
      corruptOperationBytes: Option[ByteString] = None,
  ): Future[Unit]

  def pendingOperationsStore(mk: () => PendingOperationStore[TestPendingOperationMessage]): Unit = {

    def withStore(
        testCode: PendingOperationStore[TestPendingOperationMessage] => Future[Assertion]
    ): Future[Assertion] =
      testCode(mk())

    "insert and retrieve an operation" in withStore { store =>
      for {
        _ <- store.insert(op1).value
        retrieved <- store.get(op1.synchronizerId, op1.key, op1.name).value
      } yield retrieved shouldBe Some(op1)
    }

    "return None for a non-existent operation" in withStore { store =>
      store
        .get(
          DefaultTestIdentities.synchronizerId,
          "non-existent-key",
          NonEmptyString.tryCreate("non-existent-name"),
        )
        .value
        .map(_ shouldBe None)
    }

    "succeed when inserting an identical operation twice" in withStore { store =>
      for {
        _ <- store.insert(op1).value
        _ <- store.insert(op1).value
        retrieved <- store.get(op1.synchronizerId, op1.key, op1.name).value
      } yield retrieved shouldBe Some(op1)
    }

    "fail when inserting a conflicting operation" in withStore { store =>
      val conflictingInsertResult = for {
        _ <- store.insert(op1)
        _ <- store.insert(op1Modified) // Attempt to insert with same key but with different data
      } yield ()

      conflictingInsertResult.value.map { result =>
        val expectedError = ConflictingPendingOperationError(
          synchronizerId = op1.synchronizerId,
          key = op1.key,
          name = op1.name,
        )
        result shouldBe Left(expectedError)
      }
    }

    "insert and update an operation" in withStore { store =>
      for {
        insertResult <- store.insert(op1).value
        _ = insertResult shouldBe Right(())
        retrievedBefore <- store.get(op1.synchronizerId, op1.key, op1.name).value
        _ = retrievedBefore shouldBe Some(op1)
        _ <- store.updateOperation(op1Modified.operation, op1.synchronizerId, op1.name, op1.key)
        retrievedAfter <- store.get(op1.synchronizerId, op1.key, op1.name).value
      } yield {
        retrievedBefore shouldBe Some(op1)
        retrievedAfter shouldBe Some(op1Modified)
      }
    }

    "succeed when updating a non-existent operation" in withStore { store =>
      for {
        _ <- store.updateOperation(op1.operation, op1.synchronizerId, op1.name, op1.key)
        notUpdated <- store.get(op1.synchronizerId, op1.key, op1.name).value
      } yield notUpdated shouldBe None
    }

    "retrieve only operations matching operation name" in withStore { store =>
      val da = SynchronizerId.tryFromString("da::default")
      val acme = SynchronizerId.tryFromString("acme::default")
      val onpr = NonEmptyString.tryCreate("onpr")
      val offpr = NonEmptyString.tryCreate("offpr")
      val onprDa = (1 to 5).toList.map(i => createOp(onpr.unwrap, s"key$i", s"onpr$i", da))
      val offprDa = (6 to 10).toList.map(i => createOp(offpr.unwrap, s"key$i", s"offpr$i", da))
      val onprAcme = (11 to 15).toList.map(i => createOp(onpr.unwrap, s"key$i", s"onpr$i", acme))
      val offprAcme = (16 to 20).toList.map(i => createOp(offpr.unwrap, s"key$i", s"offpr$i", acme))
      val allOperations = onprDa ++ offprDa ++ onprAcme ++ offprAcme

      for {
        empty <- EitherT.right[ConflictingPendingOperationError](store.getAll(onpr))
        _ <- MonadUtil.sequentialTraverse(allOperations)(store.insert)
        onprs <- EitherT.right[ConflictingPendingOperationError](store.getAll(onpr))
        offprs <- EitherT.right[ConflictingPendingOperationError](store.getAll(offpr))
      } yield {
        empty shouldBe Set.empty
        onprs shouldBe onprDa.toSet ++ onprAcme
        offprs shouldBe offprDa.toSet ++ offprAcme.toSet
      }
    }

    "insert and delete an operation" in withStore { store =>
      for {
        insertResult <- store.insert(op2).value
        _ = insertResult shouldBe Right(())
        retrievedBefore <- store.get(op2.synchronizerId, op2.key, op2.name).value
        _ = retrievedBefore shouldBe Some(op2)
        _ <- store.delete(op2.synchronizerId, op2.key, op2.name)
        retrievedAfter <- store.get(op2.synchronizerId, op2.key, op2.name).value
      } yield {
        retrievedAfter shouldBe None
      }
    }

    "succeed when deleting a non-existent operation" in withStore { store =>
      store
        .delete(DefaultTestIdentities.synchronizerId, "key", NonEmptyString.tryCreate("name"))
        .map(_ => succeed)
    }

    "succeed in inserting an operation with an empty key" in withStore { store =>
      val opWithEmptyKey = createOp(name = "opName1", key = "", data = "data")
      for {
        _ <- store.insert(opWithEmptyKey).value
        retrieved <- store
          .get(opWithEmptyKey.synchronizerId, opWithEmptyKey.key, opWithEmptyKey.name)
          .value
      } yield retrieved shouldBe Some(opWithEmptyKey)
    }

    "fail with an exception when getting a corrupt operation" in withStore { store =>
      val testFlow = for {
        _ <- FutureUnlessShutdown.outcomeF(
          insertCorruptedData(
            op3,
            Some(store),
            corruptOperationBytes = Some(ByteString.copyFromUtf8("unparseable garbage")),
          )
        )
        _ <- store.get(op3.synchronizerId, op3.key, op3.name).value
      } yield ()

      whenReady(testFlow.unwrap.failed) { e =>
        e shouldBe a[DbDeserializationException]
        e.getMessage should include("Failed to deserialize pending operation byte string")
      }
    }

    "insert atomically when called concurrently" in withStore { store =>
      // Create a list of operations: one original and 10 conflicting ops:
      val conflictingOps = List.fill(10)(op1Modified)
      val allOps = op1 :: conflictingOps

      // Sanity-checks:
      // - Assert that all 10 conflicting operations are identical
      conflictingOps.foreach(_ shouldBe op1Modified)
      // - Assert that the original operation is indeed different from the modified one
      op1 should not be op1Modified
      // - Assert that their composite keys are identical, which is why they conflict
      op1.compositeKey shouldBe op1Modified.compositeKey

      // Shuffle to ensure the "winner" of the race is not deterministic :D
      val randomizedOps = scala.util.Random.shuffle(allOps)

      // Concurrently execute an insert for every operation
      val insertFutures = randomizedOps.map(op => store.insert(op).value)
      val allInsertsF = FutureUnlessShutdown.sequence(insertFutures)

      for {
        results <- allInsertsF // Wait for all concurrent inserts to complete
        actuallyStoredOp <- store.get(op1.synchronizerId, op1.key, op1.name).value
      } yield {
        val (successfulInserts, failedInserts) = results.partition(_.isRight)

        actuallyStoredOp should be(defined)
        val winner = actuallyStoredOp.value
        winner should (be(op1) or be(op1Modified))

        // Asserts that the number of successful and failed inserts is consistent
        // with whichever operation won the race to insert.
        winner match {
          case op if op == op1 =>
            withClue("If op1 won the race, all op1Modified inserts should have failed:") {
              successfulInserts should have length 1
              failedInserts should have length 10
            }

          case op if op == op1Modified =>
            withClue("If op1Modified won the race, only the op1 inserts should have failed:") {
              successfulInserts should have length 10
              failedInserts should have length 1
            }

          // To satisfy the compiler, make the match exhaustive and robust
          case _ =>
            fail(s"The winner of the race was an unexpected operation: $winner")
        }

      }
    }

  }
}

object PendingOperationStoreTest {

  private def createOp(
      name: String,
      key: String,
      data: String,
      synchronizerId: SynchronizerId = DefaultTestIdentities.synchronizerId,
  ): PendingOperation[TestPendingOperationMessage] =
    PendingOperation
      .create(
        trigger = PendingOperationTriggerType.SynchronizerReconnect.asString,
        name = name,
        key = key,
        operationBytes = createMessage(data).toByteString,
        operationDeserializer = TestPendingOperationMessage.fromTrustedByteString,
        synchronizerId = synchronizerId.toProtoPrimitive,
      )
      .valueOr(errorMessage => throw new DbDeserializationException(errorMessage))

  def createInvalid(
      trigger: String = PendingOperationTriggerType.SynchronizerReconnect.asString,
      name: String = "valid-name",
      key: String = "valid-key",
      operationBytes: ByteString = createMessage("valid-data").toByteString,
      operationDeserializer: ByteString => ParsingResult[TestPendingOperationMessage] =
        TestPendingOperationMessage.fromTrustedByteString,
      synchronizerId: String = DefaultTestIdentities.synchronizerId.toProtoPrimitive,
  ): Either[String, PendingOperation[TestPendingOperationMessage]] =
    PendingOperation.create(
      trigger,
      name,
      key,
      operationBytes,
      operationDeserializer,
      synchronizerId,
    )

  protected val op1: PendingOperation[TestPendingOperationMessage] =
    createOp("opName1", "opKey1", "operation-1-data")
  protected val op2: PendingOperation[TestPendingOperationMessage] =
    createOp("opName2", "opKey2", "operation-2-data")
  protected val op3: PendingOperation[TestPendingOperationMessage] =
    createOp("opName3", "opKey3", "operation-3-data")
  protected val op1Modified: PendingOperation[TestPendingOperationMessage] =
    op1.cp(operation = createMessage("modified-data"))

  private def protocolVersionRepresentative(
      pv: ProtocolVersion
  ): RepresentativeProtocolVersion[TestPendingOperationMessage.type] =
    TestPendingOperationMessage.protocolVersionRepresentativeFor(pv)

  final case class TestPendingOperationMessage(data: String)(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[
        TestPendingOperationMessage.type
      ]
  ) extends HasProtocolVersionedWrapper[TestPendingOperationMessage] {
    @transient override protected lazy val companionObj: TestPendingOperationMessage.type =
      TestPendingOperationMessage

    def toProtoV0: VersionedMessageV0 = VersionedMessageV0(data)
  }

  object TestPendingOperationMessage
      extends VersioningCompanion[TestPendingOperationMessage]
      with IgnoreInSerializationTestExhaustivenessCheck {

    def name: String = "TestPendingOperationMessage"

    override val versioningTable: VersioningTable = VersioningTable(
      ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(VersionedMessageV0)(
        supportedProtoVersion(_)(fromProtoV0),
        _.toProtoV0,
      )
    )

    def fromProtoV0(message: VersionedMessageV0): ParsingResult[TestPendingOperationMessage] =
      createMessage(message.msg).asRight

    def createMessage(data: String): TestPendingOperationMessage =
      TestPendingOperationMessage(data)(protocolVersionRepresentative(ProtocolVersion.minimum))
  }
}

final class PendingOperationTest extends AnyWordSpec with BaseTest {

  import com.digitalasset.canton.store.PendingOperationStoreTest.createInvalid

  "PendingOperation" should {

    "fail to create an operation with an unknown trigger" in {
      val result = createInvalid(trigger = "unknown-trigger")
      result shouldBe Left("Unknown pending operation trigger type: unknown-trigger")
    }

    "fail to create an operation with an empty name" in {
      val result = createInvalid(name = "")
      result shouldBe Left("Missing pending operation name (blank): ")
    }

    "fail to create an operation with invalid operation bytes" in {
      val result = createInvalid(operationBytes = ByteString.copyFromUtf8("some-data"))
      result.left.value should include("Failed to deserialize pending operation byte string")
    }

    "fail to create an operation with invalid synchronizer ID" in {
      val result = createInvalid(synchronizerId = "invalid-sync-id")
      result.left.value should include("Failed to deserialize synchronizer ID string")
    }

  }

}
