// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.error.ValueSwitch

import java.time.Duration
import com.daml.ledger.participant.state.kvutils.TestHelpers._
import com.daml.ledger.participant.state.kvutils.store.events.DamlTransactionRejectionEntry
import com.daml.ledger.participant.state.kvutils.store.{DamlLogEntry, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmission
import com.daml.ledger.participant.state.v2.Update
import com.daml.ledger.test._
import com.daml.lf.command.ApiCommand
import com.daml.lf.crypto
import com.daml.lf.crypto.Hash
import com.daml.lf.data.{FrontStack, Ref, SortedLookupList}
import com.daml.lf.transaction.Node
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{
  ContractId,
  ValueList,
  ValueOptional,
  ValueParty,
  ValueRecord,
  ValueTextMap,
  ValueUnit,
  ValueVariant,
}
import com.daml.logging.LoggingContext
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.compat.immutable.LazyList
import scala.jdk.CollectionConverters._

class KVUtilsTransactionSpec extends AnyWordSpec with Matchers with Inside {

  import KVTest._

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val errorVersionSwitch =
    new ValueSwitch[com.google.rpc.status.Status](enableSelfServiceErrorCodes = false)

  private val alice = party("Alice")
  private val bob = party("Bob")
  private val eve = party("Eve")
  private val bobValue = ValueParty(bob)

  "transaction" should {

    val templateArgs: Map[TestDar, SimplePackage => Value] = Map(
      SimplePackagePartyTestDar -> (_ => bobValue),
      SimplePackageOptionalTestDar -> (_ => ValueOptional(Some(bobValue))),
      SimplePackageListTestDar -> (_ => ValueList(FrontStack(bobValue))),
      SimplePackageTextMapTestDar -> (_ => ValueTextMap(SortedLookupList(Map("bob" -> bobValue)))),
      SimplePackageVariantTestDar -> (simplePackage =>
        ValueVariant(
          Some(simplePackage.typeConstructorId("Simple:SimpleVariant")),
          name("SV"),
          bobValue,
        )
      ),
      SimplePackageTupleTestDar -> (simplePackage =>
        ValueRecord(
          Some(simplePackage.typeConstructorId("DA.Types:Tuple2")),
          FrontStack(
            Some(name("_1")) -> bobValue,
            Some(name("_2")) -> ValueUnit,
          ).toImmArray,
        )
      ),
      // Types not yet supported in Daml:
      //
      // "<party: Party>": "Value.ValueStruct(FrontStack(Ref.Name.assertFromString("party") -> bobValue).toImmArray),
      // "GenMap Party Unit": Value.ValueGenMap(FrontStack(bobValue -> ValueUnit).toImmArray),
      // "GenMap Unit Party": Value.ValueGenMap(FrontStack(ValueUnit -> bobValue).toImmArray),
    )

    val p0 = mkParticipantId(0)
    val p1 = mkParticipantId(1)

    "add the key of an exercised contract as a submission input" in KVTest.runTestWithSimplePackage(
      alice,
      bob,
      eve,
    ) { simplePackage =>
      val seed0 = seed(0)
      val seed1 = seed(1)
      for {
        transaction1 <- runSimpleCommand(alice, seed0, simpleCreateCmd(simplePackage))
        result <- submitTransaction(
          submitter = alice,
          transaction = transaction1,
          submissionSeed = seed0,
        )
        (entryId, logEntry) = result
        contractId = contractIdOfCreateTransaction(
          KeyValueConsumption.logEntryToUpdate(
            entryId,
            logEntry,
            errorVersionSwitch,
          )(loggingContext)
        )

        transaction2 <- runSimpleCommand(
          alice,
          seed1,
          simplePackage.simpleExerciseArchiveCmd(contractId),
        )
        submission <- prepareTransactionSubmission(
          submitter = alice,
          transaction = transaction2,
          submissionSeed = seed1,
        )
      } yield {
        submission.getInputDamlStateList.asScala.filter(_.hasContractKey) should not be empty
      }
    }

    "be able to submit a transaction" in KVTest.runTestWithSimplePackage(alice, bob, eve) {
      simplePackage =>
        val seed = hash(this.getClass.getName)
        for {
          transaction <- runSimpleCommand(alice, seed, simpleCreateCmd(simplePackage))
          logEntry <- submitTransaction(
            submitter = alice,
            transaction = transaction,
            submissionSeed = seed,
          ).map(_._2)
        } yield {
          logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY
          logEntry.getTransactionEntry.hasBlindingInfo shouldBe true
        }
    }

    "be able to pre-execute a transaction" in KVTest.runTestWithSimplePackage(alice, bob, eve) {
      simplePackage =>
        val seed = hash(this.getClass.getName)
        for {
          transaction <- runSimpleCommand(alice, seed, simpleCreateCmd(simplePackage))
          preExecutionResult <- preExecuteTransaction(
            submitter = alice,
            transaction = transaction,
            submissionSeed = seed,
          ).map(_._2)
        } yield {
          preExecutionResult.successfulLogEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY
          preExecutionResult.successfulLogEntry.getTransactionEntry.hasBlindingInfo shouldBe true
        }
    }

    "be able to pre-execute exercising a consuming choice by key" in
      KVTest.runTestWithSimplePackage(alice, bob, eve) { simplePackage =>
        for {
          _ <- preExecuteCreateSimpleContract(alice, seed(0), simplePackage)
          preparedSubmission <- prepareExerciseReplaceByKey(alice, simplePackage)(seed(1))
          result <- preExecute(preparedSubmission).map(_._2)
        } yield {
          result.successfulLogEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY
        }
      }

    "reject a pre-executed exercise by key referring to a replaced key" in
      KVTest.runTestWithSimplePackage(alice, bob, eve) { simplePackage =>
        val seeds =
          LazyList
            .from(0)
            .map(i => seed(i))

        for {
          _ <- preExecuteCreateSimpleContract(alice, seeds.head, simplePackage)
          preparedSubmissions <- inParallelReadOnly(
            seeds.slice(1, 3).map(prepareExerciseReplaceByKey(alice, simplePackage))
          )
          preExecutionResults <- sequentially(preparedSubmissions.map(preExecute(_).map(_._2)))
        } yield {
          val Seq(resultA, resultB) = preExecutionResults
          resultA.successfulLogEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY

          resultB.successfulLogEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
          resultB.successfulLogEntry.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.EXTERNALLY_INCONSISTENT_KEYS
        }
      }

    "reject a pre-executed exercise by key referring to a consumed contract" in
      KVTest.runTestWithSimplePackage(alice, bob, eve) { simplePackage =>
        for {
          contractId <- preExecuteCreateSimpleContract(alice, seed(0), simplePackage)
          preparedSubmissions <- inParallelReadOnly(
            Seq(
              prepareExerciseArchiveCmd(alice, simplePackage, contractId)(seed(1)),
              prepareExerciseReplaceByKey(alice, simplePackage)(seed(2)),
            )
          )
          preExecutionResults <- sequentially(preparedSubmissions.map(preExecute(_).map(_._2)))
        } yield {
          val Seq(resultA, resultB) = preExecutionResults
          resultA.successfulLogEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY

          resultB.successfulLogEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
          resultB.successfulLogEntry.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.EXTERNALLY_INCONSISTENT_KEYS
        }
      }

    "reject a pre-executed submission indirectly referring to a replaced key" in
      KVTest.runTestWithSimplePackage(alice, bob, eve) { simplePackage =>
        def preExecuteCreateSimpleHolderContract(seed: Hash): KVTest[ContractId] =
          for {
            recordTime <- currentRecordTime
            createHolderTransaction <- runSimpleCommand(
              alice,
              seed,
              simplePackage.simpleHolderCreateCmd(simplePackage.mkSimpleHolderTemplateArg(alice)),
            )
            holderContractId <- preExecuteTransaction(
              submitter = alice,
              transaction = createHolderTransaction,
              submissionSeed = seed,
            )
            (entryId, preExecutionResult) = holderContractId
          } yield {
            contractIdOfCreateTransaction(
              KeyValueConsumption.logEntryToUpdate(
                entryId,
                preExecutionResult.successfulLogEntry,
                errorVersionSwitch,
                Some(recordTime),
              )(loggingContext)
            )
          }

        def prepareExerciseReplaceHeldByKey(
            holderContractId: ContractId
        )(seed: Hash): KVTest[DamlSubmission] =
          for {
            exerciseTransaction <- runSimpleCommand(
              alice,
              seed,
              simplePackage.simpleHolderExerciseReplaceHeldByKeyCmd(holderContractId),
            )
            submission <- prepareTransactionSubmission(
              submitter = alice,
              transaction = exerciseTransaction,
              submissionSeed = seed,
            )
          } yield submission

        val seeds =
          LazyList
            .from(0)
            .map(i => seed(i))

        for {
          _ <- preExecuteCreateSimpleContract(alice, seeds.head, simplePackage)
          holderContractId <- preExecuteCreateSimpleHolderContract(seeds(1))
          preparedSubmissions <- inParallelReadOnly(
            seeds
              .slice(1, 3)
              .map(prepareExerciseReplaceHeldByKey(holderContractId))
          )
          preExecutionResults <- sequentially(preparedSubmissions.map(preExecute(_).map(_._2)))
        } yield {
          val Seq(resultA, resultB) = preExecutionResults
          resultA.successfulLogEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY

          resultB.successfulLogEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
          resultB.successfulLogEntry.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.EXTERNALLY_INCONSISTENT_KEYS
        }
      }

    "reject transaction with out of bounds LET" in KVTest.runTestWithSimplePackage(
      alice,
      bob,
      eve,
    ) { simplePackage =>
      val seed = hash(this.getClass.getName)
      for {
        transaction <- runSimpleCommand(alice, seed, simpleCreateCmd(simplePackage))
        conf <- getDefaultConfiguration
        logEntry <- submitTransaction(
          submitter = alice,
          transaction = transaction,
          submissionSeed = seed,
          letDelta = conf.timeModel.maxSkew.plusMillis(1),
        )
          .map(_._2)
      } yield {
        logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
        logEntry.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.INVALID_LEDGER_TIME
      }
    }

    "be able to exercise and rejects double spends" in KVTest.runTestWithSimplePackage(
      alice,
      bob,
      eve,
    ) { simplePackage =>
      val seeds =
        LazyList
          .from(0)
          .map(i => seed(i))
      for {
        transaction1 <- runSimpleCommand(alice, seeds.head, simpleCreateCmd(simplePackage))
        result <- submitTransaction(
          submitter = alice,
          transaction = transaction1,
          submissionSeed = seeds.head,
        )
        (entryId, logEntry) = result
        contractId = contractIdOfCreateTransaction(
          KeyValueConsumption.logEntryToUpdate(
            entryId,
            logEntry,
            errorVersionSwitch,
          )(loggingContext)
        )

        transaction2 <- runSimpleCommand(
          alice,
          seeds(1),
          simplePackage.simpleExerciseArchiveCmd(contractId),
        )
        logEntry2 <- submitTransaction(
          submitter = alice,
          transaction = transaction2,
          submissionSeed = seeds(1),
        ).map(_._2)

        // Try to double consume.
        logEntry3 <- submitTransaction(
          submitter = alice,
          transaction = transaction2,
          submissionSeed = seeds(1),
        ).map(_._2)

      } yield {
        logEntry2.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY
        logEntry3.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
        logEntry3.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.EXTERNALLY_INCONSISTENT_KEYS
      }
    }

    "reject transactions by unallocated submitters" in KVTest.runTestWithSimplePackage(bob, eve) {
      simplePackage =>
        val seed = hash(this.getClass.getName)
        for {
          configEntry <- submitConfig { c =>
            c.copy(generation = c.generation + 1)
          }
          transaction <- runSimpleCommand(alice, seed, simpleCreateCmd(simplePackage))
          txEntry <- submitTransaction(
            submitter = alice,
            transaction = transaction,
            submissionSeed = seed,
          ).map(_._2)
        } yield {
          configEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY
          txEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
          txEntry.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.SUBMITTING_PARTY_NOT_KNOWN_ON_LEDGER
        }
    }

    for ((testDar, additionalContractValue) <- templateArgs) {
      s"accept transactions with unallocated parties in values: $testDar" in {
        val simplePackage = new SimplePackage(testDar)
        val command = simplePackage.simpleCreateCmd(
          simplePackage.mkSimpleTemplateArg("Alice", "Eve", additionalContractValue(simplePackage))
        )
        KVTest.runTestWithPackage(simplePackage, alice, eve) {
          val seed = hash(this.getClass.getName)
          for {
            transaction <- runCommand(alice, seed, command)
            txEntry <- submitTransaction(
              submitter = alice,
              transaction = transaction,
              submissionSeed = seed,
            ).map(_._2)
          } yield {
            txEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY
          }
        }
      }
    }

    "reject transactions with unallocated informee" in KVTest.runTestWithSimplePackage(alice, bob) {
      simplePackage =>
        val seed = hash(this.getClass.getName)
        for {
          transaction <- runSimpleCommand(alice, seed, simpleCreateCmd(simplePackage))
          txEntry1 <- submitTransaction(
            submitter = alice,
            transaction = transaction,
            submissionSeed = seed,
          )
            .map(_._2)
        } yield {
          txEntry1.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
          txEntry1.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.PARTIES_NOT_KNOWN_ON_LEDGER
        }
    }

    // FIXME: review this test
    "reject transactions for unhosted parties" in KVTest.runTestWithSimplePackage(bob, eve) {
      simplePackage =>
        val seed = hash(this.getClass.getName)
        for {
          configEntry <- submitConfig { c =>
            c.copy(generation = c.generation + 1)
          }
          createTx <- withParticipantId(p1)(
            runSimpleCommand(alice, seed, simpleCreateCmd(simplePackage))
          )

          newParty <- withParticipantId(p1)(allocateParty("unhosted", alice))
          txEntry1 <- withParticipantId(p0)(
            submitTransaction(submitter = newParty, createTx, seed).map(_._2)
          )
          txEntry2 <- withParticipantId(p1)(
            submitTransaction(submitter = newParty, transaction = createTx, submissionSeed = seed)
              .map(_._2)
          )

        } yield {
          configEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY
          txEntry1.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
          txEntry1.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.SUBMITTER_CANNOT_ACT_VIA_PARTICIPANT
          txEntry2.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY
        }
    }

    "reject unauthorized transactions" in KVTest.runTestWithSimplePackage(alice, eve) {
      simplePackage =>
        val seed = hash(this.getClass.getName)
        for {
          // Submit a creation of a contract with owner 'Alice', but submit it as 'Bob'.
          transaction <- runSimpleCommand(alice, seed, simpleCreateCmd(simplePackage))
          bob <- allocateParty("bob", bob)
          txEntry <- submitTransaction(
            submitter = bob,
            transaction = transaction,
            submissionSeed = seed,
          )
            .map(_._2)
        } yield {
          txEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY
          txEntry.getTransactionRejectionEntry.getReasonCase shouldEqual DamlTransactionRejectionEntry.ReasonCase.VALIDATION_FAILURE
        }
    }

    "update metrics" in KVTest.runTestWithSimplePackage(alice, eve) { simplePackage =>
      val seed = hash(this.getClass.getName)
      for {
        // Submit a creation of a contract with owner 'Alice', but submit it as 'Bob'.
        transaction <- runSimpleCommand(alice, seed, simpleCreateCmd(simplePackage))
        bob <- allocateParty("bob", bob)
        _ <- submitTransaction(submitter = bob, transaction = transaction, submissionSeed = seed)
          .map(_._2)
      } yield {
        val disputed = DamlTransactionRejectionEntry.ReasonCase.VALIDATION_FAILURE
        // Check that we're updating the metrics (assuming this test at least has been run)
        metrics.daml.kvutils.committer.transaction.accepts.getCount should be >= 1L
        metrics.daml.kvutils.committer.transaction.rejection(disputed.name).getCount should be >= 1L
        metrics.daml.kvutils.committer.runTimer("transaction").getCount should be >= 1L
        metrics.daml.kvutils.committer.transaction.interpretTimer.getCount should be >= 1L
        metrics.daml.kvutils.committer.transaction.runTimer.getCount should be >= 1L
      }
    }

    "properly archive transient contracts and keys" in KVTest.runTestWithSimplePackage(
      alice,
      bob,
      eve,
    ) { simplePackage =>
      val seeds =
        LazyList
          .from(0)
          .map(i => seed(i))

      val simpleCreateAndExerciseCmd =
        simplePackage.simpleCreateAndExerciseArchiveCmd(mkSimpleCreateArg(simplePackage))

      for {
        tx1 <- runSimpleCommand(alice, seeds.head, simpleCreateAndExerciseCmd)
        createAndExerciseTx1 <- submitTransaction(alice, tx1, seeds.head).map(_._2)

        tx2 <- runSimpleCommand(alice, seeds(1), simpleCreateAndExerciseCmd)
        createAndExerciseTx2 <- submitTransaction(alice, tx2, seeds(1)).map(_._2)

        finalState <- scalaz.State.get[KVTestState]
      } yield {
        createAndExerciseTx1.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY
        createAndExerciseTx2.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.TRANSACTION_ENTRY

        // Check that all contracts and keys are in the archived state.
        finalState.damlState.foreach { case (_, v) =>
          v.getValueCase match {
            case DamlStateValue.ValueCase.CONTRACT_KEY_STATE =>
              v.getContractKeyState.getContractId shouldBe Symbol("empty")

            case DamlStateValue.ValueCase.CONTRACT_STATE =>
              val cs = v.getContractState
              cs.hasArchivedAt shouldBe true

            case _ =>
              succeed
          }
        }
      }
    }

    "allow for missing completion info in log entries" in KVTest.runTestWithSimplePackage(
      alice,
      bob,
      eve,
    ) { simplePackage =>
      val seed = hash(this.getClass.getName)
      for {
        transaction <- runSimpleCommand(alice, seed, simpleCreateCmd(simplePackage))
        result <- submitTransaction(
          submitter = alice,
          transaction = transaction,
          submissionSeed = seed,
        )
      } yield {
        val (entryId, entry) = result
        // Clear the submitter info from the log entry
        val strippedEntry = entry.toBuilder
        strippedEntry.getTransactionEntryBuilder.clearSubmitterInfo

        // Process into updates and verify
        val updates =
          KeyValueConsumption.logEntryToUpdate(
            entryId,
            strippedEntry.build,
            errorVersionSwitch,
          )(loggingContext)
        inside(updates) { case Seq(txAccepted: Update.TransactionAccepted) =>
          txAccepted.optCompletionInfo should be(None)
        }
      }
    }

    "use max deduplication duration as deduplication period" in KVTest.runTestWithSimplePackage(
      alice,
      bob,
      eve,
    ) { simplePackage =>
      val seed = hash(this.getClass.getName)
      val command = simpleCreateCmd(simplePackage)
      val maxDeduplicationDuration = Duration.ofHours(2)
      for {
        _ <- preExecuteConfig(existingConfig => {
          existingConfig.copy(
            generation = existingConfig.generation + 1,
            maxDeduplicationTime = maxDeduplicationDuration,
          )
        })
        transaction <- runSimpleCommand(alice, seed, command)
        preExecutionResult <- preExecuteTransaction(
          submitter = alice,
          transaction = transaction,
          submissionSeed = seed,
          deduplicationDuration = Duration.ofHours(1),
        ).map(_._2)
      } yield {
        preExecutionResult.successfulLogEntry.getTransactionEntry.getSubmitterInfo.getDeduplicationDuration shouldBe Conversions
          .buildDuration(maxDeduplicationDuration)
      }
    }
  }

  private def preExecuteCreateSimpleContract(
      submitter: Ref.Party,
      seed: Hash,
      simplePackage: SimplePackage,
  ): KVTest[ContractId] =
    for {
      recordTime <- currentRecordTime
      createTransaction <- runSimpleCommand(submitter, seed, simpleCreateCmd(simplePackage))
      creationResult <- preExecuteTransaction(
        submitter = submitter,
        transaction = createTransaction,
        submissionSeed = seed,
      )
      (entryId, preExecutionResult) = creationResult
    } yield contractIdOfCreateTransaction(
      KeyValueConsumption.logEntryToUpdate(
        entryId,
        preExecutionResult.successfulLogEntry,
        errorVersionSwitch,
        Some(recordTime),
      )(loggingContext)
    )

  private def prepareExerciseReplaceByKey(
      submitter: Ref.Party,
      simplePackage: SimplePackage,
  )(
      seed: Hash
  ): KVTest[DamlSubmission] =
    for {
      exerciseTransaction <- runSimpleCommand(
        submitter,
        seed,
        simplePackage.simpleExerciseReplaceByKeyCmd(submitter),
      )
      submission <- prepareTransactionSubmission(
        submitter = submitter,
        transaction = exerciseTransaction,
        submissionSeed = seed,
      )
    } yield submission

  private def prepareExerciseArchiveCmd(
      submitter: Ref.Party,
      simplePackage: SimplePackage,
      contractId: ContractId,
  )(
      seed: Hash
  ): KVTest[DamlSubmission] =
    for {
      exerciseTransaction <- runSimpleCommand(
        submitter,
        seed,
        simplePackage.simpleExerciseArchiveCmd(contractId),
      )
      submission <- prepareTransactionSubmission(
        submitter = submitter,
        transaction = exerciseTransaction,
        submissionSeed = seed,
      )
    } yield submission

  private def contractIdOfCreateTransaction(updates: Seq[Update]): ContractId =
    inside(updates) { case Seq(update: Update.TransactionAccepted) =>
      inside(update.transaction.nodes.values.toSeq) { case Seq(create: Node.Create) =>
        create.coid
      }
    }

  private def simpleCreateCmd(simplePackage: SimplePackage): ApiCommand =
    simplePackage.simpleCreateCmd(mkSimpleCreateArg(simplePackage))

  private def mkSimpleCreateArg(simplePackage: SimplePackage): Value =
    simplePackage.mkSimpleTemplateArg("Alice", "Eve", bobValue)

  private def seed(i: Int): Hash = hash(this.getClass.getName + i.toString)

  private def hash(s: String) = crypto.Hash.hashPrivateKey(s)
}
