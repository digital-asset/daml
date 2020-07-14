// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.Duration

import com.codahale.metrics.MetricRegistry
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting.PreExecutionResult
import com.daml.ledger.participant.state.v1._
import com.daml.lf.command.{Command, Commands}
import com.daml.lf.crypto
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.engine.Engine
import com.daml.lf.transaction.Transaction
import com.daml.metrics.Metrics
import scalaz.State
import scalaz.std.list._
import scalaz.syntax.traverse._

import scala.collection.JavaConverters._

final case class KVTestState(
    participantId: ParticipantId,
    recordTime: Timestamp,
    defaultConfig: Configuration,
    nextEntryId: Int,
    damlState: Map[DamlStateKey, DamlStateValue]) {}

object KVTest {

  import TestHelpers._
  import scalaz.State._

  type KVTest[A] = State[KVTestState, A]

  def initialTestState: KVTestState =
    KVTestState(
      participantId = mkParticipantId(0),
      recordTime = Timestamp.Epoch.addMicros(1000000),
      defaultConfig = theDefaultConfig,
      nextEntryId = 0,
      damlState = Map.empty
    )

  def runTest[A](test: KVTest[A]): A =
    test.eval(initialTestState)

  def runTestWithPackage[A](additionalContractDataTy: String, parties: Party*)(test: KVTest[A]): A =
    (for {
      _ <- uploadArchive(additionalContractDataTy)
      _ <- parties.toList.traverse(p => allocateParty(p, p))
      r <- test
    } yield r).eval(initialTestState)

  def runTestWithSimplePackage[A](parties: Party*)(test: KVTest[A]): A =
    runTestWithPackage(DefaultAdditionalContractDataTy, parties: _*)(test)

  def uploadArchive(additionalContractDataTy: String): KVTest[Unit] =
    for {
      archiveLogEntry <- submitArchives(
        "simple-archive-submission",
        archiveWithContractData(additionalContractDataTy)).map(_._2)
      _ = assert(archiveLogEntry.getPayloadCase == DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_ENTRY)
    } yield ()

  def uploadSimpleArchive: KVTest[Unit] = uploadArchive(DefaultAdditionalContractDataTy)

  def freshEntryId: KVTest.KVTest[DamlLogEntryId] =
    for {
      s <- get
      _ <- modify[KVTestState](s => s.copy(nextEntryId = s.nextEntryId + 1))
    } yield TestHelpers.mkEntryId(s.nextEntryId)

  def setParticipantId(pid: ParticipantId): KVTest[Unit] =
    modify(_.copy(participantId = pid))

  def withParticipantId[A](pid: ParticipantId)(test: KVTest[A]): KVTest[A] =
    for {
      oldState <- get
      _ <- modify[KVTestState](_.copy(participantId = pid))
      x <- test
      _ <- modify[KVTestState](_.copy(participantId = oldState.participantId))
    } yield x

  def getDefaultConfiguration: KVTest[Configuration] =
    gets(_.defaultConfig)

  def setDefaultConfiguration(config: Configuration): KVTest[Unit] =
    modify(_.copy(defaultConfig = config))

  def getConfiguration: KVTest[Configuration] =
    getDamlState(Conversions.configurationStateKey)
      .flatMap {
        case None => getDefaultConfiguration
        case Some(v) =>
          State.state(Configuration.decode(v.getConfigurationEntry.getConfiguration).right.get)
      }

  def setRecordTime(rt: Timestamp): KVTest[Unit] =
    modify(_.copy(recordTime = rt))

  def advanceRecordTime(micros: Long): KVTest[Unit] =
    modify(s => s.copy(recordTime = s.recordTime.addMicros(micros)))

  def addDamlState(newState: Map[DamlStateKey, DamlStateValue]): KVTest[Unit] =
    modify(s => s.copy(damlState = s.damlState ++ newState))

  def getDamlState(key: DamlStateKey): KVTest[Option[DamlStateValue]] =
    gets(s => s.damlState.get(key))

  def submitArchives(
      submissionId: String,
      archives: DamlLf.Archive*
  ): KVTest[(DamlLogEntryId, DamlLogEntry)] =
    get.flatMap { testState =>
      submit(
        createArchiveSubmission(submissionId, testState, archives: _*)
      )
    }

  def preExecuteArchives(
      submissionId: String,
      archives: DamlLf.Archive*
  ): KVTest[(DamlLogEntryId, PreExecutionResult)] =
    get.flatMap { testState =>
      preExecute(
        createArchiveSubmission(submissionId, testState, archives: _*)
      )
    }

  def runCommand(
      submitter: Party,
      submissionSeed: crypto.Hash,
      additionalContractDataTy: String,
      cmds: Command*,
  ): KVTest[(SubmittedTransaction, Transaction.Metadata)] =
    for {
      s <- get[KVTestState]
      (tx, meta) = engine
        .submit(
          cmds = Commands(
            submitter = submitter,
            commands = ImmArray(cmds),
            ledgerEffectiveTime = s.recordTime,
            commandsReference = "cmds-ref",
          ),
          participantId = s.participantId,
          submissionSeed = submissionSeed,
        )
        .consume(
          { coid =>
            s.damlState
              .get(Conversions.contractIdToStateKey(coid))
              .map { v =>
                Conversions.decodeContractInstance(v.getContractState.getContractInstance)
              }
          }, { _ =>
            Some(decodedPackageWithContractData(additionalContractDataTy))
          }, { _ =>
            sys.error("no keys")
          }
        )
        .getOrElse(sys.error("Engine.submit fail"))
    } yield tx -> meta

  def runSimpleCommand(
      submitter: Party,
      submissionSeed: crypto.Hash,
      cmds: Command*,
  ): KVTest[(SubmittedTransaction, Transaction.Metadata)] =
    runCommand(submitter, submissionSeed, DefaultAdditionalContractDataTy, cmds: _*)

  def submitTransaction(
      submitter: Party,
      transaction: (SubmittedTransaction, Transaction.Metadata),
      submissionSeed: crypto.Hash,
      letDelta: Duration = Duration.ZERO,
      commandId: CommandId = randomLedgerString,
      deduplicationTime: Duration = Duration.ofDays(1)): KVTest[(DamlLogEntryId, DamlLogEntry)] =
    for {
      testState <- get[KVTestState]
      submInfo = createSubmitterInfo(submitter, commandId, deduplicationTime, testState)
      (tx, txMetaData) = transaction
      subm = transactionToSubmission(submissionSeed, letDelta, testState, submInfo, tx, txMetaData)
      result <- submit(subm)
    } yield result

  def preExecuteTransaction(
      submitter: Party,
      transaction: (SubmittedTransaction, Transaction.Metadata),
      submissionSeed: crypto.Hash,
      letDelta: Duration = Duration.ZERO,
      commandId: CommandId = randomLedgerString,
      deduplicationTime: Duration = Duration.ofDays(1))
    : KVTest[(DamlLogEntryId, PreExecutionResult)] =
    for {
      testState <- get[KVTestState]
      submInfo = createSubmitterInfo(submitter, commandId, deduplicationTime, testState)
      (tx, txMetaData) = transaction
      subm = transactionToSubmission(submissionSeed, letDelta, testState, submInfo, tx, txMetaData)
      result <- preExecute(subm)
    } yield result

  def submitConfig(
      configModify: Configuration => Configuration,
      submissionId: SubmissionId = randomLedgerString,
      mrtDelta: Duration = MinMaxRecordTimeDelta
  ): KVTest[DamlLogEntry] =
    for {
      testState <- get[KVTestState]
      oldConf <- getConfiguration
      result <- submit(
        createConfigurationSubmission(configModify, submissionId, mrtDelta, testState, oldConf)
      )
    } yield result._2

  def preExecuteConfig(
      configModify: Configuration => Configuration,
      submissionId: SubmissionId = randomLedgerString,
      mrtDelta: Duration = MinMaxRecordTimeDelta
  ): KVTest[PreExecutionResult] =
    for {
      testState <- get[KVTestState]
      oldConf <- getConfiguration
      result <- preExecute(
        createConfigurationSubmission(configModify, submissionId, mrtDelta, testState, oldConf)
      )
    } yield result._2

  def submitPartyAllocation(
      subId: String,
      hint: String,
      participantId: ParticipantId): KVTest[DamlLogEntry] =
    submit(
      createPartySubmission(subId, hint, participantId)
    ).map(_._2)

  def preExecutePartyAllocation(
      subId: String,
      hint: String,
      participantId: ParticipantId): KVTest[PreExecutionResult] =
    preExecute(
      createPartySubmission(subId, hint, participantId)
    ).map(_._2)

  def allocateParty(subId: String, hint: String): KVTest[Party] =
    for {
      testState <- get[KVTestState]
      result <- submitPartyAllocation(subId, hint, testState.participantId).map { logEntry =>
        assert(logEntry.getPayloadCase == DamlLogEntry.PayloadCase.PARTY_ALLOCATION_ENTRY)
        Ref.Party.assertFromString(logEntry.getPartyAllocationEntry.getParty)
      }
    } yield result

  private[kvutils] val metrics = new Metrics(new MetricRegistry)

  private def submit(submission: DamlSubmission): KVTest[(DamlLogEntryId, DamlLogEntry)] =
    for {
      testState <- get[KVTestState]
      entryId <- freshEntryId
      (logEntry, newState) = keyValueCommitting.processSubmission(
        entryId = entryId,
        recordTime = testState.recordTime,
        defaultConfig = testState.defaultConfig,
        submission = submission,
        participantId = testState.participantId,
        inputState = submission.getInputDamlStateList.asScala.map { key =>
          key -> testState.damlState.get(key)
        }.toMap,
      )
      _ <- addDamlState(newState)
    } yield {
      // Verify that all state touched matches with "submissionOutputs".
      assert(
        newState.keySet subsetOf keyValueCommitting.submissionOutputs(submission)
      )
      // Verify that we can always process the log entry
      val _ = KeyValueConsumption.logEntryToUpdate(entryId, logEntry)

      entryId -> logEntry
    }

  private def preExecute(
      damlSubmission: DamlSubmission): KVTest[(DamlLogEntryId, PreExecutionResult)] =
    for {
      testState <- get[KVTestState]
      entryId <- freshEntryId
      inputKeys = damlSubmission.getInputDamlStateList.asScala
      preExecutionResult @ PreExecutionResult(
        readSet,
        successfulLogEntry,
        newState,
        outOfTimeBoundsLogEntry,
        _,
        _,
        _) = keyValueCommitting
        .preExecuteSubmission(
          defaultConfig = testState.defaultConfig,
          submission = damlSubmission,
          participantId = testState.participantId,
          inputState = inputKeys.map { key =>
            {
              val damlStateValue = testState.damlState
                .get(key)
              key -> (damlStateValue -> damlStateValue
                .map(_.toByteString)
                .getOrElse(FingerprintPlaceholder))
            }
          }.toMap,
        )
      _ <- addDamlState(newState)
    } yield {
      assert(
        readSet.keySet subsetOf inputKeys.toSet
      )
      // Verify that we can always process both the successful and rejection log entries
      KeyValueConsumption.logEntryToUpdate(
        entryId,
        successfulLogEntry,
        recordTimeFromTimeUpdateLogEntry)
      KeyValueConsumption.logEntryToUpdate(
        entryId,
        outOfTimeBoundsLogEntry,
        recordTimeFromTimeUpdateLogEntry)

      entryId -> preExecutionResult
    }

  private[this] val MinMaxRecordTimeDelta: Duration = Duration.ofSeconds(1)
  private[this] val DefaultAdditionalContractDataTy = "Party"
  private[this] val engine = Engine.DevEngine()
  private[this] val keyValueCommitting = new KeyValueCommitting(engine, metrics)
  private[this] val keyValueSubmission = new KeyValueSubmission(metrics)

  private[this] def createSubmitterInfo(
      submitter: Party,
      commandId: CommandId,
      deduplicationTime: Duration,
      testState: KVTestState): SubmitterInfo =
    SubmitterInfo(
      submitter = submitter,
      applicationId = Ref.LedgerString.assertFromString("test"),
      commandId = commandId,
      deduplicateUntil = testState.recordTime.addMicros(deduplicationTime.toNanos / 1000).toInstant,
    )

  private[this] def transactionToSubmission(
      submissionSeed: Hash,
      letDelta: Duration,
      testState: KVTestState,
      submInfo: SubmitterInfo,
      tx: SubmittedTransaction,
      txMetaData: Transaction.Metadata): DamlSubmission =
    keyValueSubmission.transactionToSubmission(
      submitterInfo = submInfo,
      meta = TransactionMeta(
        ledgerEffectiveTime = testState.recordTime.addMicros(letDelta.toNanos / 1000),
        workflowId = None,
        submissionTime = txMetaData.submissionTime,
        submissionSeed = submissionSeed,
        optUsedPackages = Some(txMetaData.usedPackages),
        optNodeSeeds = None,
        optByKeyNodes = None,
      ),
      tx = tx
    )

  private[this] def createPartySubmission(
      subId: String,
      hint: String,
      participantId: ParticipantId): DamlSubmission =
    keyValueSubmission.partyToSubmission(
      Ref.LedgerString.assertFromString(subId),
      Some(hint),
      None,
      participantId)

  private[this] def createConfigurationSubmission(
      configModify: Configuration => Configuration,
      submissionId: SubmissionId,
      mrtDelta: Duration,
      testState: KVTestState,
      oldConf: Configuration): DamlSubmission =
    keyValueSubmission.configurationToSubmission(
      maxRecordTime = testState.recordTime.addMicros(mrtDelta.toNanos / 1000),
      submissionId = submissionId,
      participantId = testState.participantId,
      config = configModify(oldConf)
    )

  private[this] def createArchiveSubmission(
      submissionId: String,
      testState: KVTestState,
      archives: DamlLf.Archive*): DamlSubmission =
    keyValueSubmission.archivesToSubmission(
      submissionId = submissionId,
      archives = archives.toList,
      sourceDescription = "description",
      participantId = testState.participantId
    )

  private[this] def recordTimeFromTimeUpdateLogEntry = Some(Timestamp.now())
}
