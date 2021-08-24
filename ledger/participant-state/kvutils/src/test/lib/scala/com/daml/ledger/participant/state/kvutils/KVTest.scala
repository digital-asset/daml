// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.Duration

import com.codahale.metrics.MetricRegistry
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting.PreExecutionResult
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmission
import com.daml.ledger.participant.state.v2.{SubmitterInfo, TransactionMeta}
import com.daml.ledger.test.SimplePackagePartyTestDar
import com.daml.lf.command.{ApiCommand, Commands}
import com.daml.lf.crypto
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.engine.Engine
import com.daml.lf.language.Ast
import com.daml.lf.transaction.{SubmittedTransaction, Transaction}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import scalaz.std.list._
import scalaz.syntax.traverse._
import scalaz.{Reader, State}

import scala.jdk.CollectionConverters._

final case class KVTestState(
    participantId: Ref.ParticipantId,
    recordTime: Timestamp,
    defaultConfig: Configuration,
    nextEntryId: Int,
    engine: Engine,
    keyValueSubmission: KeyValueSubmission,
    keyValueCommitting: KeyValueCommitting,
    uploadedPackages: Map[Ref.PackageId, Ast.Package],
    damlState: Map[DamlStateKey, DamlStateValue],
)

object KVTest {

  import TestHelpers._
  import scalaz.State._

  type KVTest[A] = State[KVTestState, A]

  // This returns `State`, not `Reader`, to avoid having to convert between them.
  def KVReader[A](f: KVTestState => A): KVTest[A] =
    Reader(f).state

  private[this] val MinMaxRecordTimeDelta: Duration = Duration.ofSeconds(1)
  private[this] val DefaultSimplePackage: SimplePackage = new SimplePackage(
    SimplePackagePartyTestDar
  )

  private[kvutils] val metrics = new Metrics(new MetricRegistry)

  private def initialTestState: KVTestState = {
    val engine = Engine.DevEngine()
    KVTestState(
      participantId = mkParticipantId(0),
      recordTime = Timestamp.Epoch.addMicros(1000000),
      defaultConfig = theDefaultConfig,
      nextEntryId = 0,
      engine = engine,
      keyValueSubmission = new KeyValueSubmission(metrics),
      keyValueCommitting = new KeyValueCommitting(engine, metrics),
      uploadedPackages = Map.empty,
      damlState = Map.empty,
    )
  }

  def sequentially[A](operations: Seq[KVTest[A]]): KVTest[List[A]] =
    operations.toList.sequence

  def inParallelReadOnly[A](operations: Seq[KVTest[A]]): KVTest[Seq[A]] =
    KVReader { state =>
      operations.map(_.eval(state))
    }

  def runTest[A](test: KVTest[A]): A =
    test.eval(initialTestState)

  def runTestWithPackage[A](
      simplePackage: SimplePackage,
      parties: Ref.Party*
  )(test: KVTest[A])(implicit loggingContext: LoggingContext): A =
    (for {
      _ <- uploadArchive(simplePackage)
      _ <- parties.toList.traverse(p => allocateParty(p, p))
      r <- test
    } yield r).eval(initialTestState)

  def runTestWithSimplePackage[A](
      parties: Ref.Party*
  )(test: SimplePackage => KVTest[A])(implicit loggingContext: LoggingContext): A =
    runTestWithPackage(DefaultSimplePackage, parties: _*)(test(DefaultSimplePackage))

  private def uploadArchive(
      simplePackage: SimplePackage
  )(implicit loggingContext: LoggingContext): KVTest[Unit] =
    for {
      archiveLogEntry <- submitArchives(
        "simple-archive-submission",
        simplePackage.archives.values.toSeq: _*
      ).map(_._2)
      _ = assert(archiveLogEntry.getPayloadCase == DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_ENTRY)
      _ <- modify[KVTestState](state =>
        state.copy(uploadedPackages = state.uploadedPackages ++ simplePackage.packages)
      )
    } yield ()

  def freshEntryId: KVTest.KVTest[DamlLogEntryId] =
    for {
      s <- get
      _ <- modify[KVTestState](s => s.copy(nextEntryId = s.nextEntryId + 1))
    } yield TestHelpers.mkEntryId(s.nextEntryId)

  def setParticipantId(pid: Ref.ParticipantId): KVTest[Unit] =
    modify(_.copy(participantId = pid))

  def withParticipantId[A](pid: Ref.ParticipantId)(test: KVTest[A]): KVTest[A] =
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
          State.state(Configuration.decode(v.getConfigurationEntry.getConfiguration).toOption.get)
      }

  def currentRecordTime: KVTest[Timestamp] =
    get[KVTestState].map(_.recordTime)

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
  )(implicit loggingContext: LoggingContext): KVTest[(DamlLogEntryId, DamlLogEntry)] =
    get.flatMap { testState =>
      submit(
        createArchiveSubmission(submissionId, testState, archives: _*)
      )
    }

  def preExecuteArchives(
      submissionId: String,
      archives: DamlLf.Archive*
  )(implicit loggingContext: LoggingContext): KVTest[(DamlLogEntryId, PreExecutionResult)] =
    get.flatMap { testState =>
      preExecute(
        createArchiveSubmission(submissionId, testState, archives: _*)
      )
    }

  def runCommand(
      submitter: Ref.Party,
      submissionSeed: crypto.Hash,
      command: ApiCommand,
  ): KVTest[(SubmittedTransaction, Transaction.Metadata)] =
    KVReader { state =>
      state.engine
        .submit(
          submitters = Set(submitter),
          readAs = Set.empty,
          cmds = Commands(
            commands = ImmArray(command),
            ledgerEffectiveTime = state.recordTime,
            commandsReference = "cmds-ref",
          ),
          participantId = state.participantId,
          submissionSeed = submissionSeed,
        )
        .consume(
          pcs = contractId =>
            state.damlState
              .get(Conversions.contractIdToStateKey(contractId))
              .map { v =>
                Conversions.decodeContractInstance(v.getContractState.getContractInstance)
              },
          packages = state.uploadedPackages.get,
          keys = globalKey =>
            state.damlState
              .get(Conversions.globalKeyToStateKey(globalKey.globalKey))
              .map(value => Conversions.decodeContractId(value.getContractKeyState.getContractId)),
        )
        .fold(error => throw new RuntimeException(error.message), identity)
    }

  def runSimpleCommand(
      submitter: Ref.Party,
      submissionSeed: crypto.Hash,
      command: ApiCommand,
  ): KVTest[(SubmittedTransaction, Transaction.Metadata)] =
    runCommand(submitter, submissionSeed, command)

  def submitTransaction(
      submitter: Ref.Party,
      transaction: (SubmittedTransaction, Transaction.Metadata),
      submissionSeed: crypto.Hash,
      letDelta: Duration = Duration.ZERO,
      commandId: Ref.CommandId = randomLedgerString,
      deduplicationTime: Duration = Duration.ofDays(1),
  )(implicit loggingContext: LoggingContext): KVTest[(DamlLogEntryId, DamlLogEntry)] =
    prepareTransactionSubmission(
      submitter,
      transaction,
      submissionSeed,
      letDelta,
      commandId,
      deduplicationTime,
    ).flatMap(submit)

  def preExecuteTransaction(
      submitter: Ref.Party,
      transaction: (SubmittedTransaction, Transaction.Metadata),
      submissionSeed: crypto.Hash,
      letDelta: Duration = Duration.ZERO,
      commandId: Ref.CommandId = randomLedgerString,
      deduplicationTime: Duration = Duration.ofDays(1),
  )(implicit loggingContext: LoggingContext): KVTest[(DamlLogEntryId, PreExecutionResult)] =
    prepareTransactionSubmission(
      submitter,
      transaction,
      submissionSeed,
      letDelta,
      commandId,
      deduplicationTime,
    ).flatMap(preExecute)

  def prepareTransactionSubmission(
      submitter: Ref.Party,
      transaction: (SubmittedTransaction, Transaction.Metadata),
      submissionSeed: crypto.Hash,
      letDelta: Duration = Duration.ZERO,
      commandId: Ref.CommandId = randomLedgerString,
      deduplicationTime: Duration = Duration.ofDays(1),
  ): KVTest[DamlSubmission] = KVReader { testState =>
    val (tx, txMetaData) = transaction
    val submitterInfo = createSubmitterInfo(
      submitter,
      commandId,
      deduplicationTime,
      randomLedgerString,
    )
    testState.keyValueSubmission.transactionToSubmission(
      submitterInfo = submitterInfo,
      meta = TransactionMeta(
        ledgerEffectiveTime = testState.recordTime.addMicros(letDelta.toNanos / 1000),
        workflowId = None,
        submissionTime = txMetaData.submissionTime,
        submissionSeed = submissionSeed,
        optUsedPackages = Some(txMetaData.usedPackages),
        optNodeSeeds = None,
        optByKeyNodes = None,
      ),
      tx = tx,
    )
  }

  def submitConfig(
      configModify: Configuration => Configuration,
      submissionId: Ref.SubmissionId = randomLedgerString,
      minMaxRecordTimeDelta: Duration = MinMaxRecordTimeDelta,
  )(implicit loggingContext: LoggingContext): KVTest[DamlLogEntry] =
    for {
      testState <- get[KVTestState]
      oldConf <- getConfiguration
      result <- submit(
        createConfigurationSubmission(
          configModify,
          submissionId,
          minMaxRecordTimeDelta,
          testState,
          oldConf,
        )
      )
    } yield result._2

  def preExecuteConfig(
      configModify: Configuration => Configuration,
      submissionId: Ref.SubmissionId = randomLedgerString,
      minMaxRecordTimeDelta: Duration = MinMaxRecordTimeDelta,
  )(implicit loggingContext: LoggingContext): KVTest[PreExecutionResult] =
    for {
      testState <- get[KVTestState]
      oldConf <- getConfiguration
      result <- preExecute(
        createConfigurationSubmission(
          configModify,
          submissionId,
          minMaxRecordTimeDelta,
          testState,
          oldConf,
        )
      )
    } yield result._2

  def submitPartyAllocation(
      subId: String,
      hint: String,
      participantId: Ref.ParticipantId,
  )(implicit loggingContext: LoggingContext): KVTest[DamlLogEntry] =
    get[KVTestState]
      .flatMap(testState => submit(createPartySubmission(subId, hint, participantId, testState)))
      .map(_._2)

  def preExecutePartyAllocation(
      subId: String,
      hint: String,
      participantId: Ref.ParticipantId,
  )(implicit loggingContext: LoggingContext): KVTest[PreExecutionResult] =
    get[KVTestState]
      .flatMap(testState =>
        preExecute(createPartySubmission(subId, hint, participantId, testState))
      )
      .map(_._2)

  def allocateParty(
      subId: String,
      hint: String,
  )(implicit loggingContext: LoggingContext): KVTest[Ref.Party] =
    for {
      testState <- get[KVTestState]
      result <- submitPartyAllocation(subId, hint, testState.participantId).map { logEntry =>
        assert(logEntry.getPayloadCase == DamlLogEntry.PayloadCase.PARTY_ALLOCATION_ENTRY)
        Ref.Party.assertFromString(logEntry.getPartyAllocationEntry.getParty)
      }
    } yield result

  private def submit(
      submission: DamlSubmission
  )(implicit loggingContext: LoggingContext): KVTest[(DamlLogEntryId, DamlLogEntry)] =
    for {
      testState <- get[KVTestState]
      entryId <- freshEntryId
      (logEntry, newState) = testState.keyValueCommitting.processSubmission(
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
        newState.keySet subsetOf KeyValueCommitting.submissionOutputs(submission)
      )
      // Verify that we can always process the log entry.
      val _ = KeyValueConsumption.logEntryToUpdate(entryId, logEntry)

      entryId -> logEntry
    }

  def preExecute(
      damlSubmission: DamlSubmission
  )(implicit loggingContext: LoggingContext): KVTest[(DamlLogEntryId, PreExecutionResult)] =
    for {
      testState <- get[KVTestState]
      entryId <- freshEntryId
      inputKeys = damlSubmission.getInputDamlStateList.asScala
      inputState <- createInputState(inputKeys)
      preExecutionResult = testState.keyValueCommitting.preExecuteSubmission(
        defaultConfig = testState.defaultConfig,
        submission = damlSubmission,
        participantId = testState.participantId,
        inputState = inputState,
      )
      PreExecutionResult(readSet, successfulLogEntry, newState, outOfTimeBoundsLogEntry, _, _) =
        preExecutionResult
      _ <- addDamlState(newState)
    } yield {
      assert(
        readSet subsetOf inputKeys.toSet
      )
      // Verify that we can always process both the successful and rejection log entries
      KeyValueConsumption.logEntryToUpdate(
        entryId,
        successfulLogEntry,
        recordTimeFromTimeUpdateLogEntry,
      )
      KeyValueConsumption.logEntryToUpdate(
        entryId,
        outOfTimeBoundsLogEntry,
        recordTimeFromTimeUpdateLogEntry,
      )

      entryId -> preExecutionResult
    }

  private[this] def createInputState(
      inputKeys: collection.Seq[DamlStateKey]
  ): KVTest[Map[DamlStateKey, Option[DamlStateValue]]] = KVReader { state =>
    inputKeys.view
      .map(key => key -> state.damlState.get(key))
      .toMap
  }

  private def createSubmitterInfo(
      submitter: Ref.Party,
      commandId: Ref.CommandId,
      deduplicationTime: Duration,
      submissionId: Ref.SubmissionId,
  ): SubmitterInfo =
    SubmitterInfo(
      actAs = List(submitter),
      applicationId = Ref.LedgerString.assertFromString("test"),
      commandId = commandId,
      deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(deduplicationTime),
      submissionId = submissionId,
      ledgerConfiguration = null,
    )

  private[this] def createPartySubmission(
      subId: String,
      hint: String,
      participantId: Ref.ParticipantId,
      testState: KVTestState,
  ): DamlSubmission =
    testState.keyValueSubmission.partyToSubmission(
      Ref.LedgerString.assertFromString(subId),
      Some(hint),
      None,
      participantId,
    )

  private[this] def createConfigurationSubmission(
      configModify: Configuration => Configuration,
      submissionId: Ref.SubmissionId,
      minMaxRecordTimeDelta: Duration,
      testState: KVTestState,
      oldConf: Configuration,
  ): DamlSubmission =
    testState.keyValueSubmission.configurationToSubmission(
      maxRecordTime = testState.recordTime.addMicros(minMaxRecordTimeDelta.toNanos / 1000),
      submissionId = submissionId,
      participantId = testState.participantId,
      config = configModify(oldConf),
    )

  private[this] def createArchiveSubmission(
      submissionId: String,
      testState: KVTestState,
      archives: DamlLf.Archive*
  ): DamlSubmission =
    testState.keyValueSubmission.archivesToSubmission(
      submissionId = submissionId,
      archives = archives.toList,
      sourceDescription = "description",
      participantId = testState.participantId,
    )

  private[this] def recordTimeFromTimeUpdateLogEntry: Option[Timestamp] =
    Some(Timestamp.now())
}
