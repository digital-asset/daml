// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.Duration

import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.command.{Command, Commands}
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml_lf_dev.DamlLf
import scalaz.State
import scalaz.syntax.traverse._
import scalaz.std.list._

import scala.collection.JavaConverters._

final case class KVTestState(
    engine: Engine,
    participantId: ParticipantId,
    recordTime: Timestamp,
    defaultConfig: Configuration,
    nextEntryId: Int,
    damlState: Map[DamlStateKey, DamlStateValue]) {}

object KVTest {
  import scalaz.State._
  import TestHelpers._

  type KVTest[A] = State[KVTestState, A]

  def initialTestState: KVTestState =
    KVTestState(
      engine = Engine(),
      participantId = mkParticipantId(0),
      recordTime = Timestamp.Epoch.addMicros(1000000),
      defaultConfig = theDefaultConfig,
      nextEntryId = 0,
      damlState = Map.empty
    )

  def runTest[A](test: KVTest[A]): A =
    test.eval(initialTestState)

  def runTestWithSimplePackage[A](parties: Party*)(test: KVTest[A]): A =
    (for {
      _ <- uploadSimpleArchive
      _ <- parties.toList.map(p => allocateParty(p, p)).sequenceU
      r <- test
    } yield r).eval(initialTestState)

  def uploadSimpleArchive: KVTest[Unit] =
    for {
      archiveLogEntry <- submitArchives("simple-archive-submission", simpleArchive).map(_._2)
      _ = assert(archiveLogEntry.getPayloadCase == DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_ENTRY)
    } yield ()

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

  def submit(submission: DamlSubmission): KVTest[(DamlLogEntryId, DamlLogEntry)] =
    for {
      testState <- get[KVTestState]
      entryId <- freshEntryId
      (logEntry, newState) = KeyValueCommitting.processSubmission(
        engine = testState.engine,
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
        newState.keySet subsetOf
          KeyValueCommitting.submissionOutputs(entryId, submission)
      )

      // Verify that we can always process the log entry
      val _ = KeyValueConsumption.logEntryToUpdate(entryId, logEntry)

      entryId -> logEntry
    }

  def submitArchives(
      submissionId: String,
      archives: DamlLf.Archive*
  ): KVTest[(DamlLogEntryId, DamlLogEntry)] =
    get.flatMap { testState =>
      submit(
        KeyValueSubmission.archivesToSubmission(
          submissionId = submissionId,
          archives = archives.toList,
          sourceDescription = "description",
          participantId = testState.participantId
        )
      )
    }

  val minMRTDelta: Duration = theDefaultConfig.timeModel.minTtl

  private val participant = Ref.ParticipantId.assertFromString("participant")

  def runCommand(submitter: Party, cmds: Command*): KVTest[SubmittedTransaction] =
    for {
      s <- get[KVTestState]
      tx = s.engine
        .submit(
          cmds = Commands(
            submitter = submitter,
            commands = ImmArray(cmds),
            ledgerEffectiveTime = s.recordTime,
            commandsReference = "cmds-ref",
          ),
          participantId = participant,
          submissionSeed = None,
        )
        .consume(
          { coid =>
            s.damlState
              .get(Conversions.absoluteContractIdToStateKey(coid))
              .map { v =>
                Conversions.decodeContractInstance(v.getContractState.getContractInstance)
              }
          }, { pkgId =>
            Some(simpleDecodedPackage)
          }, { _ =>
            sys.error("no keys")
          }
        )
        .getOrElse(sys.error("Engine.submit fail"))
    } yield tx

  def submitTransaction(
      submitter: Party,
      tx: SubmittedTransaction,
      mrtDelta: Duration = minMRTDelta,
      letDelta: Duration = Duration.ZERO,
      commandId: CommandId = randomLedgerString): KVTest[(DamlLogEntryId, DamlLogEntry)] =
    for {
      testState <- get[KVTestState]
      submInfo = SubmitterInfo(
        submitter = submitter,
        applicationId = Ref.LedgerString.assertFromString("test"),
        commandId = commandId,
        maxRecordTime = testState.recordTime.addMicros(mrtDelta.toNanos / 1000)
      )
      subm = KeyValueSubmission.transactionToSubmission(
        submitterInfo = submInfo,
        meta = TransactionMeta(
          ledgerEffectiveTime = testState.recordTime.addMicros(letDelta.toNanos / 1000),
          workflowId = None
        ),
        tx = tx
      )
      result <- submit(subm)
    } yield result

  def submitConfig(
      configModify: Configuration => Configuration,
      submissionId: SubmissionId = randomLedgerString,
      mrtDelta: Duration = minMRTDelta
  ): KVTest[DamlLogEntry] =
    for {
      testState <- get[KVTestState]
      oldConf <- getConfiguration
      result <- submit(
        KeyValueSubmission.configurationToSubmission(
          maxRecordTime = testState.recordTime.addMicros(mrtDelta.toNanos / 1000),
          submissionId = submissionId,
          participantId = testState.participantId,
          config = configModify(oldConf)
        )
      )
    } yield result._2

  def submitPartyAllocation(
      subId: String,
      hint: String,
      participantId: ParticipantId): KVTest[DamlLogEntry] =
    submit(
      KeyValueSubmission.partyToSubmission(
        Ref.LedgerString.assertFromString(subId),
        Some(hint),
        None,
        participantId)
    ).map(_._2)

  def allocateParty(subId: String, hint: String): KVTest[Party] =
    for {
      testState <- get[KVTestState]
      result <- submitPartyAllocation(subId, hint, testState.participantId).map { logEntry =>
        assert(logEntry.getPayloadCase == DamlLogEntry.PayloadCase.PARTY_ALLOCATION_ENTRY)
        Ref.Party.assertFromString(logEntry.getPartyAllocationEntry.getParty)
      }
    } yield result

}
