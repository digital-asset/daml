// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.crashrecovery

import com.digitalasset.canton.HasTempDirectory
import com.digitalasset.canton.admin.api.client.data.ParticipantStatus.SubmissionReady
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.sync.SyncServiceError.{
  SyncServiceSynchronizerDisabledUs,
  SyncServiceSynchronizerDisconnect,
}
import com.digitalasset.canton.participant.synchronizer.SynchronizerRegistryError.ConnectionErrors.FailedToConnectToSequencers
import com.digitalasset.canton.sequencing.client.ResilientSequencerSubscription
import com.digitalasset.canton.sequencing.client.ResilientSequencerSubscription.LostSequencerSubscription
import org.scalatest.Assertion

import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}
import scala.util.Try

final class SynchronizerRecoveryTest extends BaseSynchronizerRestartTest with HasTempDirectory {

  def dropSynchronizerStorage(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    clue("Dropping synchronizer storage ...") {
      await(sequencerPlugin.recreateDatabases())
      await(postgresPlugin.recreateDatabase(remoteSequencer1).failOnShutdown)
      await(postgresPlugin.recreateDatabase(mediator1).failOnShutdown)
      remoteSequencer1.clear_cache()
      mediator1.clear_cache()
    }
  }

  def bootstrapSynchronizer(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    clue("Waiting for nodes to be running before bootstrapping ...") {
      mediator1.health.wait_for_ready_for_initialization()
      remoteSequencer1.health.wait_for_ready_for_initialization()
      networkBootstrapper(env).bootstrap()
    }
  }

  private val commonLogEntriesWhichMayOccur = Seq[LogEntry => Assertion](
    _.message should include("CANCELLED"),
    _.message should include("UNAVAILABLE"),
    _.message should (include("UNAUTHENTICATED") and include(
      "not match the synchronizer id of the synchronizer the participant"
    )),
    // This is similar to the line above: the connection pool catches mismatches while validating connections
    _.message should include("Sequencer connection has changed attributes"),
    _.message should (include(SyncServiceSynchronizerDisconnect.id) and include(
      "fatally disconnected because of Trust threshold 1 is no longer reachable"
    )),
    _.message should (include("PERMISSION_DENIED") and include("access is disabled")),
    _.message should include(LostSequencerSubscription.id),
    _.message should include(SyncServiceSynchronizerDisabledUs.id),
    _.message should include("Token refresh aborted due to shutdown."),
  )

  private def expectedLogsForConnectionFailure(usingConnectionPool: Boolean) = {
    val seq = Seq[(LogEntry => Assertion, String)](
      (_.shouldBeCommandFailure(FailedToConnectToSequencers), "Failure to connect")
    )
    if (usingConnectionPool)
      (
        (logEntry: LogEntry) =>
          logEntry.warningMessage should include(
            "Validation failure: Connection is not on expected sequencer"
          ),
        "Connection validation failure",
      ) +: seq
    else seq
  }

  "if synchronizer loses all state, it will consider previously connected participant disabled" in {
    implicit env: TestConsoleEnvironment =>
      import env.*

      clue("Start and connect all participants")(
        startAndConnectAllParticipants(remoteSequencer1)
      )

      clue("Assert that participant1 can ping participant2")(
        participant1.health.ping(participant2.id)
      )

      clue("Restart and assert logs ...")(
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            clue("Restart synchronizer with dropped synchronizer storage")(
              restart(
                remoteSequencer1,
                Some(mediator1),
                afterKill = dropSynchronizerStorage,
                afterStart = bootstrapSynchronizer,
              )
            )
            clue("Check all participants are disconnected")(
              // after losing state, synchronizer considers participants disabled and causes participants to disconnect
              checkAllDisconnected(remoteSequencer1, 60.seconds)
            )
          },
          LogEntry.assertLogSeq(
            Seq.empty,
            commonLogEntriesWhichMayOccur,
          ),
        )
      )

      val usingConnectionPool = participant1.config.sequencerClient.useNewConnectionPool

      clue("Reconnect and assert throwable and logs ...")(
        loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
          participant1.synchronizers.reconnect(daName),
          LogEntry.assertLogSeq(expectedLogsForConnectionFailure(usingConnectionPool)),
        )
      )

      clue("Reconnect_all and assert throwable and logs ...")(
        loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
          participant1.synchronizers.reconnect_all(ignoreFailures = false),
          LogEntry.assertLogSeq(expectedLogsForConnectionFailure(usingConnectionPool)),
        )
      )
  }

  "if synchronizer loses all state, participant that has not connected before can still connect" in {
    implicit env: TestConsoleEnvironment =>
      import env.*

      clue("Connect participant1 to the synchronizer")(
        connectSynchronizer(daName, remoteSequencer1, participant1)
      )

      clue("Assert participant1 is enabled and working")(
        participant1.health.ping(participant1)
      )

      clue("Restart and assert logs ...")(
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            clue("Restart synchronizer with dropped synchronizer storage")(
              restart(
                remoteSequencer1,
                Some(mediator1),
                afterKill = dropSynchronizerStorage,
                afterStart = bootstrapSynchronizer,
              )
            )
            clue("Check all participants are disconnected")(
              // after losing state, synchronizer considers participants disabled and causes participants to disconnect
              checkAllDisconnected(remoteSequencer1, 60.seconds)
            )
          },
          LogEntry.assertLogSeq(
            Seq.empty,
            commonLogEntriesWhichMayOccur,
          ),
        )
      )

      val usingConnectionPool = participant1.config.sequencerClient.useNewConnectionPool

      clue("Reconnect participant1 to the synchronizer and assert that its connection is corrupt")(
        loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
          participant1.synchronizers.reconnect(daName),
          LogEntry.assertLogSeq(expectedLogsForConnectionFailure(usingConnectionPool)),
        )
      )

      clue("Assert participant2 can still connect to the synchronizer")(
        connectSynchronizer(daName, remoteSequencer1, participant2)
      )

      clue("Assert health status of participants and sequencer")(
        eventually() {
          val status = health.status()
          val participantStatus = status.participantStatus
          val sequencerStatus = remoteSequencer1.health.status.trySuccess

          sequencerStatus.connectedParticipants should contain(
            participantStatus(participant2.name).id
          )
          participantStatus(participant1.name).connectedSynchronizers shouldBe empty
          participantStatus(participant2.name).connectedSynchronizers should contain(
            sequencerStatus.synchronizerId -> SubmissionReady(true)
          )
        }
      )

      clue("Assert participant2 is enabled and working")(
        participant2.health.ping(participant2.id)
      )
  }

  "if synchronizer loses some of the events, participant will fail to reconnect" in {
    implicit env: TestConsoleEnvironment =>
      import env.*

      clue("Start and connect all participants")(
        startAndConnectAllParticipants(remoteSequencer1)
      )

      clue("Assert that participant1 can ping participant2")(
        participant1.health.ping(participant2.id)
      )

      clue("Restart and assert logs ...")(
        loggerFactory.assertLoggedWarningsAndErrorsSeq[Unit](
          {
            val sequencerDump = tempDirectory.toTempFile("db-dump-sequencer.tar")
            val mediatorDump = tempDirectory.toTempFile("db-dump-mediator.tar")

            clue("Create backup of synchronizer data") {
              await(
                postgresDumpRestore.saveDump(mediator1, mediatorDump)
              )
              await(
                postgresDumpRestore.saveDump(remoteSequencer1, sequencerDump)
              )
              await(
                sequencerPlugin.dumpDatabases(tempDirectory)
              )
            }

            def restoreDumps(): Unit = {
              await(sequencerPlugin.restoreDatabases(tempDirectory))
              await(postgresDumpRestore.restoreDump(remoteSequencer1, sequencerDump.path))
              await(postgresDumpRestore.restoreDump(mediator1, mediatorDump.path))
            }

            clue("Create more events after backup")(
              participant1.health.ping(participant2.id)
            )

            clue("Restart synchronizer and restore dumps")(
              restart(remoteSequencer1, Some(mediator1), afterKill = restoreDumps())
            )

            // after synchronizer restarts, participants will attempt to reconnect and on each attempt request a new synchronizer
            // timestamp. this will kick the sequencer to create more events for this member and this will eventually
            // cause the event stream to reach the timestamp that the participant has subscribed from (which is ahead of
            // where the synchronizer was restored at). Upon receiving a different event for its subscription it will
            // notice the synchronizer has "forked" and will disconnect.
            clue("Check all participants are disconnected")(
              checkAllDisconnected(
                remoteSequencer1,
                60.seconds,
                () => Try(participants.local.foreach(_.testing.fetch_synchronizer_times())),
              )
            )
            ()
          },
          logEntries => {
            // Make sure there is no unrelated/unexpected error
            forEvery(logEntries) { logEntry =>
              if (logEntry.message.contains("TRANSIENT_FAILURE")) {
                // first off, the client will get a cancelled signal from the server due to the forced crash
                succeed
              } else if (logEntry.message.contains("CANCELLED")) {
                // first off, the client will get a cancelled signal from the server due to the forced crash
                succeed
              } else if (logEntry.message.contains("UNAVAILABLE")) {
                // after the server has crashed, the client complains that the server is unavailable.
                succeed
              } else if (logEntry.message.contains("AcknowledgementFailed")) {
                // If a client reconnects before the server has generated further events,
                // the server will complain that the client tries to acknowledge an non-existing event.
                succeed
              } else if (logEntry.message.contains(LostSequencerSubscription.id)) {
                // the resilient sequencer subscription will complain about the lost message
                succeed
              } else if (
                logEntry.message.contains(ResilientSequencerSubscription.ForkHappened.id)
              ) {
                // Additionally to the handler event below, the system will emit an appropriate error code with instructions
                succeed
              } else if (logEntry.message.contains(SyncServiceSynchronizerDisconnect.id)) {
                succeed
              } else if (logEntry.message.contains("EventValidationError")) {
                succeed
              } else if (logEntry.message.contains("ForkHappened")) {
                // If a client reconnects after the server has generated further events,
                // the acknowledgement will succeed, but the client will detect a fork.
                succeed
              } else if (logEntry.throwable.exists(_.getMessage.contains("ForkHappened"))) {
                succeed
              } else if (logEntry.commandFailureMessage contains "FetchTime") {
                // a handful of the fetchTime requests will exceed their deadline while the synchronizer is coming up
                succeed
              } else {
                // NB: if you are adding assertions here, make sure to add these BEFORE
                // the logEntry.throwable / logEntry.commandFailureMessage above,
                // otherwise these will not be considered as these do fail() internally.
                fail(s"Unexpected log entry: $logEntry")
              }
            }

            def participantIsInformed(participantRef: String): Assertion =
              forAtLeast(1, logEntries) { logEntry =>
                logEntry.loggerName should include(s"participant=$participantRef")
              // The details of the log message depend on the exact interleaving.
              }

            participantIsInformed("participant1")
            participantIsInformed("participant2")
          },
        )
      )
  }

  def await[A](f: Future[A]): A = Await.result(f, 30.seconds)
}
