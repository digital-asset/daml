// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.health.ComponentHealthState
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.sequencing.SequencerConnectionXPool.SequencerConnectionXPoolError
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.util.LoggerUtil
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext, config}
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level.{INFO, WARN}

import scala.concurrent.duration.*

class SequencerConnectionXPoolImplTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown
    with ConnectionPoolTestHelpers {

  import ConnectionPoolTestHelpers.*

  "SequencerConnectionXPool" should {
    "initialize in the happy path" in {
      withConnectionPool(
        nbConnections = PositiveInt.tryCreate(3),
        trustThreshold = PositiveInt.tryCreate(3),
        index => mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = index),
      ) { (pool, createdConnections, listener, _) =>
        val initializedF = pool.start()

        clue("Normal start") {
          initializedF.futureValueUS.valueOrFail("initialization")
          listener.shouldStabilizeOn(ComponentHealthState.Ok())
          pool.nbSequencers shouldBe NonNegativeInt.tryCreate(3)
          pool.physicalSynchronizerIdO shouldBe Some(testSynchronizerId(1))
        }

        clue("Stop connections non-fatally") {
          (0 to 1).foreach(createdConnections(_).fail(reason = "test"))
          // Connections are restarted
          listener.shouldStabilizeOn(ComponentHealthState.Ok())
        }

        clue("Stop connections fatally") {
          (0 to 1).foreach(createdConnections(_).fatal(reason = "test"))
          // Connections are not restarted
          listener.shouldStabilizeOn(
            ComponentHealthState.degraded(
              "only 1 connection(s) to different sequencers available, trust threshold = 3"
            )
          )
          pool.nbSequencers shouldBe NonNegativeInt.one
        }

        createdConnections(2).fatal(reason = "test")
        listener.shouldStabilizeOn(ComponentHealthState.failed("no connection available"))
      }
    }

    "signal when the threshold cannot be reached during validation" in {
      withConnectionPool(
        nbConnections = PositiveInt.tryCreate(3),
        trustThreshold = PositiveInt.tryCreate(2),
        // 3 connections all on the same sequencer ID -- the trust threshold of 2 is unreachable
        _ => mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = 1),
      ) { (pool, _, listener, _) =>
        inside(pool.start().futureValueUS) {
          case Left(SequencerConnectionXPoolError.ThresholdUnreachableError(message)) =>
            message shouldBe "Trust threshold of 2 is no longer reachable"
        }
        listener.shouldStabilizeOn(ComponentHealthState.failed("Component is closed"))
      }
    }

    "signal when the threshold cannot be reached upon closing with fatal" in {
      withConnectionPool(
        nbConnections = PositiveInt.tryCreate(3),
        trustThreshold = PositiveInt.tryCreate(2),
        // 3 connections on 3 different sequencer IDs
        attributesForConnection =
          index => mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = index),
        blockValidation = _ => true,
      ) { (pool, createdConnections, listener, _) =>
        val initializedF = pool.start()

        clue("Threshold remains reachable") {
          always(durationOfSuccess = 1.second) {
            initializedF.value.isCompleted shouldBe false
          }
        }

        createdConnections(0).fatal(reason = "test")
        clue("Threshold is still reachable") {
          always(durationOfSuccess = 1.second) {
            initializedF.value.isCompleted shouldBe false
          }
        }

        createdConnections(1).fatal(reason = "test")
        clue("Threshold is no longer reachable") {
          inside(initializedF.futureValueUS) {
            case Left(SequencerConnectionXPoolError.ThresholdUnreachableError(message)) =>
              message shouldBe s"Trust threshold of 2 is no longer reachable"
          }
        }

        listener.shouldStabilizeOn(ComponentHealthState.failed("Component is closed"))
      }
    }

    "signal when initialization times out" in {
      val testTimeout = config.NonNegativeDuration.tryFromDuration(2.seconds)

      withConnectionPool(
        nbConnections = PositiveInt.tryCreate(3),
        trustThreshold = PositiveInt.tryCreate(2),
        attributesForConnection =
          index => mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = index),
        testTimeouts = timeouts.copy(sequencerInfo = testTimeout),
        blockValidation = _ => true,
      ) { (pool, _, listener, _) =>
        inside(pool.start().futureValueUS) {
          case Left(SequencerConnectionXPoolError.TimeoutError(message)) =>
            message shouldBe s"Connection pool failed to initialize within ${LoggerUtil
                .roundDurationForHumans(testTimeout.duration)}"
        }
        listener.shouldStabilizeOn(ComponentHealthState.failed("Component is closed"))
      }
    }

    "retry a connection that fails to validate" in {
      // Test the following scenario involving restarts:
      //
      // - start
      // - getApi -> KO
      // - restart
      // - getApi -> OK, performHandshake -> KO
      // - restart
      // - getApi -> OK, performHandshake -> OK, getSynchronizerAndSequencerIds -> KO
      // - restart
      // - getApi -> OK, performHandshake -> OK, getSynchronizerAndSequencerIds -> OK, getStaticSynchronizerParameters -> KO
      // - restart
      // - getApi -> OK, performHandshake -> OK, getSynchronizerAndSequencerIds -> OK, getStaticSynchronizerParameters -> OK
      // - failure, triggering a restart
      // - getApi -> OK, performHandshake -> OK, getSynchronizerAndSequencerIds -> OK, getStaticSynchronizerParameters -> OK
      val testResponses = TestResponses(
        apiResponses = failureUnavailable +: Seq.fill(5)(correctApiResponse),
        handshakeResponses = failureUnavailable +: Seq.fill(4)(successfulHandshake),
        synchronizerAndSeqIdResponses =
          failureUnavailable +: Seq.fill(3)(correctSynchronizerIdResponse1),
        staticParametersResponses =
          failureUnavailable +: Seq.fill(2)(correctStaticParametersResponse),
      )

      val poolDelays = SequencerConnectionPoolDelays(
        minRestartDelay = NonNegativeFiniteDuration.tryOfMillis(100),
        maxRestartDelay = NonNegativeFiniteDuration.tryOfSeconds(10),
        warnValidationDelay = NonNegativeFiniteDuration.tryOfMillis(700), // 100 + 200 + 400 = 700
        subscriptionRequestDelay = NonNegativeFiniteDuration.tryOfSeconds(1),
      )

      withConnectionPool(
        nbConnections = PositiveInt.one,
        trustThreshold = PositiveInt.one,
        attributesForConnection =
          index => mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = index),
        responsesForConnection = { case 0 => testResponses },
        poolDelays = poolDelays,
      ) { (pool, createdConnections, listener, _) =>
        val minRestartConnectionDelay = poolDelays.minRestartDelay.duration.toMillis
        val exponentialDelays = (0 until 4).map(minRestartConnectionDelay << _)

        def retryLogEntry(delayMs: Long) =
          s"Scheduling restart after ${LoggerUtil.roundDurationForHumans(NonNegativeFiniteDuration.tryOfMillis(delayMs).toScala)}"

        val warningRegex =
          raw"""(?s)Connection has failed validation since \S+ \((\d+) (\w+) ago\). Last failure reason: "Network error: .*"""".r

        loggerFactory.assertLogsSeq(
          SuppressionRule.LevelAndAbove(INFO) && SuppressionRule.LoggerNameContains(
            "ConnectionHandler"
          )
        )(
          {
            pool.start().futureValueUS.valueOrFail("initialization")
            listener.shouldStabilizeOn(ComponentHealthState.Ok())
          },
          logEntries => {
            // 4 retries, due to one failure at each call
            // The retry delay is exponential
            logEntries
              .map(_.message)
              .filter(_.contains("Scheduling restart after")) shouldBe exponentialDelays.map(
              retryLogEntry
            )

            // Warnings should be triggered after `warnValidationDelay`
            // There can be multiple warnings if the system is slow
            val warnings = logEntries.filter(_.level == WARN).map(_.warningMessage)
            warnings should not be empty
            forEvery(warnings) {
              case warningRegex(value, unit) =>
                val failedValidationDuration = FiniteDuration(value.toLong, unit)
                failedValidationDuration shouldBe >(poolDelays.warnValidationDelay.toScala)
              case _ => fail("warning log entry not found")
            }
          },
        )

        loggerFactory.assertLogsSeq(
          SuppressionRule.LevelAndAbove(INFO) && SuppressionRule.LoggerNameContains(
            "ConnectionHandler"
          )
        )(
          {
            createdConnections(0).fail(reason = "test")
            listener.shouldStabilizeOn(ComponentHealthState.Ok())
          },
          logEntries => {
            // The retry delay is reset after the connection has been validated
            logEntries
              .map(_.message)
              .filter(_.contains("Scheduling restart after"))
              .loneElement shouldBe retryLogEntry(minRestartConnectionDelay)

            // There is no warning
            logEntries.filter(_.level == WARN) shouldBe empty
          },
        )

        // Ensure all responses were used, confirming that the proper retries took place.
        // The test would fail if it were to consume more responses, because they default to failures.
        testResponses.assertAllResponsesSent()
      }
    }

    "take into account an expected physical synchronizer ID" in {
      withConnectionPool(
        nbConnections = PositiveInt.tryCreate(10),
        trustThreshold = PositiveInt.tryCreate(2),
        {
          case index if index < 8 =>
            mkConnectionAttributes(synchronizerIndex = 2, sequencerIndex = index)
          case index => mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = index)

        },
        expectedSynchronizerIdO = Some(testSynchronizerId(1)),
      ) { (pool, _, _, _) =>
        loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(WARN))(
          {
            pool.start().futureValueUS.valueOrFail("initialization")
            pool.nbSequencers shouldBe NonNegativeInt.tryCreate(2)
          },
          logEntries => {
            // All 8 connections on the wrong synchronizer should be rejected either before or after
            // the threshold is reached.
            // There can be more than 1 warning for a given connection if it is poked more than once when validated.

            forAll(logEntries) { entry =>
              forExactly(
                1,
                Seq(
                  // Connections validated before the threshold is reached
                  badSynchronizerAssertion(goodSynchronizerId = 1, badSynchronizerId = 2),
                  // Connections validated after the threshold is reached
                  badBootstrapAssertion(goodSynchronizerId = 1, badSynchronizerId = 2),
                ),
              )(assertion => assertion(entry))
            }

            // The 8 connections must be represented
            val rx = raw"(?s).* internal-sequencer-connection-test-(\d+) .*".r
            logEntries.collect {
              _.warningMessage match { case rx(number) => number }
            }.distinct should have size 8
          },
        )

        pool.physicalSynchronizerIdO shouldBe Some(testSynchronizerId(1))

        // Changing the expected synchronizer is not supported
        inside(
          pool
            .updateConfig(pool.config.copy(expectedPSIdO = Some(testSynchronizerId(2))))
        ) { case Left(SequencerConnectionXPoolError.InvalidConfigurationError(error)) =>
          error shouldBe "The expected physical synchronizer ID can only be changed during a node restart."
        }
      }
    }

    "handle configuration changes: add and remove connections" in {
      withConnectionPool(
        nbConnections = PositiveInt.tryCreate(5),
        trustThreshold = PositiveInt.tryCreate(5),
        index => mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = index),
      ) { (pool, createdConnections, _, _) =>
        pool.start().futureValueUS.valueOrFail("initialization")

        pool.nbSequencers shouldBe NonNegativeInt.tryCreate(5)
        pool.physicalSynchronizerIdO shouldBe Some(testSynchronizerId(1))

        val initialConnections = createdConnections.snapshotAndClear()

        val newConnectionConfigs = (11 to 13).map(mkDummyConnectionConfig(_))
        pool
          .updateConfig(
            pool.config.copy(connections =
              NonEmpty.from(pool.config.connections.drop(2) ++ newConnectionConfigs).value
            )
          )
          .valueOrFail("update config")
        createdConnections should have size 3

        eventually() {
          contentsShouldEqual(
            pool.contents,
            Map(
              testSequencerId(2) -> Set(initialConnections(2)),
              testSequencerId(3) -> Set(initialConnections(3)),
              testSequencerId(4) -> Set(initialConnections(4)),
              testSequencerId(11) -> Set(createdConnections(11)),
              testSequencerId(12) -> Set(createdConnections(12)),
              testSequencerId(13) -> Set(createdConnections(13)),
            ),
          )
        }
      }
    }

    "handle configuration changes: change the endpoint of a connection" in {
      withConnectionPool(
        nbConnections = PositiveInt.tryCreate(3),
        trustThreshold = PositiveInt.tryCreate(3),
        index => mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = index),
      ) { (pool, createdConnections, _, _) =>
        pool.start().futureValueUS.valueOrFail("initialization")

        pool.nbSequencers shouldBe NonNegativeInt.tryCreate(3)
        pool.physicalSynchronizerIdO shouldBe Some(testSynchronizerId(1))

        val initialConnections = createdConnections.snapshotAndClear()

        val newConnectionConfig = mkDummyConnectionConfig(0, endpointIndexO = Some(4))
        pool
          .updateConfig(
            pool.config.copy(connections =
              NonEmpty
                .from(pool.config.connections.drop(1) :+ newConnectionConfig)
                .value
            )
          )
          .valueOrFail("update config")
        createdConnections should have size 1
        createdConnections(0) should not be initialConnections(0)
        createdConnections(0).config shouldBe newConnectionConfig

        eventually() {
          contentsShouldEqual(
            pool.contents,
            Map(
              testSequencerId(0) -> Set(createdConnections(0)),
              testSequencerId(1) -> Set(initialConnections(1)),
              testSequencerId(2) -> Set(initialConnections(2)),
            ),
          )
        }
      }
    }

    "handle configuration changes: change the trust threshold" in {
      withConnectionPool(
        nbConnections = PositiveInt.tryCreate(6),
        trustThreshold = PositiveInt.tryCreate(4),
        // 4 connections on a synchronizer
        // 2 connections on a different synchronizer
        attributesForConnection = {
          case index if index < 4 =>
            mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = index)
          case index => mkConnectionAttributes(synchronizerIndex = 2, sequencerIndex = index)
        },
        blockValidation = {
          case 3 | 5 => true
          case _ => false
        },
      ) { (pool, _, _, unblockValidation) =>
        val initializedF = pool.start()

        clue("Threshold remains reachable") {
          always(durationOfSuccess = 1.second) {
            initializedF.value.isCompleted shouldBe false
          }
        }

        clue("Increased threshold is rejected as unreachable") {
          inside(
            pool
              .updateConfig(pool.config.copy(trustThreshold = PositiveInt.tryCreate(6)))
          ) { case Left(SequencerConnectionXPoolError.InvalidConfigurationError(error)) =>
            error should include("Trust threshold 6 cannot be reached")
          }
        }

        clue("Reduced threshold is ambiguous") {
          inside(
            pool
              .updateConfig(pool.config.copy(trustThreshold = PositiveInt.tryCreate(1)))
          ) { case Left(SequencerConnectionXPoolError.InvalidConfigurationError(error)) =>
            error should include("Please configure a higher trust threshold")
          }
        }

        clue("Reachable threshold") {
          // A threshold of 3 can be reached with a single group, and the other connections generate warnings
          loggerFactory.assertLoggedWarningsAndErrorsSeq(
            {
              pool
                .updateConfig(pool.config.copy(trustThreshold = PositiveInt.tryCreate(3)))
                .valueOrFail("update config")
              pool.config.trustThreshold shouldBe PositiveInt.tryCreate(3)

              unblockValidation(3)
              unblockValidation(5)

              initializedF.futureValueUS.valueOrFail("initialization")

              eventually() {
                // Wait until the bad bootstraps have been logged
                loggerFactory.numberOfRecordedEntries shouldBe 2
              }
            },
            LogEntry.assertLogSeq(
              Seq(
                (
                  badBootstrapAssertion(goodSynchronizerId = 1, badSynchronizerId = 2),
                  "bad bootstrap",
                )
              )
            ),
          )
        }
      }
    }

    "handle configuration changes: removed connections are ignored when checking new threshold" in {
      withConnectionPool(
        nbConnections = PositiveInt.tryCreate(8),
        trustThreshold = PositiveInt.tryCreate(6),
        // 5 connections on synchronizer 1
        // 3 connections on synchronizer 2
        attributesForConnection = {
          case index if index < 5 =>
            mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = index)
          case index => mkConnectionAttributes(synchronizerIndex = 2, sequencerIndex = index)
        },
        blockValidation = {
          case 7 => true
          case _ => false
        },
      ) { (pool, _, _, unblockValidation) =>
        val initializedF = pool.start()

        // A threshold of 6 cannot be reached
        always() {
          pool.nbSequencers shouldBe NonNegativeInt.zero
        }

        // Remove 2 connections on sequencer 1, add 2 connections on sequencer 2, lower the threshold to 5
        // -> 3 connections on synchronizer 1
        //    5 connections on synchronizer 2
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            val newConnectionConfigs = (8 to 9).map(mkDummyConnectionConfig(_))
            pool.updateConfig(
              pool.config.copy(
                trustThreshold = PositiveInt.tryCreate(5),
                connections = NonEmpty
                  .from(pool.config.connections.drop(2) ++ newConnectionConfigs)
                  .value,
              )
            )

            unblockValidation(7)
            initializedF.futureValueUS.valueOrFail("initialization")

            // Threshold should be reached on synchronizer 2
            pool.physicalSynchronizerIdO.value shouldBe testSynchronizerId(2)
            pool.nbSequencers shouldBe NonNegativeInt.tryCreate(5)

            eventually() {
              // Wait until the bad bootstraps have been logged
              loggerFactory.numberOfRecordedEntries shouldBe 3
            }
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                badBootstrapAssertion(goodSynchronizerId = 2, badSynchronizerId = 1),
                "bad bootstrap",
              )
            )
          ),
        )
      }
    }

    "provide connections as requested" in {
      withConnectionPool(
        nbConnections = PositiveInt.tryCreate(6),
        trustThreshold = PositiveInt.tryCreate(3),
        {
          case 0 | 1 | 2 => mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = 1)
          case 3 | 4 => mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = 2)
          case 5 => mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = 3)
        },
      ) { (pool, createdConnections, _, _) =>
        pool.start().futureValueUS.valueOrFail("initialization")

        pool.nbSequencers shouldBe NonNegativeInt.tryCreate(3)
        pool.physicalSynchronizerIdO shouldBe Some(testSynchronizerId(1))
        eventually() {
          pool.nbConnections shouldBe NonNegativeInt.tryCreate(6)
        }

        val createdConfigs = (0 to 5).map(createdConnections.apply).map(_.config)

        clue("one connection per sequencer") {
          val received = pool.getConnections("test", PositiveInt.three, exclusions = Set.empty)

          received.map(_.attributes.sequencerId) shouldBe Set(
            testSequencerId(1),
            testSequencerId(2),
            testSequencerId(3),
          )

          val receivedConfigs = received.map(_.config)

          receivedConfigs.intersect(createdConfigs.slice(0, 3).toSet) should have size 1
          receivedConfigs.intersect(createdConfigs.slice(3, 5).toSet) should have size 1
          receivedConfigs.intersect(createdConfigs.slice(5, 6).toSet) should have size 1
        }

        clue("round robin") {
          val exclusions = Set(testSequencerId(2), testSequencerId(3))
          val received1 = pool.getConnections("test", PositiveInt.one, exclusions)
          val received2 = pool.getConnections("test", PositiveInt.one, exclusions)
          val received3 = pool.getConnections("test", PositiveInt.one, exclusions)

          Set(received1, received2, received3).map(_.loneElement.config) shouldBe
            Set(createdConfigs(0), createdConfigs(1), createdConfigs(2))

          pool.getConnections("test", PositiveInt.one, exclusions) shouldBe received1
        }

        clue("request too many") {
          val received =
            pool.getConnections("test", PositiveInt.tryCreate(5), exclusions = Set.empty)
          received should have size 3
        }

        clue("stop and start") {
          val exclusions = Set(testSequencerId(1), testSequencerId(3))

          pool.getConnections("test", PositiveInt.three, exclusions) should have size 1

          val connectionsOnSeq2 = Set(createdConnections(3), createdConnections(4))
          connectionsOnSeq2.foreach(_.fail(reason = "test"))

          eventually() {
            // Both connections have been restarted and can be obtained
            val received =
              connectionsOnSeq2.map(_ => pool.getConnections("test", PositiveInt.three, exclusions))
            forAll(received)(_ should have size 1)
            received.flatten.map(_.config) shouldBe connectionsOnSeq2.map(_.config)
          }

          connectionsOnSeq2.foreach(_.fatal(reason = "test"))
          eventuallyForever() {
            // Connections don't get restarted
            pool.getConnections("test", PositiveInt.three, exclusions) should have size 0
          }
        }
      }
    }

    "initialize when there is a consensus on bootstrap info" in {
      withConnectionPool(
        nbConnections = PositiveInt.tryCreate(3),
        trustThreshold = PositiveInt.tryCreate(2),
        {
          case 0 => mkConnectionAttributes(synchronizerIndex = 1, sequencerIndex = 1)
          case 1 => mkConnectionAttributes(synchronizerIndex = 2, sequencerIndex = 1)
          case 2 => mkConnectionAttributes(synchronizerIndex = 2, sequencerIndex = 2)
        },
      ) { (pool, _, _, _) =>
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            pool.start().futureValueUS.valueOrFail("initialization")
            pool.nbSequencers shouldBe NonNegativeInt.tryCreate(2)
            pool.physicalSynchronizerIdO shouldBe Some(testSynchronizerId(2))

            eventually() {
              // Wait until the bad bootstrap has been logged
              loggerFactory.numberOfRecordedEntries shouldBe 1
            }
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                badBootstrapAssertion(goodSynchronizerId = 2, badSynchronizerId = 1),
                "bad bootstrap",
              )
            )
          ),
        )
      }
    }
  }

  private def badBootstrapAssertion(
      goodSynchronizerId: Int,
      badSynchronizerId: Int,
  ): LogEntry => Assertion =
    (logEntry: LogEntry) =>
      logEntry.warningMessage should fullyMatch regex
        raw"(?s)Connection internal-sequencer-connection-test-\d+ has invalid bootstrap info:" +
        raw" expected BootstrapInfo\(test-synchronizer-$goodSynchronizerId::namespace.*," +
        raw" got BootstrapInfo\(test-synchronizer-$badSynchronizerId::namespace.*"

  private def badSynchronizerAssertion(
      goodSynchronizerId: Int,
      badSynchronizerId: Int,
  ): LogEntry => Assertion =
    (logEntry: LogEntry) =>
      logEntry.warningMessage should fullyMatch regex
        raw"(?s)Connection internal-sequencer-connection-test-\d+ is not on expected synchronizer:" +
        raw" expected Some\(test-synchronizer-$goodSynchronizerId::namespace::$testedProtocolVersion-0\)," +
        raw" got test-synchronizer-$badSynchronizerId::namespace.*"

  private def contentsShouldEqual(
      contents: Map[SequencerId, Set[SequencerConnectionX]],
      created: Map[SequencerId, Set[InternalSequencerConnectionX]],
  ): Assertion = {
    // Compare based on connection configs
    val first = contents.view.mapValues(_.map(_.config)).toMap
    val second = created.view.mapValues(_.map(_.config)).toMap
    first shouldBe second
  }
}
