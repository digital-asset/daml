// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import cats.Show.Shown
import com.daml.metrics.MetricsFilterConfig
import com.daml.metrics.api.MetricQualification
import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands.Inspection.SlowCounterParticipantSynchronizerConfig
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong}
import com.digitalasset.canton.console.{
  LocalParticipantReference,
  LocalSequencerReference,
  ParticipantReference,
}
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.metrics.{MetricsConfig, MetricsReporterConfig}
import com.digitalasset.canton.participant.pruning.SortedReconciliationIntervalsHelpers
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.{SynchronizerAlias, UniquePortGenerator, config}
import monocle.Monocle.toAppliedFocusOps

import java.time.Duration as JDuration

trait AcsCommitmentMetricsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SortedReconciliationIntervalsHelpers {

  // we have several tests the require stopping and starting participants, since in memory does not persist the state these are not suited
  val isInMemory = true

  private val aliceName = "Alice"
  private val bobName = "Bob"
  private val charlieName = "Charlie"

  private var alice: PartyId = _
  private var bob: PartyId = _
  private var charlie: PartyId = _

  private var metricsSynchronizerAlias: Shown = _

  private val metricsPrefix = s"daml.participant.sync.commitments"
  private val interval = JDuration.ofSeconds(5)
  private lazy val maxDedupDuration = java.time.Duration.ofSeconds(1)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateMaxDeduplicationDurations(maxDedupDuration),
      )
      .addConfigTransform(
        _.focus(_.monitoring.metrics)
          .replace(
            MetricsConfig(
              qualifiers = Seq[MetricQualification](
                MetricQualification.Debug
              ),
              reporters = Seq(
                MetricsReporterConfig.Prometheus(
                  port = UniquePortGenerator.next,
                  filters = Seq(MetricsFilterConfig(metricsPrefix)),
                )
              ),
            )
          )
      )
      .updateTestingConfig(
        _.focus(_.maxCommitmentSendDelayMillis).replace(Some(NonNegativeInt.zero))
      )
      .withSetup { implicit env =>
        import env.*

        val synchronizers =
          Seq(daId -> synchronizerOwners1, acmeId -> synchronizerOwners2)

        synchronizers foreach { case (synchronizerId, owner) =>
          owner.foreach(
            _.topology.synchronizer_parameters
              .propose_update(
                synchronizerId,
                _.update(
                  reconciliationInterval = config.PositiveDurationSeconds(interval),
                  acsCommitmentsCatchUpParameters = None,
                ),
              )
          )
        }

        // Allocate parties
        alice = participant1.parties.enable(aliceName)
        bob = participant2.parties.enable(bobName)
        charlie = participant3.parties.enable(charlieName)

        metricsSynchronizerAlias = daName.unquoted

        participants.all.foreach { participant =>
          connect(
            participant = participant,
            localSequencerReference = sequencer1,
            synchronizerAlias = daName,
          )
          connect(
            participant = participant,
            localSequencerReference = sequencer2,
            synchronizerAlias = acmeName,
          )
        }
      }

  private def connect(
      participant: ParticipantReference,
      localSequencerReference: LocalSequencerReference,
      synchronizerAlias: SynchronizerAlias,
  ): Unit = {
    // Connect and disconnect so that we can modify the synchronizer connection config afterwards
    participant.synchronizers.connect_local(localSequencerReference, alias = synchronizerAlias)
    participant.dars.upload(CantonExamplesPath)
  }

  private def deployAndCheckContractOnParticipants(
      iou: Iou.Contract,
      deployParticipants: Seq[LocalParticipantReference],
  ): Unit = {

    logger.info(
      s"Waiting for participants ($deployParticipants) to see the contract in their ACS (${iou.id.contractId})"
    )
    eventually() {
      deployParticipants.foreach { p =>
        p.ledger_api.state.acs
          .of_all()
          .filter(_.contractId == iou.id.contractId) should not be empty
      }
    }
  }

  private def incrementIntervals(intervalCount: Int)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*
    val allCounterParticipantsCount = participants.all.size - 1
    val activeCounterParticipantsCount = participants.all.count(_.health.is_running()) - 1
    val simClock = environment.simClock.value
    val start = simClock.now.plus(JDuration.ofMillis(1))
    logger.info(
      s"we increase time with a ping $intervalCount times, to force $intervalCount  reconciliation interval and commitment generations"
    )

    for (_ <- 1 to intervalCount) {
      simClock.advance(interval)
      participant1.health.ping(participant1)
      participants.all.filter(_.health.is_running()).foreach(_.testing.fetch_synchronizer_times())
    }
    val end = simClock.now
    logger.info(
      "we check that participant1 has generated and received the correct amount of commitments"
    )
    eventually() {
      withClue(s"should have computed $intervalCount * $allCounterParticipantsCount commitments") {
        participant1.commitments
          .computed(daName, start.toInstant, end.toInstant)
          .size shouldBe (intervalCount * allCounterParticipantsCount)
      }
      withClue(
        s"should have received $intervalCount * $activeCounterParticipantsCount commitments"
      ) {
        participant1.commitments
          .received(daName, start.toInstant, end.toInstant)
          .size shouldBe (intervalCount * activeCounterParticipantsCount)
      }
    }
  }

  "Can get, set and reset metrics configuration" in { implicit env =>
    import env.*

    val slowConfig = new SlowCounterParticipantSynchronizerConfig(
      Seq(daId),
      Seq(participant2.id),
      10,
      10,
      Seq.empty,
    )
    logger.info("validate that the default config is empty")
    val initialConfig = participant1.commitments.get_config_for_slow_counter_participants(Seq(daId))
    initialConfig shouldBe Seq.empty

    logger.info("we set the config and validate that the new config matches")
    participant1.commitments.set_config_for_slow_counter_participants(Seq(slowConfig))
    val afterUpdate = participant1.commitments.get_config_for_slow_counter_participants(Seq(daId))
    afterUpdate shouldBe Seq(slowConfig)

    logger.info("we extend the config and validate it has an updated value")
    participant1.commitments.add_participant_to_individual_metrics(Seq(participant3.id), Seq(daId))
    val afterAddition = participant1.commitments.get_config_for_slow_counter_participants(Seq(daId))
    afterAddition.foreach(_.participantsMetrics shouldBe Seq(participant3.id))

    logger.info("we remove the extension and validate it is as before")
    participant1.commitments.remove_participant_from_individual_metrics(
      Seq(participant3.id),
      Seq(daId),
    )
    val afterRemoval = participant1.commitments.get_config_for_slow_counter_participants(Seq(daId))
    afterRemoval shouldBe Seq(slowConfig)

    logger.info("we remove the config and validate that it is empty")
    participant1.commitments.remove_config_for_slow_counter_participants(
      Seq(participant2.id),
      Seq(daId),
    )
    val afterClear = participant1.commitments.get_config_for_slow_counter_participants(Seq(daId))
    afterClear shouldBe Seq.empty
  }

  "updating configurations should overwrite existing" in { implicit env =>
    import env.*

    val initialConfig = new SlowCounterParticipantSynchronizerConfig(
      Seq(daId),
      Seq(participant2.id),
      10,
      10,
      Seq(participant2.id),
    )

    val update1Config = new SlowCounterParticipantSynchronizerConfig(
      Seq(daId, acmeId),
      Seq(participant3.id),
      15,
      15,
      Seq.empty,
    )

    val update2Config = new SlowCounterParticipantSynchronizerConfig(
      Seq(daId, acmeId),
      Seq.empty,
      20,
      20,
      Seq(participant3.id),
    )

    participant1.commitments.set_config_for_slow_counter_participants(Seq(initialConfig))
    participant1.commitments.set_config_for_slow_counter_participants(Seq(update1Config))
    participant1.commitments.set_config_for_slow_counter_participants(Seq(update2Config))

    val resultConfigs = participant1.commitments.get_config_for_slow_counter_participants(Seq.empty)

    val finalSynchronizer1Config = new SlowCounterParticipantSynchronizerConfig(
      Seq(daId),
      Seq.empty,
      20,
      20,
      Seq(participant3.id),
    )

    val finalSynchronizer2Config = new SlowCounterParticipantSynchronizerConfig(
      Seq(acmeId),
      Seq.empty,
      20,
      20,
      Seq(participant3.id),
    )

    resultConfigs should contain theSameElementsAs Seq(
      finalSynchronizer1Config,
      finalSynchronizer2Config,
    )

    participant1.commitments.remove_config_for_slow_counter_participants(
      Seq.empty,
      Seq(daId, acmeId),
    )

    val afterClear =
      participant1.commitments.get_config_for_slow_counter_participants(Seq(daId, acmeId))
    afterClear shouldBe Seq.empty
  }

  "Should proper group participants and synchronizers" in { implicit env =>
    import env.*

    val participant4Id = ParticipantId.apply("participant4")
    val participant5Id = ParticipantId.apply("participant5")

    val slowConfig = new SlowCounterParticipantSynchronizerConfig(
      Seq(daId, acmeId),
      Seq(participant3.id),
      10,
      10,
      Seq.empty,
    )
    logger.info(
      "we set the config on two synchronizers for participant3 and validate he is contained in all filtered searched"
    )
    participant1.commitments.set_config_for_slow_counter_participants(Seq(slowConfig))
    val initialSynchronizer2Config =
      participant1.commitments.get_config_for_slow_counter_participants(Seq(acmeId))
    val initialSynchronizerConfig =
      participant1.commitments.get_config_for_slow_counter_participants(Seq(daId))
    val initialBothConfig =
      participant1.commitments.get_config_for_slow_counter_participants(Seq(acmeId, daId))
    initialSynchronizer2Config.foreach(_.distinguishedParticipants shouldBe Seq(participant3.id))
    initialSynchronizerConfig.foreach(_.distinguishedParticipants shouldBe Seq(participant3.id))
    initialBothConfig.foreach(_.distinguishedParticipants shouldBe Seq(participant3.id))

    logger.info("we add participant4 on synchronizer2")
    participant1.commitments.add_config_distinguished_slow_counter_participants(
      Seq(participant4Id),
      Seq(acmeId),
    )
    val afterAdditionSynchronizer2Config =
      participant1.commitments.get_config_for_slow_counter_participants(Seq(acmeId))
    val afterAdditionSynchronizerConfig =
      participant1.commitments.get_config_for_slow_counter_participants(Seq(daId))
    val afterAdditionBothConfig =
      participant1.commitments.get_config_for_slow_counter_participants(Seq(acmeId, daId))

    logger.info("asking for synchronizer2 should return both participant3 and participant4")
    afterAdditionSynchronizer2Config.foreach(
      _.distinguishedParticipants shouldBe Seq(participant3.id, participant4Id)
    )

    logger.info("asking for synchronizer1 should only return participant3")
    afterAdditionSynchronizerConfig.foreach(
      _.distinguishedParticipants shouldBe Seq(participant3.id)
    )

    logger.info(
      "asking for both synchronizers should return one for synchronizer2 with both participants and one for synchronizer1 with participant3"
    )
    afterAdditionBothConfig.foreach {
      case config if config.synchronizerIds.contains(acmeId) =>
        config.distinguishedParticipants shouldBe Seq(participant3.id, participant4Id)
      case config if config.synchronizerIds.contains(daId) =>
        config.distinguishedParticipants shouldBe Seq(participant3.id)
      case _ => fail()
    }

    logger.info("we add participant5 to synchronizer1")
    participant1.commitments.add_config_distinguished_slow_counter_participants(
      Seq(participant5Id),
      Seq(daId),
    )
    val afterSecondAdditionSynchronizer2Config =
      participant1.commitments.get_config_for_slow_counter_participants(Seq(acmeId))
    val afterSecondAdditionSynchronizerConfig =
      participant1.commitments.get_config_for_slow_counter_participants(Seq(daId))
    val afterSecondAdditionBothConfig =
      participant1.commitments.get_config_for_slow_counter_participants(Seq(acmeId, daId))

    logger.info("synchronizer2 should contain participant3 & participant4")
    afterSecondAdditionSynchronizer2Config.foreach(
      _.distinguishedParticipants shouldBe Seq(participant3.id, participant4Id)
    )

    logger.info("synchronizer should contain participant3 & participant5")
    afterSecondAdditionSynchronizerConfig.foreach(
      _.distinguishedParticipants shouldBe Seq(participant3.id, participant5Id)
    )

    logger.info(
      "asking for both synchronizers should return a set containing (participant3,participant4) & (participant3,participant5)"
    )
    afterSecondAdditionBothConfig.foreach {
      case config if config.synchronizerIds.contains(acmeId) =>
        config.distinguishedParticipants shouldBe Seq(participant3.id, participant4Id)
      case config if config.synchronizerIds.contains(daId) =>
        config.distinguishedParticipants shouldBe Seq(participant3.id, participant5Id)
      case _ => fail()
    }

    logger.info("we validate that calling remove config with empty values removes everything")
    val preClean = participant1.commitments.get_config_for_slow_counter_participants(Seq.empty)
    participant1.commitments.remove_config_for_slow_counter_participants(Seq.empty, Seq.empty)
    val postClean = participant1.commitments.get_config_for_slow_counter_participants(Seq.empty)
    preClean should not be postClean
    preClean should contain(
      new SlowCounterParticipantSynchronizerConfig(
        Seq(daId),
        Seq(participant3.id, participant5Id),
        10,
        10,
        Seq.empty,
      )
    )
    preClean should contain(
      new SlowCounterParticipantSynchronizerConfig(
        Seq(acmeId),
        Seq(participant3.id, participant4Id),
        10,
        10,
        Seq.empty,
      )
    )
    postClean shouldBe Seq.empty
  }

  "Cant add slow counter participant to non-existing config" in { implicit env =>
    import env.*
    logger.info("since config is empty, then extending it should not do anything")
    val preConfig = participant1.commitments.get_config_for_slow_counter_participants(Seq.empty)
    participant1.commitments.add_config_distinguished_slow_counter_participants(
      Seq(participant2.id),
      Seq(daId),
    )
    preConfig shouldBe Seq.empty
    val postConfig = participant1.commitments.get_config_for_slow_counter_participants(Seq.empty)
    preConfig shouldBe postConfig
  }

  "Cant add slow counter participant to non-existing config with existing different synchronizer" in {
    implicit env =>
      import env.*
      val slowConfig = new SlowCounterParticipantSynchronizerConfig(
        Seq(daId),
        Seq(participant3.id),
        10,
        10,
        Seq.empty,
      )
      participant1.commitments.set_config_for_slow_counter_participants(Seq(slowConfig))

      logger.info("since config for acme is empty, then extending it should not do anything")
      val preConfig = participant1.commitments.get_config_for_slow_counter_participants(Seq.empty)
      participant1.commitments.add_config_distinguished_slow_counter_participants(
        Seq(participant2.id),
        Seq(acmeId),
      )
      preConfig shouldBe Seq(slowConfig)
      val postConfig = participant1.commitments.get_config_for_slow_counter_participants(Seq.empty)
      preConfig shouldBe postConfig

      participant1.commitments.remove_config_for_slow_counter_participants(Seq.empty, Seq.empty)
  }

  "Can get max long value when we have never received a counter commitment" in { implicit env =>
    import env.*
    val iou = IouSyntax
      .createIou(participant1, Some(daId))(alice, bob)
    val iou2 = IouSyntax
      .createIou(participant1, Some(daId))(alice, charlie)
    logger.info(s"deploying two IOU contract")
    deployAndCheckContractOnParticipants(iou, Seq(participant1, participant2))
    deployAndCheckContractOnParticipants(iou2, Seq(participant1, participant3))
    val simClock = environment.simClock.value

    logger.info(
      "we advance time only for participant1, so participant2 & participant3 does not generate commitments"
    )
    participant2.stop()
    participant3.stop()
    val start = simClock.now

    simClock.advance(interval.multipliedBy(2L))
    participant1.health.ping(participant1)
    participant1.testing.fetch_synchronizer_times()
    val end = simClock.now

    eventually() {
      participant1.commitments.computed(daName, start.toInstant, end.toInstant).size shouldBe 4
    }

    logger.info(
      "participant1 should see the other two participants being behind with longMaxValue (because they have never send a commitment)"
    )

    eventually() {
      val allIntervalsBehind =
        participant1.commitments.get_intervals_behind_for_counter_participants(
          Seq.empty,
          Seq(daId),
          None,
        )
      allIntervalsBehind.size shouldBe 2
      allIntervalsBehind.foreach(_.intervalsBehind shouldBe NonNegativeLong.maxValue)
    }

    participant2.start()
    participant3.start()

    participants.all.foreach { participant =>
      connect(
        participant = participant,
        localSequencerReference = sequencer1,
        synchronizerAlias = daName,
      )
      connect(
        participant = participant,
        localSequencerReference = sequencer2,
        synchronizerAlias = acmeName,
      )
    }
  }

  "Can get intervals behind for counter participant" onlyRunWhen (!isInMemory) in { implicit env =>
    import env.*

    logger.info("we increment one interval so all participants have send at least one commitment")
    incrementIntervals(1)
    eventually() {
      participant1.commitments
        .get_intervals_behind_for_counter_participants(
          Seq(participant2, participant3),
          Seq(daId),
          None,
        )
        .foreach(_.intervalsBehind shouldBe NonNegativeLong.zero)
    }
    logger.info("we stop participant2 so it will fall behind")
    participant2.stop()
    eventually() {
      participant2.health.is_running() shouldBe false
    }
    val incremental = 2L
    incrementIntervals(incremental.toInt)
    logger.info("we validate that we see participant2 being behind")
    eventually() {
      val allIntervalsBehind =
        participant1.commitments.get_intervals_behind_for_counter_participants(
          Seq.empty,
          Seq(daId),
          None,
        )
      allIntervalsBehind.nonEmpty shouldBe true
      allIntervalsBehind.foreach(
        _.intervalsBehind shouldBe NonNegativeLong.tryCreate(incremental - 1)
      )
    }

    logger.info("we validate we can check status of participant3 even if it is not behind")
    val participant3Behind = participant1.commitments.get_intervals_behind_for_counter_participants(
      Seq(participant3.id),
      Seq(daId),
      None,
    )
    withClue("participant 3 should be behind") {
      participant3Behind.nonEmpty shouldBe true
      participant3Behind.foreach(_.intervalsBehind shouldBe NonNegativeLong.zero)
    }
  }

  "can get metrics for participants" onlyRunWhen (!isInMemory) in { implicit env =>
    import env.*
    logger.info("we restart participant2 to build the configs and then stop it again")

    participant2.start()
    val slowConfig = new SlowCounterParticipantSynchronizerConfig(
      Seq(daId),
      Seq(participant2.id),
      1,
      1,
      Seq.empty,
    )
    val secondSlowConfig = new SlowCounterParticipantSynchronizerConfig(
      Seq(daId),
      Seq.empty,
      1,
      1,
      Seq(participant2.id, participant3.id),
    )
    participant2.stop()
    participant1.commitments.set_config_for_slow_counter_participants(Seq(slowConfig))
    incrementIntervals(2)

    logger.info("we validate participant3 is keeping the default up to date")
    participant1.metrics
      .get_long_point(
        s"$metricsPrefix.$metricsSynchronizerAlias.largest-counter-participant-latency"
      )
      .value shouldBe 0

    logger.info("we validate the distinguished participant2 is falling behind")
    participant1.metrics
      .get_long_point(
        s"$metricsPrefix.$metricsSynchronizerAlias.largest-distinguished-counter-participant-latency"
      )
      .value should be > 0L

    participant1.commitments.set_config_for_slow_counter_participants(Seq(secondSlowConfig))
    incrementIntervals(2)

    logger.info(
      "we validate that no default participants and no distinguished participant is behind"
    )

    eventually() {
      participant1.metrics
        .get_long_point(
          s"$metricsPrefix.$metricsSynchronizerAlias.largest-counter-participant-latency"
        )
        .value shouldBe 0
      participant1.metrics
        .get_long_point(
          s"$metricsPrefix.$metricsSynchronizerAlias.largest-distinguished-counter-participant-latency"
        )
        .value shouldBe 0
    }

    logger.info(
      "we validate we can see the uniquely monitored participant2 (still behind) and participant3"
    )

    eventually() {
      participant1.metrics
        .get_long_point(
          s"$metricsPrefix.$metricsSynchronizerAlias.counter-participant-latency.participant2"
        )
        .value should be > 0L
      participant1.metrics
        .get_long_point(
          s"$metricsPrefix.$metricsSynchronizerAlias.counter-participant-latency.participant3"
        )
        .value shouldBe 0
    }
  }

  "no wait participants does not affect default latency metric" onlyRunWhen (!isInMemory) in {
    implicit env =>
      import env.*

      val slowConfig = new SlowCounterParticipantSynchronizerConfig(
        Seq(daId),
        Seq.empty,
        1,
        1,
        Seq.empty,
      )
      participant1.commitments.set_config_for_slow_counter_participants(Seq(slowConfig))
      logger.info("ensuring participant2 is up to date after previous test")
      participant2.start()
      connect(
        participant = participant2,
        localSequencerReference = sequencer1,
        synchronizerAlias = daName,
      )
      connect(
        participant = participant2,
        localSequencerReference = sequencer2,
        synchronizerAlias = acmeName,
      )

      participant2.testing.fetch_synchronizer_times()
      eventually() {
        participant1.commitments
          .get_intervals_behind_for_counter_participants(Seq(participant2), Seq(daId), None)
          .foreach(_.intervalsBehind shouldBe NonNegativeLong.zero)
      }

      val participant2Id = participant2.id
      participant2.stop()
      incrementIntervals(2)

      eventually() {
        participant1.metrics
          .get_long_point(
            s"$metricsPrefix.$metricsSynchronizerAlias.largest-counter-participant-latency"
          )
          .value should be > 0L
      }

      logger.info("setting no wait for participant2")
      participant1.commitments.set_no_wait_commitments_from(Seq(participant2Id), Seq(daId))
      incrementIntervals(2)

      eventually() {
        participant1.metrics
          .get_long_point(
            s"$metricsPrefix.$metricsSynchronizerAlias.largest-counter-participant-latency"
          )
          .value shouldBe 0
      }

      participant2.start()
      participant1.commitments.set_wait_commitments_from(Seq(participant2.id), Seq(daId))

  }

  "if slowest participant catch up, we should see next participant behind" onlyRunWhen (!isInMemory) in {
    implicit env =>
      import env.*

      participant1.commitments.get_config_for_slow_counter_participants(
        Seq.empty
      ) shouldBe Seq.empty

      participant2.stop()
      incrementIntervals(2)
      participant3.stop()
      incrementIntervals(2)
      val bothBehind = participant1.metrics
        .get_long_point(
          s"$metricsPrefix.$metricsSynchronizerAlias.largest-counter-participant-latency"
        )

      participant2.start()
      connect(
        participant = participant2,
        localSequencerReference = sequencer1,
        synchronizerAlias = daName,
      )
      connect(
        participant = participant2,
        localSequencerReference = sequencer2,
        synchronizerAlias = acmeName,
      )

      bothBehind.value should be > 0L
      incrementIntervals(1)

      logger.info(
        "the latency should now correlate to participant3 that was less behind than participant2"
      )
      eventually() {
        val afterParticipant2Catchup = participant1.metrics
          .get_long_point(
            s"$metricsPrefix.$metricsSynchronizerAlias.largest-counter-participant-latency"
          )
        afterParticipant2Catchup.value should be > 0L
        bothBehind.value should be > afterParticipant2Catchup.value
      }

  }
}

class AcsCommitmentMetricsIntegrationTestDefault extends AcsCommitmentMetricsIntegrationTest {
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2"))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )
}

class AcsCommitmentMetricsIntegrationTestPostgres extends AcsCommitmentMetricsIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2"))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )
  override val isInMemory = false
}
