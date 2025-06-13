// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.dynamicsynchronizerparameters

import com.daml.test.evidence.scalatest.AccessTestScenario
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.SecureConfiguration
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.admin.api.client.data.{
  DynamicSynchronizerParameters as ConsoleDynamicSynchronizerParameters,
  OnboardingRestriction,
  ParticipantSynchronizerLimits,
  TrafficControlParameters,
}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong}
import com.digitalasset.canton.config.{DbConfig, StorageConfig}
import com.digitalasset.canton.console.ConsoleEnvironment.Implicits.*
import com.digitalasset.canton.console.{CommandFailure, SequencerReference}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.sequencing.protocol.SequencerErrors
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.TopologyManagerError.InvalidSynchronizer
import com.digitalasset.canton.topology.{
  ForceFlag,
  ForceFlags,
  SynchronizerId,
  TopologyManagerError,
}
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion
import org.slf4j.event.Level

import scala.concurrent.duration.*

trait SynchronizerParametersFixture {
  protected def increaseConfirmationResponseTimeout(
      old: ConsoleDynamicSynchronizerParameters
  ): ConsoleDynamicSynchronizerParameters =
    old.update(confirmationResponseTimeout =
      old.confirmationResponseTimeout + config.NonNegativeFiniteDuration.ofSeconds(1L)
    )

  import SynchronizerParametersFixture.*

  protected def getCurrentSynchronizerParameters(
      synchronizer: Synchronizer
  ): ConsoleDynamicSynchronizerParameters =
    synchronizer.sequencer.topology.synchronizer_parameters
      .get_dynamic_synchronizer_parameters(synchronizer.synchronizerId)

  protected def setDynamicSynchronizerParameters(
      synchronizer: Synchronizer,
      parameters: ConsoleDynamicSynchronizerParameters,
  ): Unit =
    synchronizer.sequencer.topology.synchronizer_parameters
      .propose(
        synchronizer.synchronizerId,
        parameters,
        mustFullyAuthorize = true,
      )
      .discard

  protected def updateDynamicSynchronizerParameters(
      synchronizer: Synchronizer,
      update: ConsoleDynamicSynchronizerParameters => ConsoleDynamicSynchronizerParameters,
  ): Unit = synchronizer.sequencer.topology.synchronizer_parameters
    .propose_update(
      synchronizer.synchronizerId,
      update,
      mustFullyAuthorize = true,
    )

  protected def daSynchronizer(implicit env: TestConsoleEnvironment) =
    SynchronizerParametersFixture.Synchronizer(env.sequencer1)
}

object SynchronizerParametersFixture {
  private[dynamicsynchronizerparameters] final case class Synchronizer(
      sequencer: SequencerReference
  ) {
    val synchronizerId: SynchronizerId = sequencer.synchronizer_id
  }
}

trait SynchronizerParametersChangeIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SynchronizerParametersFixture
    with SecurityTestSuite
    with AccessTestScenario {

  private lazy val dynamicReconciliationInterval = config.PositiveDurationSeconds.ofSeconds(2)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1.addConfigTransforms(
      ConfigTransforms.useStaticTime,
      // Disable retries in the ping service so that any submission error is reported reliably
      // This makes the log messages more deterministic.
      ConfigTransforms.updateAllParticipantConfigs_(
        _.focus(_.parameters.adminWorkflow.retries).replace(false)
      ),
    ) withSetup { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, alias = daName)
    }

  protected lazy val sequencersForPlugin: MultiSynchronizer =
    MultiSynchronizer(Seq(Set("sequencer1"), Set("sequencer2")).map(_.map(InstanceName.tryCreate)))

  private lazy val defaultParameters = ConsoleDynamicSynchronizerParameters.initialValues(
    new SimClock(loggerFactory = loggerFactory),
    testedProtocolVersion,
  )

  private def acmeSynchronizer(implicit env: TestConsoleEnvironment) =
    SynchronizerParametersFixture.Synchronizer(env.sequencer2)

  "A synchronizer operator" can {
    "list and set synchronizer parameters" in { implicit env =>
      val synchronizerParameters1 = increaseConfirmationResponseTimeout(defaultParameters)
      val synchronizerParameters2 = increaseConfirmationResponseTimeout(synchronizerParameters1)
      val synchronizerParameters3 = increaseConfirmationResponseTimeout(synchronizerParameters2)

      getCurrentSynchronizerParameters(daSynchronizer) shouldBe defaultParameters
      getCurrentSynchronizerParameters(acmeSynchronizer) shouldBe defaultParameters

      setDynamicSynchronizerParameters(daSynchronizer, synchronizerParameters1)
      getCurrentSynchronizerParameters(daSynchronizer) shouldBe synchronizerParameters1
      getCurrentSynchronizerParameters(acmeSynchronizer) shouldBe defaultParameters

      setDynamicSynchronizerParameters(daSynchronizer, synchronizerParameters2)
      setDynamicSynchronizerParameters(acmeSynchronizer, synchronizerParameters3)
      getCurrentSynchronizerParameters(daSynchronizer) shouldBe synchronizerParameters2
      getCurrentSynchronizerParameters(acmeSynchronizer) shouldBe synchronizerParameters3
    }

    "update synchronizer parameters" in { implicit env =>
      val currentSynchronizerParameters = getCurrentSynchronizerParameters(daSynchronizer)
      val modifier = increaseConfirmationResponseTimeout _
      val expectedNewSynchronizerParameters = modifier(currentSynchronizerParameters)

      updateDynamicSynchronizerParameters(daSynchronizer, modifier)

      getCurrentSynchronizerParameters(daSynchronizer) shouldBe expectedNewSynchronizerParameters
    }

    "update reconciliation interval" in { implicit env =>
      val newDynamicReconciliationInterval =
        dynamicReconciliationInterval.plusSeconds(1)

      val reconciliationTest = TestDynamicParams[config.PositiveDurationSeconds](
        daSynchronizer,
        setValue = (p, ri) => p.update(reconciliationInterval = ri),
        getValue = _.reconciliationInterval,
        initialValue = dynamicReconciliationInterval,
        newValue = newDynamicReconciliationInterval,
        name = "reconciliation interval",
      )
      TestDynamicParams.runTest(reconciliationTest)
    }

    "update confirmationRequestsMaxRate" in { implicit env =>
      import env.*

      val initialValue = NonNegativeInt.tryCreate(10)

      val confirmationRequestsMaxRateTest = TestDynamicParams[NonNegativeInt](
        daSynchronizer,
        setValue = (p, mrpp) => p.update(confirmationRequestsMaxRate = mrpp),
        getValue = _.confirmationRequestsMaxRate,
        initialValue = initialValue,
        newValue = initialValue * 10,
        name = "confirmation requests max rate",
      )
      TestDynamicParams.runTest(confirmationRequestsMaxRateTest)
    }

    "update maxRequestSize" in { implicit env =>
      import env.*

      val initialValue = NonNegativeInt.tryCreate(60000)

      val maxRequestSizeTest = TestDynamicParams[config.RequireTypes.NonNegativeInt](
        daSynchronizer,
        setValue = (p, mrs) => p.update(maxRequestSize = mrs),
        getValue = _.maxRequestSize,
        initialValue = initialValue,
        newValue = initialValue * 10,
        name = "max request size",
      )
      TestDynamicParams.runTest(maxRequestSizeTest)
    }

    // This is just for the snippets
    "perform simple synchronizer parameters operations" in { implicit env =>
      import env.*

      val synchronizerId = acmeSynchronizer.synchronizerId
      val mySequencer = acmeSynchronizer.sequencer
      val myParticipant = participant1
      participant1.synchronizers.connect_local(mySequencer, alias = acmeName)

      /*
        This does not test much, the only goal is to have the snippets included in the
        documentation. We don't use proper `snippets` because we would need to introduce
        a new environment which makes the documentation generation slower.
       */

      // user-manual-entry-begin:-begin: GetDynamicSynchronizerParameters
      myParticipant.topology.synchronizer_parameters.get_dynamic_synchronizer_parameters(
        synchronizerId
      )
      // user-manual-entry-begin:-end: GetDynamicSynchronizerParameters

      // user-manual-entry-begin:-begin: GetSingleDynamicSynchronizerParameter
      myParticipant.topology.synchronizer_parameters
        .get_dynamic_synchronizer_parameters(synchronizerId)
        .confirmationResponseTimeout
      myParticipant.topology.synchronizer_parameters
        .get_dynamic_synchronizer_parameters(synchronizerId)
        .mediatorDeduplicationTimeout
      myParticipant.topology.synchronizer_parameters
        .get_dynamic_synchronizer_parameters(synchronizerId)
        .mediatorReactionTimeout
      myParticipant.topology.synchronizer_parameters
        .get_dynamic_synchronizer_parameters(synchronizerId)
        .assignmentExclusivityTimeout
      myParticipant.topology.synchronizer_parameters
        .get_dynamic_synchronizer_parameters(synchronizerId)
        .topologyChangeDelay
      myParticipant.topology.synchronizer_parameters
        .get_dynamic_synchronizer_parameters(synchronizerId)
        .ledgerTimeRecordTimeTolerance
      myParticipant.topology.synchronizer_parameters
        .get_dynamic_synchronizer_parameters(synchronizerId)
        .reconciliationInterval
      myParticipant.topology.synchronizer_parameters
        .get_dynamic_synchronizer_parameters(synchronizerId)
        .confirmationRequestsMaxRate
      myParticipant.topology.synchronizer_parameters
        .get_dynamic_synchronizer_parameters(synchronizerId)
        .maxRequestSize
      myParticipant.topology.synchronizer_parameters
        .get_dynamic_synchronizer_parameters(synchronizerId)
        .sequencerAggregateSubmissionTimeout
      myParticipant.topology.synchronizer_parameters
        .get_dynamic_synchronizer_parameters(synchronizerId)
        .trafficControl
      // user-manual-entry-begin:-end: GetSingleDynamicSynchronizerParameter

      // user-manual-entry-begin:-begin: SetDynamicSynchronizerParameters
      mySequencer.topology.synchronizer_parameters
        .propose_update(
          synchronizerId,
          _.update(
            confirmationResponseTimeout = 40.seconds,
            mediatorDeduplicationTimeout = 2.minutes,
            preparationTimeRecordTimeTolerance = 1.minute,
            mediatorReactionTimeout = 20.seconds,
            assignmentExclusivityTimeout = 1.second,
            topologyChangeDelay = 0.millis,
            reconciliationInterval = 5.seconds,
            confirmationRequestsMaxRate = 100,
            maxRequestSize = 100000,
            sequencerAggregateSubmissionTimeout = 5.minutes,
            trafficControl = Some(
              TrafficControlParameters(
                maxBaseTrafficAmount = NonNegativeLong.tryCreate(204800),
                readVsWriteScalingFactor = 200,
                maxBaseTrafficAccumulationDuration = 12.minutes,
                setBalanceRequestSubmissionWindowSize = 10.minutes,
                enforceRateLimiting = false,
                baseEventCost = NonNegativeLong.zero,
              )
            ),
          ),
        )

      // For ledger time record time tolerance, use the dedicated set method
      mySequencer.topology.synchronizer_parameters
        .set_ledger_time_record_time_tolerance(synchronizerId, 60.seconds)
      // user-manual-entry-begin:-end: SetDynamicSynchronizerParameters

      // user-manual-entry-begin:-begin: UpdateDynamicSynchronizerParameters,
      mySequencer.topology.synchronizer_parameters.propose_update(
        synchronizerId,
        _.update(
          confirmationResponseTimeout = 10.seconds,
          topologyChangeDelay = 1.second,
        ),
      )
      // user-manual-entry-begin:-end: UpdateDynamicSynchronizerParameters

      // If you add another dynamic synchronizer parameter, the following fails to compile. If so, add a new set/get method.
      mySequencer.topology.synchronizer_parameters.get_dynamic_synchronizer_parameters(
        synchronizerId
      ) match {
        case ConsoleDynamicSynchronizerParameters(
              _confirmationResponseTimeout,
              _mediatorReactionTimeout,
              _assignmentExclusivityTimeout,
              _topologyChangeDelay,
              _ledgerTimeRecordTimeTolerance,
              _mediatorDeduplicationTimeout,
              _reconciliationInterval,
              _confirmationRequestsMaxRate,
              _maxRequestSize,
              _sequencerAggregateSubmissionTimeout,
              _trafficControlParameters,
              _onboardingRestriction,
              _acsCommitmentsCatchupConfig,
              _preparationTimeRecordTimeTolerance,
            ) =>
          ()
      }
    }

    "update confirmationRequestsMaxRate to 0 and expect it to fail" in { implicit env =>
      import env.*

      val zero = NonNegativeInt.tryCreate(0)
      Seq(participant1, participant2).foreach(
        _.synchronizers.connect_local(daSynchronizer.sequencer, alias = daName)
      )
      Seq(participant1, participant2).foreach(
        _.packages.synchronize_vetting()
      )
      participant1.health.ping(participant2.id)

      updateDynamicSynchronizerParameters(
        daSynchronizer,
        _.update(confirmationRequestsMaxRate = zero),
      )
      eventually() {
        getCurrentSynchronizerParameters(daSynchronizer).confirmationRequestsMaxRate shouldBe zero
      }

      loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
        participant1.health.maybe_ping(participant2.id, config.NonNegativeDuration.ofSeconds(3)),
        LogEntry.assertLogSeq(
          Seq(
            (
              { entry =>
                entry.shouldBeCantonErrorCode(SequencerErrors.Overloaded)
                entry.message should include("Submission rate exceeds rate limit of 0/s.")
              },
              "Overload",
            ),
            (_.message should include("Failed ping"), "command failure"),
          )
        ),
      )
    }

    "error when setting the same synchronizer parameters twice" in { implicit env =>
      setDynamicSynchronizerParameters(daSynchronizer, defaultParameters)
      getCurrentSynchronizerParameters(daSynchronizer) shouldBe defaultParameters

      // Proposing an existing mapping with the same dynamic synchronizer properties triggers
      // an ALREADY_EXISTS topology error.
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        setDynamicSynchronizerParameters(daSynchronizer, defaultParameters),
        _.errorMessage should include regex "ALREADY_EXISTS/TOPOLOGY_MAPPING_ALREADY_EXISTS.*A matching topology mapping authorized with the same keys already exists in this state",
      )
    }

    "not change synchronizer parameters on alien synchronizers" in { implicit env =>
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        daSynchronizer.sequencer.topology.synchronizer_parameters
          .propose(
            acmeSynchronizer.synchronizerId,
            defaultParameters,
            store = daSynchronizer.synchronizerId,
            // explicitly specifying the desired signing key and force flag to not trigger
            // the error NO_APPROPRIATE_SINGING_KEY_IN_STORE while automatically determining
            // a suitable signing key
            signedBy = Some(daSynchronizer.sequencer.id.fingerprint),
            mustFullyAuthorize = true,
            force = ForceFlags(ForceFlag.AllowUnvalidatedSigningKeys),
          ),
        _.shouldBeCommandFailure(
          InvalidSynchronizer,
          s"Invalid synchronizer ${acmeSynchronizer.synchronizerId}",
        ),
      )
    }

    val secureConfigurationAsset: SecurityTest = SecurityTest(
      property = SecureConfiguration,
      asset = "synchronizer",
    )

    def increaseTolerance(
        p: ConsoleDynamicSynchronizerParameters
    ): ConsoleDynamicSynchronizerParameters =
      p.update(preparationTimeRecordTimeTolerance = p.preparationTimeRecordTimeTolerance * 2)

    "not configure insecure values for mediatorDeduplicationTimeout and preparationTimeRecordTimeTolerance" taggedAs secureConfigurationAsset
      .setAttack(
        Attack(
          actor = "a malicious participant",
          threat = "submits two requests with the same UUID",
          mitigation = "The mediator rejects the second requests.",
        )
      ) in { _ =>
      // Prevent construction of synchronizer parameters with mediatorDeduplicationTimeout being too low
      val d = config.NonNegativeFiniteDuration.ofSeconds(10)
      val ex = the[RuntimeException] thrownBy ConsoleDynamicSynchronizerParameters(
        confirmationResponseTimeout = config.NonNegativeFiniteDuration.Zero,
        mediatorReactionTimeout = config.NonNegativeFiniteDuration.Zero,
        assignmentExclusivityTimeout = config.NonNegativeFiniteDuration.Zero,
        topologyChangeDelay = d,
        ledgerTimeRecordTimeTolerance = config.NonNegativeFiniteDuration.Zero,
        mediatorDeduplicationTimeout = d,
        reconciliationInterval = config.PositiveDurationSeconds.ofSeconds(1),
        maxRequestSize = NonNegativeInt.zero,
        sequencerAggregateSubmissionTimeout = config.NonNegativeFiniteDuration.Zero,
        trafficControl = None,
        onboardingRestriction = OnboardingRestriction.UnrestrictedOpen,
        acsCommitmentsCatchUp = None,
        participantSynchronizerLimits =
          ParticipantSynchronizerLimits(confirmationRequestsMaxRate = NonNegativeInt.zero),
        preparationTimeRecordTimeTolerance = d,
      )
      ex.getMessage shouldBe "The preparationTimeRecordTimeTolerance (10s) must be at most half of the mediatorDeduplicationTimeout (10s)."
    }

    "require force to immediately increase preparationTimeRecordTimeTolerance" taggedAs secureConfigurationAsset
      .setAttack(
        Attack(
          actor = "a synchronizer operator",
          threat = "configures a too high value for preparationTimeRecordTimeTolerance by accident",
          mitigation =
            s"the synchronizer operator's topology managers require the use of the force flag ForceFlag.PreparationTimeRecordTimeToleranceIncrease",
        )
      ) in { implicit env =>
      // Refuse to increase preparationTimeRecordTimeTolerance without increasing mediatorDeduplicationTimeout
      val oldParams = getCurrentSynchronizerParameters(daSynchronizer)
      val oldTol = oldParams.preparationTimeRecordTimeTolerance
      val oldDedup = oldParams.mediatorDeduplicationTimeout

      val oldToErrorMessage = (oldTol * 2).toString
      val oldDedupErrorMessage = oldDedup.toString

      assertThrowsAndLogsCommandFailures(
        daSynchronizer.sequencer.topology.synchronizer_parameters
          .set_preparation_time_record_time_tolerance(daSynchronizer.synchronizerId, oldTol * 2),
        _.shouldBeCommandFailure(
          TopologyManagerError.IncreaseOfPreparationTimeRecordTimeTolerance,
          s"Unable to increase preparationTimeRecordTimeTolerance to $oldToErrorMessage, " +
            s"because it must not be more than half of mediatorDeduplicationTimeout ($oldDedupErrorMessage).",
        ),
      )

      // Increase mediatorDeduplicationTimeout up front so it won't prevent increasing tolerance
      updateDynamicSynchronizerParameters(
        daSynchronizer,
        p => p.update(mediatorDeduplicationTimeout = p.mediatorDeduplicationTimeout * 2),
      )

      // Require force to increase the parameter.
      assertThrowsAndLogsCommandFailures(
        updateDynamicSynchronizerParameters(daSynchronizer, increaseTolerance),
        _.shouldBeCommandFailure(
          TopologyManagerError.IncreaseOfPreparationTimeRecordTimeTolerance,
          s"The parameter preparationTimeRecordTimeTolerance can currently not be increased to",
        ),
      )

      // Increase the parameter when using force
      val params = getCurrentSynchronizerParameters(daSynchronizer)
      val newParams = increaseTolerance(params)
      daSynchronizer.sequencer.topology.synchronizer_parameters.propose(
        daSynchronizer.synchronizerId,
        newParams,
        mustFullyAuthorize = true,
        force = ForceFlags(ForceFlag.PreparationTimeRecordTimeToleranceIncrease),
      )

      getCurrentSynchronizerParameters(daSynchronizer) shouldBe newParams
    }
  }

  "A participant" can {
    "query the synchronizer parameters" in { implicit env =>
      import env.*

      eventually() {
        // participant needs a bit more time before seeing the changes
        ConsoleDynamicSynchronizerParameters(
          participant1.topology.synchronizer_parameters
            .list(
              store = daSynchronizer.synchronizerId
            )
            .head
            .item
        ) shouldBe getCurrentSynchronizerParameters(daSynchronizer)
      }
    }
  }

  private[dynamicsynchronizerparameters] case class TestDynamicParams[P](
      synchronizer: SynchronizerParametersFixture.Synchronizer,
      setValue: (ConsoleDynamicSynchronizerParameters, P) => ConsoleDynamicSynchronizerParameters,
      getValue: ConsoleDynamicSynchronizerParameters => P,
      initialValue: P,
      newValue: P,
      name: String,
  ) {
    def setDynamicParameter(value: P): Unit =
      synchronizer.sequencer.topology.synchronizer_parameters
        .propose_update(
          synchronizer.synchronizerId,
          setValue(_, value),
        )

    def getDynamicParameter(): P = {
      val parameters = synchronizer.sequencer.topology.synchronizer_parameters
        .get_dynamic_synchronizer_parameters(synchronizer.synchronizerId)
      getValue(parameters)
    }
  }

  private object TestDynamicParams {
    def runTest[P](test: TestDynamicParams[P]): Assertion = {
      test.setDynamicParameter(test.initialValue)

      val initialParameterValue = test.getDynamicParameter()
      val expectedInitialParameterValue = test.initialValue

      initialParameterValue shouldBe expectedInitialParameterValue

      test.setDynamicParameter(test.newValue)
      test.getDynamicParameter() shouldBe test.newValue
    }
  }
}

class SynchronizerParametersChangeIntegrationTestInMemory
    extends SynchronizerParametersChangeIntegrationTest {
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[StorageConfig.Memory](
      loggerFactory,
      sequencersForPlugin,
    )
  )
}

class SynchronizerParametersChangeBftOrderingIntegrationTestInMemory
    extends SynchronizerParametersChangeIntegrationTest {
  registerPlugin(
    new UseBftSequencer(loggerFactory, sequencersForPlugin)
  )
}

trait SynchronizerParametersRestartIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SynchronizerParametersFixture {
  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P0_S1M1

  private lazy val defaultParameters =
    ConsoleDynamicSynchronizerParameters.defaultValues(testedProtocolVersion)

  "Dynamic synchronizer parameters" should {
    "not be read from config upon restart" in { implicit env =>
      import env.*

      // initial setup
      getCurrentSynchronizerParameters(daSynchronizer) shouldBe defaultParameters

      // change synchronizer parameters
      val newSynchronizerParameters = increaseConfirmationResponseTimeout(defaultParameters)
      updateDynamicSynchronizerParameters(daSynchronizer, increaseConfirmationResponseTimeout)
      getCurrentSynchronizerParameters(daSynchronizer) shouldBe newSynchronizerParameters

      // restarting the synchronizer
      mediator1.stop()
      sequencer1.stop()
      sequencer1.is_running shouldBe false
      sequencer1.start()
      mediator1.start()

      // we should get the new value upon restart
      getCurrentSynchronizerParameters(daSynchronizer) shouldBe newSynchronizerParameters
    }
  }
}

// No SynchronizerParametersRestartIntegrationTestInMemory as persistence is required by restart

class SynchronizerParametersRestartIntegrationTestPostgres
    extends SynchronizerParametersRestartIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

class SynchronizerParametersRestartBftOrderingIntegrationTestPostgres
    extends SynchronizerParametersRestartIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
