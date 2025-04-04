// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.dynamicsynchronizerparameters

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  SendDecision,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration as InternalNonNegativeFiniteDuration

import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
trait LedgerTimeRecordTimeToleranceChangesIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with HasCycleUtils {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(ConfigTransforms.useStaticTime)
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, daName)
        defaultLedgerTimeRecordTimeTolerance = InternalNonNegativeFiniteDuration.fromConfig(
          sequencer1.topology.synchronizer_parameters
            .get_dynamic_synchronizer_parameters(daId)
            .ledgerTimeRecordTimeTolerance
        )

        sequencer = getProgrammableSequencer(sequencer1.name)
      }

  protected var sequencer: ProgrammableSequencer = _
  private var defaultLedgerTimeRecordTimeTolerance: InternalNonNegativeFiniteDuration = _

  /*
    For each confirmation request sent by participant1, we remember the
    current time as well as the max sequencing time using the provided
    hook.
   */
  private def setSequencerPolicy(
      rememberMaxSequencingTime: ((CantonTimestamp, CantonTimestamp)) => Unit
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    sequencer.setPolicy_("remember max sequencing times policy") { submissionRequest =>
      if (submissionRequest.sender == participant1.id && submissionRequest.isConfirmationRequest)
        rememberMaxSequencingTime((environment.now, submissionRequest.maxSequencingTime))

      SendDecision.Process
    }
  }

  private def submitCommand(id: String)(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    createCycleContract(participant1, participant1.adminParty, id)
  }

  "Computation of max sequencing time" should {
    "take updates of ledgerTimeRecordTimeTolerance into account" in { implicit env =>
      import env.*

      val confirmationRequestMaxSequencingTimes =
        new ConcurrentLinkedQueue[(CantonTimestamp, CantonTimestamp)]()
      setSequencerPolicy(confirmationRequestMaxSequencingTimes.add)

      // First command submitted with default dynamic synchronizer parameters.
      val command1Time = environment.now
      val command1ExpectedConfirmationRequestMaxSequencingTimes = List(
        (command1Time, command1Time + defaultLedgerTimeRecordTimeTolerance)
      )
      submitCommand("first-command")
      confirmationRequestMaxSequencingTimes.toArray.toList shouldBe command1ExpectedConfirmationRequestMaxSequencingTimes

      confirmationRequestMaxSequencingTimes.clear()

      // Double the mediatorDeduplicationTimeout and ledgerTimeRecordTimeTolerance
      sequencer1.topology.synchronizer_parameters.propose_update(
        daId,
        previous =>
          previous.update(
            mediatorDeduplicationTimeout = previous.mediatorDeduplicationTimeout * 2,
            ledgerTimeRecordTimeTolerance = previous.ledgerTimeRecordTimeTolerance * 2,
          ),
      )

      environment.simClock.value.advance(Duration.ofDays(1))

      // Second command submitted after doubling ledgerTimeRecordTimeTolerance.
      val command2Time = environment.now
      val command2ExpectedConfirmationRequestMaxSequencingTimes = List(
        (
          command2Time,
          // We expect the submitter to adapt Submission.maxSequencingTimeO according to
          // the doubled tolerance.
          command2Time + defaultLedgerTimeRecordTimeTolerance * NonNegativeInt
            .tryCreate(2),
        )
      )

      submitCommand("second-command")
      confirmationRequestMaxSequencingTimes.toArray.toList shouldBe command2ExpectedConfirmationRequestMaxSequencingTimes
    }
  }
}

class LedgerTimeRecordTimeToleranceChangesIntegrationTestDefault
    extends LedgerTimeRecordTimeToleranceChangesIntegrationTest {
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory)
  )
  // we need to register the ProgrammableSequencer after the ReferenceBlockSequencer
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}

class LedgerTimeRecordTimeToleranceChangesBftOrderingIntegrationTestDefault
    extends LedgerTimeRecordTimeToleranceChangesIntegrationTest {
  registerPlugin(
    new UseBftSequencer(loggerFactory)
  )
  // we need to register the ProgrammableSequencer after the ReferenceBlockSequencer
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}

//class LedgerTimeRecordTimeToleranceChangesIntegrationTestPostgres extends LedgerTimeRecordTimeToleranceChangesIntegrationTest {
//  registerPlugin(new UsePostgres(loggerFactory))
//  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
//  // we need to register the ProgrammableSequencer after the ReferenceBlockSequencer
//  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
//}
