// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.console.{ParticipantReference, SequencerReference}
import com.digitalasset.canton.integration.{CommunityIntegrationTest, SharedEnvironment}
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters
import com.digitalasset.canton.time.DelegatingSimClock
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId.Synchronizer

import java.time.Instant

/** Facilitates writing tests that exercise an offline party replication.
  *
  * Offline party replication requires a silent synchronizer. A synchronizer is silent when the
  * transaction processing has been halted.
  *
  * Halting the transaction processing is accomplished by setting the synchronizer parameter
  * `confirmationRequestsMaxRate` to 0.
  *
  * Effectively changing the `confirmationRequestsMaxRate` requires to wait minimally for the total
  * duration of `mediatorReactionTimeout` and the `confirmationResponseTimeout` to have elapsed.
  */
private[multihostedparties] trait UseSilentSynchronizerInTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  private val loweredTimeout = config.NonNegativeFiniteDuration.ofSeconds(4)
  private val bufferTime = config.NonNegativeFiniteDuration.ofMillis(100)
  private val mediatorReactionTimeout = loweredTimeout
  private val confirmationResponseTimeout = loweredTimeout

  /** Minimal wait time for a synchronizer parameters change to become valid.
    */
  private val waitTime = mediatorReactionTimeout + confirmationResponseTimeout + bufferTime

  /** Adjusts the `mediatorReactionTimeout` and the `confirmationResponseTimeout` such that the
    * minimal wait time, after having silenced a synchronizer, is as short as possible.
    *
    * Hint: That such minimal wait time has indeed elapsed before proceeding is checked in the party
    * replication repair macros, for example.
    */
  protected def adjustTimeouts(sequencer: SequencerReference): Unit =
    sequencer.topology.synchronizer_parameters.propose_update(
      synchronizerId = sequencer.synchronizer_id,
      _.update(
        mediatorReactionTimeout = mediatorReactionTimeout,
        confirmationResponseTimeout = confirmationResponseTimeout,
      ),
    )

  /** Returns the effective time from when on the synchronizer is silent.
    */
  protected def silenceSynchronizerAndAwaitEffectiveness(
      synchronizer: Synchronizer,
      sequencer: SequencerReference,
      participant: ParticipantReference,
      simClock: Option[DelegatingSimClock],
  ): Instant =
    silenceSynchronizerAndAwaitEffectiveness(synchronizer, Seq(sequencer), participant, simClock)

  /** Returns the effective time from when on the synchronizer is silent.
    */
  protected def silenceSynchronizerAndAwaitEffectiveness(
      synchronizer: Synchronizer,
      sequencers: Seq[SequencerReference],
      participant: ParticipantReference,
      simClock: Option[DelegatingSimClock] = None,
  ): Instant = {
    sequencers.foreach(
      _.topology.synchronizer_parameters.propose_update(
        synchronizer.logicalSynchronizerId,
        _.update(confirmationRequestsMaxRate = NonNegativeInt.zero),
      )
    )

    eventually() {
      val confirmationRequestsMaxRate = participant.topology.synchronizer_parameters
        .latest(store = synchronizer)
        .participantSynchronizerLimits
        .confirmationRequestsMaxRate

      confirmationRequestsMaxRate shouldBe NonNegativeInt.zero
    }

    waitForSynchronizerParametersChange(simClock)

    participant.topology.synchronizer_parameters
      .list(store = synchronizer)
      .filter { change =>
        change.item.participantSynchronizerLimits.confirmationRequestsMaxRate == NonNegativeInt.zero
      }
      .map(change => change.context.validFrom)
      .loneElement
  }

  protected def resumeSynchronizerAndAwaitEffectiveness(
      synchronizer: Synchronizer,
      sequencer: SequencerReference,
      participant: ParticipantReference,
      simClock: Option[DelegatingSimClock],
  ): Unit =
    resumeSynchronizerAndAwaitEffectiveness(synchronizer, Seq(sequencer), participant, simClock)

  protected def resumeSynchronizerAndAwaitEffectiveness(
      synchronizer: Synchronizer,
      sequencers: Seq[SequencerReference],
      participant: ParticipantReference,
      simClock: Option[DelegatingSimClock] = None,
  ): Unit = {
    sequencers.foreach(
      _.topology.synchronizer_parameters.propose_update(
        synchronizer.logicalSynchronizerId,
        _.update(confirmationRequestsMaxRate =
          DynamicSynchronizerParameters.defaultConfirmationRequestsMaxRate
        ),
      )
    )

    eventually() {
      val confirmationRequestsMaxRate = participant.topology.synchronizer_parameters
        .latest(store = synchronizer)
        .participantSynchronizerLimits
        .confirmationRequestsMaxRate

      confirmationRequestsMaxRate shouldBe DynamicSynchronizerParameters.defaultConfirmationRequestsMaxRate
    }

    waitForSynchronizerParametersChange(simClock)
  }

  /** Facilitates waiting for a minimum duration in tests, specifically `mediatorReactionTimeout` +
    * `confirmationResponseTimeout`, after a synchronizer has been silenced. This delay is essential
    * for certain assertions, such as party replication repair macro checks, to ensure sufficient
    * time has elapsed before proceeding.
    *
    * Ideally, tests should avoid real-time waits. For tests utilizing a `SimClock`, time can be
    * advanced programmatically. Tests requiring actual time sleeps, it's important to configure
    * `mediatorReactionTimeout` and `confirmationResponseTimeout` to their shortest practical values
    * to minimize wait times without introducing test flakiness (that is use
    * [[UseSilentSynchronizerInTest#adjustTimeouts]]).
    */
  private def waitForSynchronizerParametersChange(simClock: Option[DelegatingSimClock]): Unit =
    simClock.fold(Threading.sleep(waitTime.duration.toMillis))(_.advance(waitTime.asJava))

}
