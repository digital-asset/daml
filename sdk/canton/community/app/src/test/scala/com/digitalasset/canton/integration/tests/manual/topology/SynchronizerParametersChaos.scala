// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import com.digitalasset.canton.admin.api.client.data.DynamicSynchronizerParameters
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.Mutex
import com.digitalasset.canton.{ScalaFuturesWithPatience, config}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.concurrent.{Future, blocking}

/** This changes one synchronizer parameter at a time (alternating between different values).
  *
  * Parameter is selected in constructor. It uses global lock to prevent concurrent changes to
  * params.
  * @param parameterToChange
  *   selected synchronizer parameter to change
  */
class SynchronizerParametersChaos(
    val parameterToChange: SynchronizerParametersChaos.Param,
    val logger: TracedLogger,
) extends ScalaFuturesWithPatience
    with TopologyOperations {

  override def name: String = "SynchronizerParametersChaos"

  override def companion: TopologyOperationsCompanion = SynchronizerParametersChaos

  private val initialSynchronizerParams: AtomicReference[Option[DynamicSynchronizerParameters]] =
    new AtomicReference(None)

  override def runTopologyChanges()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Future[Unit] = {
    import env.*
    val initialParams = getInitialSynchronizerParams(env)
    val synchronizerOwners =
      getSynchronizerOwners(synchronizerId = synchronizer1Id, sequencer1).collect {
        case p: ParticipantReference => p
      }
    val n = (synchronizerOwners.size - 1) / 3
    val selectedMajorityOfParticipants = synchronizerOwners.take(2 * n + 1)

    val operations = Seq(
      () =>
        setParameterBlocking(initialParams, synchronizer1Id, selectedMajorityOfParticipants, 1.2),
      () =>
        setParameterBlocking(initialParams, synchronizer1Id, selectedMajorityOfParticipants, 1.1),
      () =>
        setParameterBlocking(initialParams, synchronizer1Id, selectedMajorityOfParticipants, 1.0),
    ).map(topologyChange => Future(topologyChange()))

    Future.sequence(operations).map(_ => ())
  }

  private def getInitialSynchronizerParams(env: TestConsoleEnvironment) = {
    import env.*
    val currentSynchronizerParams = participant1.topology.synchronizer_parameters
      .get_dynamic_synchronizer_parameters(synchronizer1Id)
    initialSynchronizerParams
      .updateAndGet { p =>
        Some(p.getOrElse(currentSynchronizerParams))
      }
      .getOrElse(currentSynchronizerParams)
  }

  private def setParameter(
      base: DynamicSynchronizerParameters,
      synchronizerId: SynchronizerId,
      participants: Set[ParticipantReference],
      multiplier: Double,
  ): Unit = {
    import com.digitalasset.canton.integration.tests.manual.topology.SynchronizerParametersChaos.*
    participants.foreach { participant =>
      parameterToChange match {
        case ConfirmationResponseTimeout =>
          participant.topology.synchronizer_parameters
            .propose_update(
              synchronizerId = synchronizerId,
              _.update(confirmationResponseTimeout =
                multiply(base.confirmationResponseTimeout, multiplier)
              ),
            )
        case MediatorReactionTimeout =>
          participant.topology.synchronizer_parameters
            .propose_update(
              synchronizerId = synchronizerId,
              _.update(mediatorReactionTimeout = multiply(base.mediatorReactionTimeout, multiplier)),
            )
        case AssignmentExclusivityTimeout =>
          participant.topology.synchronizer_parameters
            .propose_update(
              synchronizerId = synchronizerId,
              _.update(assignmentExclusivityTimeout =
                multiply(base.assignmentExclusivityTimeout, multiplier)
              ),
            )
        case LedgerTimeRecordTimeTolerance =>
          participant.topology.synchronizer_parameters
            .set_ledger_time_record_time_tolerance(
              synchronizerId = synchronizerId,
              multiply(base.ledgerTimeRecordTimeTolerance, multiplier),
            )
        case MediatorDeduplicationTimeout =>
          participant.topology.synchronizer_parameters
            .propose_update(
              synchronizerId = synchronizerId,
              _.update(mediatorDeduplicationTimeout =
                multiply(base.mediatorDeduplicationTimeout, multiplier)
              ),
            )

        case SynchronizerParametersChaos.ReconciliationInterval =>
          participant.topology.synchronizer_parameters
            .propose_update(
              synchronizerId = synchronizerId,
              _.update(reconciliationInterval = multiply(base.reconciliationInterval, multiplier)),
            )
        case ConfirmationRequestsMaxRate =>
          participant.topology.synchronizer_parameters
            .propose_update(
              synchronizerId = synchronizerId,
              _.update(confirmationRequestsMaxRate =
                multiply(base.confirmationRequestsMaxRate, multiplier)
              ),
            )
        case MaxRequestSize =>
          participant.topology.synchronizer_parameters
            .propose_update(
              synchronizerId = synchronizerId,
              _.update(maxRequestSize = multiply(base.maxRequestSize, multiplier)),
            )

        case SequencerAggregateSubmissionTimeout =>
          participant.topology.synchronizer_parameters
            .propose_update(
              synchronizerId = synchronizerId,
              _.update(sequencerAggregateSubmissionTimeout =
                multiply(base.sequencerAggregateSubmissionTimeout, multiplier)
              ),
            )
      }
    }
  }
  @SuppressWarnings(Array("com.digitalasset.canton.RequireBlocking"))
  private def setParameterBlocking(
      base: DynamicSynchronizerParameters,
      synchronizerId: SynchronizerId,
      participants: Set[ParticipantReference],
      multiplier: Double,
  )(implicit loggingContext: ErrorLoggingContext, env: TestConsoleEnvironment): Unit = {
    import com.digitalasset.canton.integration.tests.manual.topology.SynchronizerParametersChaos.*
    blocking {
      lock.exclusive {
        withOperation_("setParameters")(
          s"setting synchronizer parameter $parameterToChange for $synchronizerId factor $multiplier"
        ) {
          setParameter(base, synchronizerId, participants, multiplier)
        }
      }
    }
  }

  private def multiply(
      value: NonNegativeInt,
      multiplier: Double,
  ): NonNegativeInt = NonNegativeInt.tryCreate((value.value * multiplier).toInt)

  private def multiply(
      duration: config.NonNegativeFiniteDuration,
      multiplier: Double,
  ): config.NonNegativeFiniteDuration = config.NonNegativeFiniteDuration
    .fromDuration((duration.underlying.toMillis * multiplier).toInt.millisecond)
    .getOrElse(duration)

  private def multiply(
      duration: config.PositiveDurationSeconds,
      multiplier: Double,
  ): config.PositiveDurationSeconds = config.PositiveDurationSeconds
    .fromDuration((duration.underlying.toMillis * multiplier).toInt.millisecond)
    .getOrElse(duration)
}

object SynchronizerParametersChaos extends TopologyOperationsCompanion {
  // shared lock for all instances
  val lock = new Mutex()
  sealed trait Param

  case object ConfirmationResponseTimeout extends Param
  case object MediatorReactionTimeout extends Param
  case object AssignmentExclusivityTimeout extends Param
  case object LedgerTimeRecordTimeTolerance extends Param
  case object MediatorDeduplicationTimeout extends Param
  case object ReconciliationInterval extends Param
  case object ConfirmationRequestsMaxRate extends Param
  case object MaxRequestSize extends Param
  case object SequencerAggregateSubmissionTimeout extends Param
}
