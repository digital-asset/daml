// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import better.files.File
import cats.syntax.parallel.*
import com.digitalasset.canton.BaseTest.eventually
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.{BaseTest, config}
import org.scalatest.EitherValues.*
import org.scalatest.OptionValues.convertOptionToValuable
import org.slf4j.event.Level

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Future, blocking}
import scala.math.Ordering.Implicits.*

/*
This class provides helpers to do party onboarding (replication) and offboarding.
Note that it is assumes that there is only one active party in the target participant.

Exclusive resources:
- participant3
- participant4

Party onboarding:
- Authorizing the party (observation rights)
- Importing the ACS on the target participants
- After some time, changing permission to confirmation

Party offboarding:
- Un-authorizing the party
- Deleting the contracts
 */
@SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
private[topology] class PartyReplicationChaos(val logger: TracedLogger) extends TopologyOperations {
  override def name: String = "PartyReplicationChaos"

  import TopologyOperations.*
  import PartyReplicationChaos.*
  import org.scalatest.matchers.should.Matchers.*

  private val workerStatus: TrieMap[ParticipantId, Status] = TrieMap()

  override def companion: TopologyOperationsCompanion = PartyReplicationChaos

  /*
  The decision time out has an impact on the progress of the record order publisher. Since we are using the ping
  in the offboarding to make sure that a timestamp becomes clean, we need to make sure that timeout for the ping
  is big enough.
   */
  private def pingTimeout(
      decisionTimeout: config.NonNegativeFiniteDuration
  ): config.NonNegativeDuration =
    config.NonNegativeDuration.ofMillis(decisionTimeout.underlying.toMillis + 5000)

  override lazy val reservations: Reservations =
    Reservations(exclusiveParticipants = fromToParticipants.values.toSeq)

  /*
    For each (key, value) we will attempt to replicate a party from `key` to `value`.
    Note that we expect exclusive usage of `value`
   */
  private lazy val fromToParticipants: Map[String, String] =
    Map("participant1" -> "participant3", "participant2" -> "participant4")

  override def runTopologyChanges()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Future[Unit] = {
    import env.*

    val actions: Seq[Action] = fromToParticipants.toList.map { case (sourceName, targetName) =>
      val source = lp(sourceName)
      val target = lp(targetName)

      nextAction(source, target)
    }

    actions.parTraverse_ {
      case NoAction => Future.unit
      case op: ReplicateParty =>
        Future(replicateParty(op))
      case ChangePermissions(party, partyOwner, participant) =>
        Future {
          changePermissions(party, partyOwner, participant, ParticipantPermission.Confirmation)
        }
      case operation: OffboardParty =>
        Future(offboardParty(operation))
    }
  }

  // Since the operations are ran asynchronously, I don't expect performance
  // issues with this synchronized block
  @SuppressWarnings(Array("com.digitalasset.canton.RequireBlocking"))
  private def nextAction(
      sourceParticipant: LocalParticipantReference,
      targetParticipant: LocalParticipantReference,
  )(implicit
      env: TestConsoleEnvironment,
      loggingContext: ErrorLoggingContext,
      globalReservations: Reservations,
  ): Action = {
    val now = env.environment.clock.now

    blocking(this.synchronized {
      val currentStatus =
        workerStatus.getOrElse(targetParticipant.id, Idle(CantonTimestamp.MinValue))
      currentStatus match {
        case Idle(from) =>
          if ((now - from) >= quietTime.asJava) { // we don't do the replication too often
            selectCandidateForReplication(sourceParticipant) match {
              case Some(party) =>
                workerStatus.put(targetParticipant.id, WIP)
                ReplicateParty(party, sourceParticipant, targetParticipant)

              case None =>
                logger.warn("Unable to find a party for replication.")
                NoAction
            }
          } else NoAction

        case WIP => NoAction

        case observing: PartyObserving =>
          if (now >= observing.minimumConfirmingTime) {
            workerStatus.put(targetParticipant.id, WIP)
            ChangePermissions(observing.party, sourceParticipant, targetParticipant)
          } else {
            NoAction
          }

        case confirming: PartyConfirming =>
          if (now >= confirming.minimumOffboardingTime) {
            workerStatus.put(targetParticipant.id, WIP)
            OffboardParty(confirming.party, sourceParticipant, targetParticipant)
          } else {
            NoAction
          }
      }
    })
  }

  override def additionalSetupPhase()(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    reservations.exclusiveParticipants.foreach { pName =>
      val participant = lp(pName)
      participant.start()
      participant.synchronizers.connect(sequencer1, daName)
      participant.dars.upload(BaseTest.PerformanceTestPath)
    }
  }

  override def initialization()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Unit =
    initializationTime.set(Some(env.environment.clock.now))

  private def selectCandidateForReplication(p: ParticipantReference)(implicit
      env: TestConsoleEnvironment,
      gloabalReservations: Reservations,
  ): Option[PartyId] = {
    val parties = hostedParties(p)
      .filter(pp => canUseParty(pp._1))
      .map { case (partyId, _) => partyId }
      .toSeq

    if (parties.isEmpty)
      None
    else
      Some(parties(Math.floor(Math.random() * parties.size).toInt))
  }

  private def replicateParty(
      replicateParty: ReplicateParty
  )(implicit
      env: TestConsoleEnvironment,
      errorLoggingContext: ErrorLoggingContext,
  ): Unit = {
    import env.*

    val party = replicateParty.party
    val from = replicateParty.from
    val to = replicateParty.to
    val operation = s"Replicating party $party from ${from.id} to ${to.id}"
    val fromLedgerEnd = from.ledger_api.state.end()

    logOperationStep("party replication")(operation)

    val dynamicSynchronizerParameters =
      from.topology.synchronizer_parameters.get_dynamic_synchronizer_parameters(daId)

    val decisionTimeout =
      dynamicSynchronizerParameters.confirmationResponseTimeout + dynamicSynchronizerParameters.mediatorReactionTimeout

    def propose(p: LocalParticipantReference): Unit = p.topology.party_to_participant_mappings
      .propose_delta(
        party,
        adds = List((to.id, ParticipantPermission.Observation)),
        store = daId,
      )
      .discard

    propose(to)
    to.synchronizers.disconnect(daName)
    propose(from)

    logOperationStep("party replication")(
      "proposals are sent, synchronizing...",
      level = Level.DEBUG,
    )

    val onboardingTx = clue("party replication")(s"$party authorizes hosting on ${to.id}") {
      eventually(topologyChangeTimeout.asFiniteApproximation, retryOnTestFailuresOnly = false) {
        logOperationStep("party replication")(
          s"Querying party to participant mappings for $party and participant ${to.id})",
          level = Level.DEBUG,
        )

        from.topology.party_to_participant_mappings
          .list(
            synchronizerId = daId,
            filterParty = party.filterString,
            filterParticipant = to.filterString,
          )
          .loneElement(s"$operation: $party authorizes replication")
          .context
      }
    }

    val partyAddedOffset = from.parties.find_party_max_activation_offset(
      partyId = party,
      participantId = to.id,
      synchronizerId = daId,
      validFrom = Some(onboardingTx.validFrom),
      beginOffsetExclusive = fromLedgerEnd,
      completeAfter = PositiveInt.one,
    )
    logOperationStep("party replication")(
      s"Exporting ACS at offset $partyAddedOffset for $party from ${from.id}"
    )

    File.usingTemporaryFile() { file =>
      clue("party replication")(s"exporting acs for $party from ${from.id}") {
        BaseTest.eventually(retryOnTestFailuresOnly = false) {
          from.repair.export_acs(
            parties = Set(party),
            exportFilePath = file.canonicalPath,
            synchronizerId = Some(daId),
            ledgerOffset = partyAddedOffset,
          )
        }
      }

      to.repair.import_acsV2(importFilePath = file.canonicalPath, synchronizerId = daId)
      logOperationStep("party replication")(
        s"Done replicating party $party from ${from.id} to ${to.id}; reconnecting ${to.id} to the synchronizer"
      )
      to.synchronizers.reconnect(daName)

      val now = environment.clock.now
      val minimumConfirmingTime: CantonTimestamp = now
        .add(decisionTimeout.asJava)

      workerStatus.put(to.id, PartyObserving(party, minimumConfirmingTime))
    }
  }

  /** Offboard party from participant `from`
    */
  private def offboardParty(
      offboardParty: OffboardParty
  )(implicit env: TestConsoleEnvironment, loggingContext: ErrorLoggingContext): Unit = {
    import env.*

    val party = offboardParty.party
    val partyOwner = offboardParty.partyOwner
    val from = offboardParty.from
    val operation = s"Offboarding party $party from ${from.id}"
    val fromLedgerEnd = from.ledger_api.state.end()

    logOperationStep("party offboarding")(operation)

    clue("party offboarding")(s"party $party is hosted on ${from.id}") {
      hostingParticipants(from, party).map { case (id, _) => id } should contain(from.id)
    }

    partyOwner.topology.party_to_participant_mappings
      .propose_delta(party, removes = List(from.id), store = daId)
      .discard

    // Give 2x the time to observe the change proposed on the "partyOwner" participant on the offboarding "from"
    // participant. We have seen this time out a number of times in the past.
    val topologyChangeObservedOnOtherParticipant = topologyChangeTimeout * 2

    val offboardingTx =
      eventually(
        timeUntilSuccess = topologyChangeObservedOnOtherParticipant.asFiniteApproximation,
        retryOnTestFailuresOnly = false,
      ) {
        from.topology.party_to_participant_mappings
          .list(
            synchronizerId = daId,
            filterParticipant = partyOwner.filterString,
            filterParty = party.filterString,
          )
          .filter(_.item.participants.forall(_.participantId != from.id))
          .loneElement(s"$operation: party revokes authorization")
      }

    val decisionTimeout =
      from.topology.synchronizer_parameters
        .get_dynamic_synchronizer_parameters(daId)
        .decisionTimeout

    // Need a ping to ensure that the clean cursor will eventually progress
    from.health.maybe_ping(from, timeout = pingTimeout(decisionTimeout))

    val partyRemovedOffset = from.parties.find_party_max_deactivation_offset(
      partyId = party,
      participantId = from.id,
      synchronizerId = daId,
      validFrom = Some(offboardingTx.context.validFrom),
      beginOffsetExclusive = fromLedgerEnd,
      completeAfter = PositiveInt.one,
    )

    logOperationStep("party offboarding")(
      s"Offset $partyRemovedOffset for ACS for $party from ${from.id}"
    )

    /*
        We know that there is no activity for the party between the snapshot and the disconnect because from the
        topology point of view, the party is not hosted on `from` anymore.
     */

    from.synchronizers.disconnect(daName)

    val contracts = from.ledger_api.state.acs
      .active_contracts_of_party(party, activeAtOffsetO = Some(partyRemovedOffset))
    val cids = contracts.map(c => LfContractId.fromString(c.createdEvent.value.contractId).value)

    from.repair.purge(daName, cids)
    from.synchronizers.reconnect(daName)

    workerStatus.put(from.id, Idle(environment.clock.now))

    logOperationStep("party offboarding")(s"Done offboarding party $party from ${from.id}")
  }

  private def changePermissions(
      party: PartyId,
      partyOwner: LocalParticipantReference,
      participant: LocalParticipantReference,
      newPermission: ParticipantPermission,
  )(implicit
      env: TestConsoleEnvironment,
      errorLoggingContext: ErrorLoggingContext,
  ): Unit = {
    import env.*

    logOperationStep("party replication")(
      s"Changing $party permissions to $newPermission on ${participant.id}"
    )

    Seq(partyOwner, participant).foreach(
      _.topology.party_to_participant_mappings
        .propose_delta(
          party,
          adds = List((participant.id, newPermission)),
          store = daId,
        )
    )

    val minimumOffboardingTime = env.environment.clock.now.plus(partyLifetime.asJava)
    workerStatus.put(participant.id, PartyConfirming(party, minimumOffboardingTime))

    eventually() {
      hostingParticipants(partyOwner, party) should contain((participant.id, newPermission))
    }

    logOperationStep("party replication")(
      s"Done changing $party permissions to $newPermission on ${participant.id}"
    )
  }

  private def hostingParticipants(
      p: LocalParticipantReference,
      party: PartyId,
  )(implicit env: TestConsoleEnvironment): Set[(ParticipantId, ParticipantPermission)] =
    p.topology.party_to_participant_mappings
      .list(synchronizerId = env.daId, filterParty = party.filterString)
      .flatMap(
        _.item.participants.map(participant => (participant.participantId, participant.permission))
      )
      .toSet

  private def hostedParties(
      p: ParticipantReference
  )(implicit env: TestConsoleEnvironment): Set[(PartyId, ParticipantPermission)] =
    p.topology.party_to_participant_mappings
      .list(synchronizerId = env.daId, filterParticipant = p.id.filterString)
      .map { result =>
        (
          result.item.partyId,
          result.item.participants
            .collect {
              case hostingParticipant if hostingParticipant.participantId == p.id =>
                hostingParticipant.permission
            }
            .loneElement(s"listing ptp on ${p.id}"),
        )
      }
      .toSet
}

private[topology] object PartyReplicationChaos extends TopologyOperationsCompanion {
  private sealed trait Action extends Product with Serializable
  private case object NoAction extends Action
  private final case class ReplicateParty(
      party: PartyId,
      from: LocalParticipantReference,
      to: LocalParticipantReference,
  ) extends Action
  private final case class ChangePermissions(
      party: PartyId,
      partyOwner: LocalParticipantReference,
      participant: LocalParticipantReference,
  ) extends Action
  private final case class OffboardParty(
      party: PartyId,
      partyOwner: LocalParticipantReference,
      from: LocalParticipantReference,
  ) extends Action

  sealed trait Status extends Product with Serializable
  final case class Idle(from: CantonTimestamp) extends Status
  case object WIP extends Status
  final case class PartyObserving(
      party: PartyId,
      minimumConfirmingTime: CantonTimestamp, // we don't want to switch to confirmation too early
  ) extends Status
  final case class PartyConfirming(
      party: PartyId,
      minimumOffboardingTime: CantonTimestamp, // we don't want to offboard party too early
  ) extends Status

  // Time between party being able to confirm and offboarding
  private val partyLifetime: config.NonNegativeFiniteDuration =
    config.NonNegativeFiniteDuration.ofSeconds(20)

  /*
    Time between one offboarding and the next onboarding.
    Party replication adds quite some load to the target participant which needs to catch up.
    As such, we don't want to do it too frequently.
   */
  private val quietTime: config.NonNegativeFiniteDuration =
    config.NonNegativeFiniteDuration.ofSeconds(20)

  override lazy val acceptableLogEntries: Seq[String] = Seq(
    // When changing hosting participants concurrently with transaction submission, we might have
    // different PTP between transaction submission and sequencing
    // ts for ACS export is not clean yet
    "The participant does not yet support serving an ACS snapshot at the requested timestamp",
    // Other
    "A security-sensitive error has been received", // Some of the logged error (see below) contain sensitive info
  )

  // When changing hosting participants concurrently with transaction submission,
  // we might have different PTP between transaction submission and sequencing
  override lazy val acceptableNonRetryableLogEntries: Seq[String] = Seq(
    "Rejected transaction due to a failed model conformance check: ViewReconstructionError"
  )
}
