// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.digitalasset.canton.admin.api.client.data.topology.ListSequencerSynchronizerStateResult
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.console.{
  InstanceReference,
  LocalSequencerReference,
  SequencerReference,
}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.tests.manual.topology.TopologyOperations.RichIterable
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.topology.ForceFlag
import com.digitalasset.canton.topology.store.TimeQuery
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.retry.{AllExceptionRetryPolicy, Pause, Success}

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

private[topology] class AddSequencer(
    sequencerToAdd: LocalSequencerReference,
    sequencerToInitializeFrom: SequencerReference,
    override val timeouts: ProcessingTimeout,
    override val logger: TracedLogger,
)(implicit override val ec: ExecutionContext, override val env: TestConsoleEnvironment)
    extends TopologyChange {

  import env.*

  override protected val topologyChangeName = s"add ${sequencerToAdd.name}"

  override val referenceNode: InstanceReference = sequencerToInitializeFrom

  override def nodesThatAffectChange(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Seq[InstanceReference] = getSynchronizerOwners(daId, referenceNode).forgetNE.toSeq

  override def preChange()(implicit errorLoggingContext: ErrorLoggingContext): Future[Unit] = {
    clue(topologyChangeName)(s"waiting for ${sequencerToAdd.name}'s identity") {
      sequencerToAdd.start()
      sequencerToAdd.health.wait_for_ready_for_initialization()
    }
    val sequencerIdentity = sequencerToAdd.topology.transactions.identity_transactions()

    clue(topologyChangeName)(s"upload identity to ${referenceNode.name}")(
      referenceNode.topology.transactions
        .load(sequencerIdentity, store = daId, ForceFlag.AlienMember)
    )

    val observeIdentityDesc = s"check all owners observe identity"
    clueF(topologyChangeName)(observeIdentityDesc)(
      getSynchronizerOwners(daId, referenceNode).forgetNE.toSeq.parTraverse { owner =>
        Pause(logger, this, 20, 500.millis, observeIdentityDesc)(
          Future.successful(
            owner.topology.namespace_delegations
              .list(
                store = daId,
                timeQuery = TimeQuery.Snapshot(env.environment.clock.now),
                filterNamespace = sequencerToAdd.id.namespace.filterString,
              )
              .sizeCompare(1) == 0
          ),
          AllExceptionRetryPolicy,
        )(Success.boolean, ec, errorLoggingContext.traceContext, implicitly)
      }
    ).map { allResults =>
      require(
        allResults.forall(identity),
        fullMessage(
          topologyChangeName,
          s"not all nodes observed the identity of ${sequencerToAdd.name}",
        ),
      )

    }
  }

  override def affectPartialChange(
      node: InstanceReference
  )(implicit errorLoggingContext: ErrorLoggingContext): Future[Unit] = Future {
    val currentSDS = getSequencersSynchronizerState(node)

    node.topology.sequencers.propose(
      daId,
      threshold = currentSDS.item.threshold,
      active = currentSDS.item.active :+ sequencerToAdd.id,
      passive = currentSDS.item.observers,
      serial = Some(currentSDS.context.serial.increment),
    )
  }

  override def validatePartialChange(
      node: InstanceReference
  )(implicit errorLoggingContext: ErrorLoggingContext): Boolean = {
    val latestAuthorized = getSequencersSynchronizerState(node).item.active
      .contains(sequencerToAdd.id)
    lazy val latestProposal = node.topology.sequencers
      .list(store = daId, proposals = true)
      .exists(result =>
        result.item.active.contains(sequencerToAdd.id) && result.context.signedBy
          .contains(node.id.fingerprint)
      ) // also check that $node sees its own signature
    latestAuthorized || latestProposal
  }

  private def getSequencersSynchronizerState(
      node: InstanceReference
  ): ListSequencerSynchronizerStateResult =
    node.topology.sequencers
      .list(store = daId)
      .loneElement(s"get sequencers for synchronizer `$daId` on node ${node.id}")

  override def validateFullChange()(implicit errorLoggingContext: ErrorLoggingContext): Boolean = {
    val currentSDS = getSequencersSynchronizerState(referenceNode)

    currentSDS.item.active.contains(sequencerToAdd.id)
  }

  override def postChange()(implicit errorLoggingContext: ErrorLoggingContext): Future[Unit] =
    Future {
      clue(topologyChangeName)("initialize sequencer") {
        val sequencerState = sequencerToInitializeFrom.topology.sequencers
          .list(store = daId)
          .loneElement(s"get sequencers for synchronizer `$daId`")
        require(
          sequencerState.item.active.contains(sequencerToAdd.id),
          fullMessage(
            topologyChangeName,
            s"did not find ${sequencerToAdd.id} in the supposed onboarding transaction ${sequencerState.item}",
          ),
        )

        val onboardingState =
          sequencerToInitializeFrom.setup.onboarding_state_for_sequencer(sequencerToAdd.id)
        sequencerToAdd.setup.assign_from_onboarding_state(onboardingState)
        sequencerToAdd.health.wait_for_initialized()

      }
    }

}

class SequencerChaos(override val logger: TracedLogger) extends TopologyOperations {

  override def name: String = "SequencerGroupChaos"

  override def companion: TopologyOperationsCompanion = SequencerChaos

  private val sequencersInProgress = new TrieMap[String, Unit]()

  override def runTopologyChanges()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Future[Unit] = {
    import env.*
    Future.sequence(addSequencers(numSequencersToAdd = 2)).void
  }

  def addSequencers(numSequencersToAdd: Int)(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
  ): List[Future[Unit]] = {
    import env.*
    val alreadyOnboardedSequencers = sequencer1.topology.sequencers
      .list(daId)
      .loneElement(s"list sequencers for synchronizer `$daId`")
      .item
      .allSequencers
    val sequencersToOnboard = sequencers.local.view
      .filter { ls =>
        val notAlreadyOnboarded = !alreadyOnboardedSequencers.exists(onboarded =>
          // sequencers aren't necessarily started yet, so `ls.id` won't work
          onboarded.uid.toProtoPrimitive.startsWith(ls.name)
        )
        val notInProgress = sequencersInProgress.putIfAbsent(ls.name, ()).isEmpty

        notAlreadyOnboarded && notInProgress
      }
      .take(numSequencersToAdd)
      .toList

    if (sequencersToOnboard.isEmpty) {
      logOperationStep("add sequencer")("No sequencer left to onboard")
      List.empty
    } else {
      sequencersToOnboard.start()
      // first start all sequencers so the actual onboarding is concurrent
      sequencersToOnboard.map { sequencer =>
        withOperation_(s"add sequencer")(s"onboarding ${sequencer.name}")
        val future = new AddSequencer(
          sequencer,
          sequencer1,
          environment.config.parameters.timeouts.processing,
          loggingContext.logger,
        ).run()
        future.onComplete { _ =>
          sequencersInProgress.remove(sequencer.name)
        }
        future
      }
    }
  }
}

object SequencerChaos extends TopologyOperationsCompanion {
  override def acceptableLogEntries: Seq[String] = Seq(
    "is already past the max sequencing time"
  )
}
