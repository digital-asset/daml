// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import cats.syntax.parallel.*
import com.digitalasset.canton.BaseTest.eventually
import com.digitalasset.canton.admin.api.client.data.SequencerConnections
import com.digitalasset.canton.admin.api.client.data.topology.ListMediatorSynchronizerStateResult
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{
  InstanceReference,
  LocalMediatorReference,
  SequencerReference,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.ForceFlag
import com.digitalasset.canton.topology.store.TimeQuery
import com.digitalasset.canton.topology.transaction.{
  NamespaceDelegation,
  OwnerToKeyMapping,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.util.FutureInstances.*
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

/*
This class provides helpers to add and remove mediators from a mediator group and to
modify the mediator group threshold.

Add mediator:
- Uploads mediator identity transactions to potentially changing set of synchronizer owners. Retries until the
  identity transactions are authorized on the synchronizer.
- Starts and adds a previously uninitialized mediator to group 0

Remove mediator:
- Selects a previously started mediator
- Waits until a mediator has been active for a minimum duration
- Stops and removes the mediator from group 0

Modify threshold:
- Checks if the threshold can be modified
- Alters mediator group 0 threshold by incrementing or decrementing it

Possible extensions:
- setup with multiple sequencers and sequencer connections
- setup with multiple mediator groups

Limitations:
- depends on a fixed sequencer1 to read topology transactions authorized on the synchronizer da
 */
class MediatorGroupChaos(
    reservedMediators: Set[String], // names of mediators not to remove
    val logger: TracedLogger,
) extends TopologyOperations
    with Matchers {
  override def name: String = "MediatorGroupChaos"

  private val mediatorRemoveCandidates = new AtomicReference[List[LocalMediatorReference]](Nil)

  private val mediatorAddCandidates = new AtomicReference[List[LocalMediatorReference]](Nil)

  import TopologyOperations.*

  override def companion: TopologyOperationsCompanion = MediatorGroupChaos

  override def initialization()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Unit = {
    mediatorRemoveCandidates.set(
      env.mediators.local
        .filter(mediator => mediator.is_running && !reservedMediators.contains(mediator.name))
        .toList
    )
    val sm = mediatorRemoveCandidates.get()
    mediatorAddCandidates.set(
      env.mediators.local
        .filter(mediator => !sm.contains(mediator) && !reservedMediators.contains(mediator.name))
        .toList
    )
  }

  override def runTopologyChanges()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Future[Unit] = {
    import env.*

    val operations = Seq(
      () => addMediator(sequencer1),
      () => removeMediator(sequencer1),
      () => modifyMediatorGroupThreshold(sequencer1),
    ).map(topologyChange => Future(topologyChange()))

    Future.sequence(operations).map(_ => ())
  }

  private def addMediator(
      sequencerToConnectTo: SequencerReference,
      group: NonNegativeInt = NonNegativeInt.zero,
  )(implicit loggingContext: ErrorLoggingContext, env: TestConsoleEnvironment): Unit = {
    mediatorAddCandidates.getAndUpdate {
      case Nil => Nil
      case _ :: tail => tail
    }.headOption match {
      case None =>
        logOperationStep("add mediator")(s"No mediator to add to ${group.unwrap} - skipping")
      case Some(mediator) =>
        withOperation_("add mediator")(s"${mediator.name} to group $group") {
          import env.*

          mediator.start()
          mediator.health.wait_for_ready_for_initialization()
          val mediatorIdentity = mediator.topology.transactions.identity_transactions()
          val mediatorId = mediator.id

          def identityTransactionsVisibleOnNode(
              node: InstanceReference,
              proposals: Boolean,
          ): Boolean = {
            val identityTransactionsOnOtherNode = node.topology.transactions
              .list(
                store = daId,
                proposals = proposals,
                operation = Some(TopologyChangeOp.Replace),
                excludeMappings = TopologyMapping.Code.all.diff(
                  Seq(NamespaceDelegation.code, OwnerToKeyMapping.code)
                ),
                filterAuthorizedKey = Some(mediatorId.fingerprint),
              )
              .result
              .map(_.transaction)
            mediatorIdentity.forall(identityTransactionsOnOtherNode.contains(_))
          }

          eventually(overallTopologyChangeTimeout) {
            val owner = getSynchronizerOwners(daId, sequencerToConnectTo).head
            catchTopologyExceptionsAndLogAsInfo(
              s"add mediator",
              s"load ${mediator.name} identity transactions to owner ${owner.name}",
              fail(_), // trigger fail so the surrounding "eventually" retries
            )(
              // Load transactions if they are not already present on the owner as a proposal or authorized
              if (
                !identityTransactionsVisibleOnNode(owner, proposals = false) &&
                !identityTransactionsVisibleOnNode(owner, proposals = true)
              ) {
                owner.topology.transactions
                  .load(mediatorIdentity, store = daId, ForceFlag.AlienMember)
              }
            )

            eventually(topologyChangeTimeout.asFiniteApproximation) {
              val mediatorIdentityAuthorizedOnSynchronizer =
                identityTransactionsVisibleOnNode(sequencerToConnectTo, proposals = false)
              mediatorIdentityAuthorizedOnSynchronizer shouldBe true
            }
          }

          logger.debug(
            fullMessage(
              "add mediator",
              s"mediator ${mediator.name} identity transactions are authorized on synchronizer",
            )
          )

          clue("add mediator")(s"${mediator.name} topology proposal")(
            eventually(overallTopologyChangeTimeout) {
              val currentMediators = listMediators(sequencerToConnectTo, group).item

              if (!currentMediators.active.contains(mediator.id)) {
                // Read the synchronizer owners afresh to improve the chances of the proposal going through in
                // case the owners have since changed.
                val owners = getSynchronizerOwners(daId, sequencerToConnectTo).forgetNE.toSeq
                val needMultiOwnerAuthorization = owners.sizeCompare(1) > 0
                parallelTopologyChangeTimeout.await_(
                  s"add mediator ${mediator.name} to group ${group.unwrap} proposals"
                )(
                  owners
                    .parTraverse(owner =>
                      Future(
                        catchTopologyExceptionsAndLogAsInfo(
                          "add mediator",
                          s"${mediator.name} to group ${group.unwrap} propose_delta on owner ${owner.name}",
                          fail(_), // trigger fail so the surrounding "eventually" retries
                        )(
                          owner.topology.mediators.propose_delta(
                            daId,
                            group = group,
                            adds = List(mediator.id),
                            mustFullyAuthorize = !needMultiOwnerAuthorization,
                            synchronize = Some(topologyChangeTimeout),
                          )
                        )
                      )
                    )
                )

                listMediators(sequencerToConnectTo, group).item.active.forgetNE should contain(
                  mediator.id
                )
              }
            }
          )

          clue("add mediator")(s"${mediator.name} starts catching up") {
            // Note that if the mediator is removed from topology between the previous step in the test and this step,
            // the mediator replica manager terminates the jvm with a fatal exception as it's not able to become active
            // when the sequencer client challenge authenticate token check is rejected because the sequencer does not
            // consider the new mediator an authorized client/member.
            // The test avoids this situation for now by not issuing add-mediator chaos in parallel.
            mediator.setup.assign(
              daId,
              SequencerConnections.single(sequencerToConnectTo),
            )
            mediatorRemoveCandidates.updateAndGet(_ :+ mediator).discard
          }
        }
    }
  }

  private def removeMediator(
      sequencer: SequencerReference,
      group: NonNegativeInt = NonNegativeInt.zero,
      minimumActiveDuration: NonNegativeFiniteDuration = NonNegativeFiniteDuration.tryOfSeconds(2L),
  )(implicit loggingContext: ErrorLoggingContext, env: TestConsoleEnvironment): Unit =
    // A bit of trickery here to avoid removing the last mediator - at least for now
    mediatorRemoveCandidates.getAndUpdate(current =>
      if (current.sizeCompare(1) > 0) current.tail else current
    ) match {
      case singleton @ (Nil | (_ :: Nil)) =>
        logOperationStep("remove mediator")(
          s"Not removing last mediator ${singleton.headOption.fold("")(_.name)} from ${group.unwrap} - skipping"
        )
      case mediator :: _ :: _ =>
        withOperation_("remove mediator")(s"${mediator.name} from group $group") {
          import env.*

          val mediatorId = clue("remove mediator")(
            s"wait until ready to remove mediator ${mediator.name} from group ${group.unwrap} with minimum active duration $minimumActiveDuration"
          )(
            eventually() {
              mediator.is_initialized shouldBe true
              val mediatorResult = listMediators(sequencer, group)
              val mediatorId = mediator.id
              mediatorResult.item.active.forgetNE should contain(mediatorId)
              CantonTimestamp.assertFromInstant(
                mediatorResult.context.validFrom
              ) should be < environment.clock.now - minimumActiveDuration
              mediatorId
            }
          )

          // Stop the mediator to suppress noise arising from time-proof retries sent by the mediator
          clue("remove mediator")(s"stopping mediator ${mediator.name}")(mediator.stop())

          clue("remove mediator")(s"${mediator.name} from group ${group.unwrap} proposal")(
            eventually(overallTopologyChangeTimeout) {
              val currentMediators = listMediators(sequencer, group).item

              if (currentMediators.active.contains(mediatorId)) {
                currentMediators.active.size should be >= 2
                val owners = getSynchronizerOwners(daId, sequencer).forgetNE.toSeq
                val needMultiOwnerAuthorization = owners.sizeCompare(1) > 0
                val remainingMediators = currentMediators.active.forgetNE.filterNot(_ == mediatorId)
                val newThreshold =
                  if (currentMediators.threshold.unwrap.compare(remainingMediators.size) > 0)
                    PositiveInt.tryCreate(remainingMediators.size)
                  else
                    currentMediators.threshold
                parallelTopologyChangeTimeout.await_(
                  s"remove mediator ${mediator.name} from group ${group.unwrap} proposal"
                )(
                  owners
                    .parTraverse(owner =>
                      Future(
                        catchTopologyExceptionsAndLogAsInfo(
                          "remove mediator",
                          s"${mediator.name} from group ${group.unwrap} propose_delta on owner ${owner.name}",
                          triggerExceptionRetryByEventually,
                        )(
                          owner.topology.mediators.propose_delta(
                            daId,
                            group = group,
                            removes = List(mediatorId),
                            updateThreshold = Some(newThreshold),
                            mustFullyAuthorize = !needMultiOwnerAuthorization,
                            synchronize = Some(topologyChangeTimeout),
                          )
                        )
                      )
                    )
                )

                listMediators(
                  sequencer,
                  group,
                ).item.active.forgetNE should not contain mediatorId
              }
            }
          )
        }
    }

  private def modifyMediatorGroupThreshold(
      sequencer: SequencerReference,
      group: NonNegativeInt = NonNegativeInt.zero,
  )(implicit loggingContext: ErrorLoggingContext, env: TestConsoleEnvironment): Unit = {
    import env.*

    withOperation_(s"modify mediator group threshold")(s"${group.unwrap}")(
      eventually(overallTopologyChangeTimeout) {
        val currentMediators = listMediators(sequencer, group).item

        // Make this a no-op when the threshold cannot be changed, otherwise move the threshold
        // "to the middle" between 1 and the number of active mediators.
        val currentThreshold = currentMediators.threshold.unwrap
        val desiredThreshold = (currentMediators.active.size + 1) / 2
        val maybeNewThresholdAndOperation =
          currentMediators.active.size match {
            case active if active > 1 && currentThreshold.compare(desiredThreshold) > 0 =>
              Some(("decrement", PositiveInt.tryCreate(currentThreshold - 1)))
            case moreThanOne if moreThanOne > 1 =>
              Some(("increment", PositiveInt.tryCreate(currentThreshold + 1)))
            case _ => None
          }

        maybeNewThresholdAndOperation.fold {
          logOperationStep(s"modify mediator group threshold")(
            s"Skipping: active=${currentMediators.active.size} threshold=${currentMediators.threshold} group=${group.unwrap}"
          )
        } { case (operation, newThreshold) =>
          val owners = getSynchronizerOwners(daId, sequencer).forgetNE.toSeq
          val needMultiOwnerAuthorization = owners.sizeCompare(1) > 0
          parallelTopologyChangeTimeout.await_(
            s"$operation mediator group ${group.unwrap} threshold proposal"
          )(
            owners
              .parTraverse(owner =>
                Future(
                  catchTopologyExceptionsAndLogAsInfo(
                    s"$operation mediator group threshold",
                    s"$operation mediator group ${group.unwrap} threshold propose_delta on owner ${owner.name}",
                    fail(_), // trigger fail so the surrounding "eventually" retries
                  )(
                    owner.topology.mediators.propose_delta(
                      daId,
                      group = group,
                      updateThreshold = Some(newThreshold),
                      mustFullyAuthorize = !needMultiOwnerAuthorization,
                      synchronize = Some(topologyChangeTimeout),
                    )
                  )
                )
              )
          )

          listMediators(sequencer, group).item.threshold shouldBe newThreshold
        }
      }
    )
  }

  private def listMediators(
      synchronizerMember: InstanceReference,
      group: NonNegativeInt,
  )(implicit env: TestConsoleEnvironment): ListMediatorSynchronizerStateResult = {
    import env.*

    synchronizerMember.topology.mediators
      .list(
        synchronizerId = daId,
        timeQuery = TimeQuery.Snapshot(environment.clock.now),
        group = Some(group),
      )
      .loneElement(s"list mediators for group $group")
  }

  private def triggerExceptionRetryByEventually(msg: String): Unit = fail(msg)

  // timeout waiting for topology transactions issued in parallel (to multiple synchronizer owners)
  // is 2x to be larger than individual topology transaction timeout
  private lazy val parallelTopologyChangeTimeout = topologyChangeTimeout * 2
  // overall timeout for list, propose, list operations is 3x the individual timeout
  private lazy val overallTopologyChangeTimeout = (topologyChangeTimeout * 3).asFiniteApproximation
}

object MediatorGroupChaos extends TopologyOperationsCompanion {
  override lazy val acceptableLogEntries: Seq[String] = Seq(
    // After mediator removal, other mediators still include removed mediator in mediator group recipients due to
    // effective time in phase 6, and sequencer rejects requests with SEQUENCER_SENDER_UNKNOWN, the participants issue LOCAL_VERDICT_TIMEOUT:
    "(Eligible) Senders are unknown:",
    "RequestInvalid(Unregistered recipients: Set(), unregistered senders:", // Additional eligible senders error added with #25270
    "LOCAL_VERDICT_TIMEOUT",
    "ALREADY_EXISTS/TOPOLOGY_MAPPING_ALREADY_EXISTS", // retry loop around proposals can cause duplicate MDS topology mappings to be proposed if topology transaction persistence is slow
    "Topology transaction is not properly authorized: No delegation found for keys", // This warning and the following error happen when a mediator change proposal ...
    "GrpcClientError: INVALID_ARGUMENT/An error occurred. Please contact the operator and inquire about the request", // ... is submitted to a recently removed synchronizer owner
    "Sequencing result message timed out.", // mediator times out phase 6 response if another mediator has since been removed and sequencer considers mediator aggregation rule invalid
  )
}
