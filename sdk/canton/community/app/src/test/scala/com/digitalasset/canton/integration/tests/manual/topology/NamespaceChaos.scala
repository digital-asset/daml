// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import cats.syntax.parallel.*
import com.digitalasset.canton.BaseTest.eventually
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{
  LocalInstanceReference,
  LocalSequencerReference,
  SequencerReference,
}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology.transaction.DecentralizedNamespaceDefinition
import com.digitalasset.canton.util.FutureInstances.*
import org.scalatest.EitherValues.convertEitherToValuable
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future
import scala.util.Random

/** Provide helpers used by chaos implementations that are based on namespaces. Allows adding and
  * removing namespace owners and modifying the namespace owner threshold.
  */
private[topology] trait NamespaceChaos extends TopologyOperations with Matchers {

  override def companion: TopologyOperationsCompanion = NamespaceChaos

  /** @return
    *   the number of chaos operations can be run at once. The concurrency should be low for AllOps
    *   tests and can be higher for individual tests.
    */
  def operationConcurrency: Int

  /** @return
    *   the string defining the namespace's purpose. Used for logging.
    */
  def namespacePurpose: String

  protected val allNodes = new AtomicReference[List[LocalInstanceReference]](Nil)
  protected val currentOwnerNodes = new AtomicReference[List[LocalInstanceReference]](Nil)
  protected val potentialOwners = new AtomicReference[List[LocalInstanceReference]](Nil)

  import TopologyOperations.*

  def generateOperations(n: Int, sequencer: LocalSequencerReference)(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
  ): List[() => Unit] =
    List
      .fill(n)(
        Random
          .shuffle(
            List(
              () => addOwner(sequencer),
              () => removeOwner(sequencer),
              () => modifySynchronizerOwnersThreshold(sequencer),
            )
          )
          .headOption
      )
      .flatten

  override def runTopologyChanges()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Future[Unit] = {
    import env.*

    val operations = generateOperations(operationConcurrency, sequencer1).map(topologyChange =>
      Future(topologyChange())
    )

    Future.sequence(operations).map(_ => ())
  }

  private def addOwner(
      sequencerToConnectTo: LocalSequencerReference
  )(implicit loggingContext: ErrorLoggingContext, env: TestConsoleEnvironment): Unit =
    potentialOwners.getAndUpdate {
      case Nil => Nil
      case _ :: tail => tail
    }.headOption match {
      case None =>
        logOperationStep(s"add $namespacePurpose owner")(
          s"no potential owners to add to $namespacePurpose - skipping"
        )
      case Some(ownerToAdd) =>
        withOperation_(s"add $namespacePurpose owner")(
          s"${ownerToAdd.name} (${ownerToAdd.namespace}) starting"
        ) {
          import env.*

          val (serial, initialNamespace) = getNamespaceDefinition(sequencerToConnectTo)
          val ownerNodesUpdated = getOwnerNodes(initialNamespace).appended(ownerToAdd)
          val modifiedOwners =
            com.daml.nonempty.NonEmpty(
              Set,
              ownerToAdd.namespace,
              initialNamespace.owners.toList*
            )
          val updatedNamespace = DecentralizedNamespaceDefinition
            .create(
              decentralizedNamespace = initialNamespace.namespace,
              threshold = initialNamespace.threshold,
              owners = modifiedOwners,
            )
            .value

          clue(s"add $namespacePurpose owner")(s"${ownerToAdd.name} authorizing") {
            topologyChangeTimeout.await_(
              s"add $namespacePurpose owner: ${ownerToAdd.name} proposals"
            )(
              ownerNodesUpdated
                .appended(ownerToAdd)
                .parTraverse(existingOwner =>
                  Future(
                    catchTopologyExceptionsAndLogAsInfo(
                      s"add $namespacePurpose owner",
                      s"${ownerToAdd.name} exception in authorization",
                      _ => (),
                    )(
                      existingOwner.topology.decentralized_namespaces
                        .propose(
                          decentralizedNamespace = updatedNamespace,
                          store = sequencerToConnectTo.synchronizer_id,
                          signedBy = Seq(existingOwner.fingerprint),
                          serial = Some(serial.increment),
                        )
                    )
                  )
                )
            )
          }
          clue(s"add $namespacePurpose owner")(s"${ownerToAdd.name} verifying")(
            eventually(
              topologyChangeTimeout.asFiniteApproximation,
              retryOnTestFailuresOnly = false,
            ) {
              expectSerialChanged(serial, sequencerToConnectTo, ownerNodesUpdated)
              val (newSerial, newDefinition) = getNamespaceDefinition(sequencerToConnectTo)
              (newSerial, newDefinition) should not equal (serial, initialNamespace)
              if (newDefinition.owners.contains(ownerToAdd.namespace)) {
                logOperationStep(s"add $namespacePurpose owner")(
                  s"${ownerToAdd.name} successfully added ${printNamespaceDef(newDefinition)}"
                )
                // We must update it atomically because of interleaving with other asynch operations
                currentOwnerNodes.updateAndGet(_.appended(ownerToAdd))
              } else {
                logOperationStep(s"add $namespacePurpose owner")(
                  s"${ownerToAdd.name} not added, because of a concurrent change"
                )
                // Failed addition means the owner returns to the pool
                potentialOwners.updateAndGet(_.appended(ownerToAdd))
              }
            }
          )

        }
    }

  private def removeOwner(
      sequencerToConnectTo: LocalSequencerReference
  )(implicit loggingContext: ErrorLoggingContext, env: TestConsoleEnvironment): Unit =
    currentOwnerNodes.getAndUpdate {
      case _ :: neck :: tail => neck :: tail
      case other => other // last owner -> do not remove it
    } match {
      case Nil =>
        logOperationStep(s"remove $namespacePurpose owner")(
          s"empty list of $namespacePurpose owners - skipping"
        )
      case _ :: Nil =>
        logOperationStep(s"remove $namespacePurpose owner")(
          s"last $namespacePurpose owner - skipping"
        )
      case ownerToRemove :: _ =>
        withOperation_(s"remove $namespacePurpose owner")(
          s"${ownerToRemove.name} (${ownerToRemove.namespace}) starting"
        ) {
          import env.*

          val (serial, initialNamespace) = getNamespaceDefinition(sequencerToConnectTo)
          val ownerNodes = getOwnerNodes(initialNamespace)
          val modifiedOwners =
            initialNamespace.owners.filterNot(_ == ownerToRemove.namespace).toList
          val updatedNamespace = DecentralizedNamespaceDefinition
            .create(
              decentralizedNamespace = initialNamespace.namespace,
              threshold = Math.min(initialNamespace.threshold.value, modifiedOwners.size),
              owners = com.daml.nonempty.NonEmpty(Set, modifiedOwners.head, modifiedOwners.tail*),
            )
            .value

          clue(s"remove $namespacePurpose owner")(s"${ownerToRemove.name} authorizing") {
            topologyChangeTimeout.await_(
              s"remove $namespacePurpose owner: ${ownerToRemove.name} proposals"
            )(
              ownerNodes
                .parTraverse(existingOwner =>
                  Future(
                    catchTopologyExceptionsAndLogAsInfo(
                      s"remove $namespacePurpose owner",
                      s"${ownerToRemove.name} exception in authorization",
                      _ => (),
                    )(
                      existingOwner.topology.decentralized_namespaces
                        .propose(
                          decentralizedNamespace = updatedNamespace,
                          store = sequencerToConnectTo.synchronizer_id,
                          signedBy = Seq(existingOwner.fingerprint),
                          serial = Some(serial.increment),
                        )
                    )
                  )
                )
            )
          }
          clue(s"remove $namespacePurpose owner")(s"${ownerToRemove.name} verifying")(
            eventually(
              topologyChangeTimeout.asFiniteApproximation,
              retryOnTestFailuresOnly = false,
            ) {
              expectSerialChanged(serial, sequencerToConnectTo, ownerNodes)
              val (newSerial, newDefinition) = getNamespaceDefinition(sequencerToConnectTo)
              (newSerial, newDefinition) should not equal (serial, initialNamespace)
              if (newDefinition.owners.contains(ownerToRemove.namespace)) {
                logOperationStep(s"remove $namespacePurpose owner")(
                  s"${ownerToRemove.name} not removed, because of a concurrent change"
                )
                // Failed removal means the owner is still active
                currentOwnerNodes.updateAndGet(_.appended(ownerToRemove))
              } else {
                logOperationStep(s"remove $namespacePurpose owner")(
                  s"${ownerToRemove.name} successfully removed ${printNamespaceDef(newDefinition)}"
                )
                // Owner goes back to the pool
                potentialOwners.updateAndGet(_.appended(ownerToRemove))
              }
            }
          )

        }
    }

  private def modifySynchronizerOwnersThreshold(
      sequencerToConnectTo: LocalSequencerReference
  )(implicit loggingContext: ErrorLoggingContext, env: TestConsoleEnvironment): Unit = {
    val (serial, initialNamespace) = getNamespaceDefinition(sequencerToConnectTo)
    val ownerNodes = getOwnerNodes(initialNamespace)
    val modifiedThreshold = initialNamespace.threshold.value % initialNamespace.owners.size + 1
    if (initialNamespace.owners.sizeIs <= 1)
      logOperationStep(s"modify $namespacePurpose owner threshold")(
        s"last $namespacePurpose owner - skipping"
      )
    else
      withOperation_(s"modify $namespacePurpose owner threshold")(
        s"threshold=$modifiedThreshold starting"
      ) {
        import env.*

        val updatedNamespace = DecentralizedNamespaceDefinition
          .create(
            decentralizedNamespace = initialNamespace.namespace,
            threshold = modifiedThreshold,
            owners = initialNamespace.owners,
          )
          .value

        clue(s"modify $namespacePurpose owner threshold")(
          s"threshold=$modifiedThreshold authorizing"
        )(
          topologyChangeTimeout.await_(
            s"modify $namespacePurpose owner threshold: threshold=$modifiedThreshold proposals"
          )(
            ownerNodes
              .parTraverse(existingOwner =>
                Future(
                  catchTopologyExceptionsAndLogAsInfo(
                    s"modify $namespacePurpose owner threshold",
                    s"threshold=$modifiedThreshold exception in authorization",
                    _ => (),
                  )(
                    existingOwner.topology.decentralized_namespaces
                      .propose(
                        decentralizedNamespace = updatedNamespace,
                        store = sequencerToConnectTo.synchronizer_id,
                        signedBy = Seq(existingOwner.fingerprint),
                        serial = Some(serial.increment),
                      )
                  )
                )
              )
          )
        )
        clue(s"modify $namespacePurpose owner threshold")(
          s"threshold=$modifiedThreshold verifying"
        )(
          eventually(
            topologyChangeTimeout.asFiniteApproximation,
            retryOnTestFailuresOnly = false,
          ) {
            expectSerialChanged(serial, sequencerToConnectTo, ownerNodes)
            val (newSerial, newDefinition) = getNamespaceDefinition(sequencerToConnectTo)
            (newSerial, newDefinition) should not equal (serial, initialNamespace)
            if (newDefinition.threshold == initialNamespace.threshold) {
              logOperationStep(s"modify $namespacePurpose owner threshold")(
                s"threshold=$modifiedThreshold not modified, because of a concurrent change"
              )
            } else {
              logOperationStep(s"modify $namespacePurpose owner threshold")(
                s"threshold=$modifiedThreshold successfully modified ${printNamespaceDef(newDefinition)}"
              )
            }
          }
        )

      }
  }

  private def expectSerialChanged(
      serial: PositiveInt,
      sequencerToConnectTo: SequencerReference,
      nodes: List[LocalInstanceReference],
  ): Unit =
    nodes.foreach { o =>
      val newSerial = getNamespaceDefinition(sequencerToConnectTo, Some(o))._1
      newSerial should be > serial
    }

  protected def getNamespaceDefinition(
      sequencerToConnectTo: SequencerReference,
      node: Option[LocalInstanceReference] = None,
  ): (PositiveInt, DecentralizedNamespaceDefinition)

  private def getOwnerNodes(
      namespaceDefinition: DecentralizedNamespaceDefinition
  ): List[LocalInstanceReference] =
    allNodes.get().filter(node => namespaceDefinition.owners.contains(node.namespace))

  private def printNamespaceDef(namespaceDefinition: DecentralizedNamespaceDefinition): String =
    s"{${namespaceDefinition.threshold.value},(${getOwnerNodes(namespaceDefinition).map(_.name).mkString(",")})}"
}

object NamespaceChaos extends TopologyOperationsCompanion {}
