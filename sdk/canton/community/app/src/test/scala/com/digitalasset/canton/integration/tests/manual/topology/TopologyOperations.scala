// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import com.digitalasset.canton.config
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.tests.manual.topology.TopologyOperations.TransactionProgress
import com.digitalasset.canton.integration.{ConfigTransform, TestConsoleEnvironment}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology.PartyId

import scala.concurrent.Future

/*
  Each of the attributes `exclusiveParticipants`, `exclusiveSequencers` and `exclusiveMediators`
  allows an operation to indicate that it wants *exclusive* usage of a node. Note that this is
  not meant for operations to indicate which nodes they are using but rather the nodes that no
  other operation should be using.
 */
final case class Reservations(
    exclusiveParticipants: Seq[String] = Nil,
    exclusiveSequencers: Seq[String] = Nil,
    exclusiveMediators: Seq[String] = Nil,
    exclusiveParties: Seq[String] = Nil,
) {
  def +(other: Reservations): Reservations =
    Reservations(
      (exclusiveParticipants ++ other.exclusiveParticipants).distinct,
      (exclusiveSequencers ++ other.exclusiveSequencers).distinct,
      (exclusiveMediators ++ other.exclusiveMediators).distinct,
      (exclusiveParties ++ other.exclusiveParties).distinct,
    )
}

/** Allow to model one operation that is performed as part of the chaos testing. Most of the methods
  * don't need to be overridden.
  *
  * If the operation needs exclusive access to some nodes, it must override exclusiveParticipants,
  * exclusiveMediators or exclusiveSequencers (see more docs below).
  */
trait TopologyOperations extends TopologyChaosLogging with TopologyOperationsHelpers {
  def name: String

  def companion: TopologyOperationsCompanion

  def additionalConfigTransforms: Seq[ConfigTransform] = Nil

  // Additional setup to run as part of the `withSetup` phase
  def additionalSetupPhase()(implicit env: TestConsoleEnvironment): Unit = ()

  // Initialization step, run before the test
  def initialization()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Unit = ()

  /** @param transactionProgress
    *   Result of the progress checker:
    *   - the key is the name of the runner and the timestamp
    *   - value is the sum of acceptances and proposals
    */
  def finalAssertions(transactionProgress: TransactionProgress)(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Unit = ()

  def reservations: Reservations = Reservations()

  def runTopologyChanges()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Future[Unit]

  def canUseParty(party: PartyId)(implicit globalReservations: Reservations): Boolean =
    reservations.exclusiveParties.exists(party.identifier.unwrap.startsWith(_)) ||
      !globalReservations.exclusiveParties.exists(party.identifier.unwrap.startsWith(_))
}

trait TopologyOperationsCompanion {
  def acceptableLogEntries: Seq[String] = Seq()
  def acceptableNonRetryableLogEntries: Seq[String] = Seq()
}

object TopologyOperations {
  lazy val isCi: Boolean = sys.env.isDefinedAt("CI")

  /** @param progress
    *   Result of the progress checker:
    *   - the key is the name of the runner and the timestamp
    *   - value is the sum of acceptances and proposals
    */
  final case class TransactionProgress(
      progress: Map[(String, CantonTimestamp), Int]
  )

  // How long to wait to apply a single topology change and also how long to wait for the change to be applied.
  // E.g. to apply and wait for a topology change to be applied, use topologyChangeTimeout * 2.
  lazy val topologyChangeTimeout: config.NonNegativeDuration =
    DefaultProcessingTimeouts.testing.default

  implicit class RichIterable[T](it: Iterable[T]) {

    /** Returns the only element of `it` if `it` has size 1, throws otherwise.
      * @param hint
      *   Context for the exception.
      * @return
      *   The element.
      */
    def loneElement(hint: String): T =
      it.headOption match {
        case Some(head) =>
          if (it.sizeCompare(1) == 0)
            head
          else
            throw new RuntimeException(
              s"Expecting a single element in collection but found ${it.size} elements. Hint: $hint"
            )

        case None =>
          throw new RuntimeException(
            s"Expecting a single element in collection but it is empty. Hint: $hint"
          )
      }
  }
}
