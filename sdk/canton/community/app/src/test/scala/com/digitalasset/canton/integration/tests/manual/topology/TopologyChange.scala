// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import cats.syntax.parallel.*
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*

import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.nowarn
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

private[topology] trait TopologyChange
    extends TopologyChaosLogging
    with TopologyOperationsHelpers
    with FlagCloseable
    with HasCloseContext {

  implicit def ec: ExecutionContext

  implicit def env: TestConsoleEnvironment

  /** The name of the specific change, eg. "add sequencer ${sequencertoOnBoard.name}"
    */
  protected def topologyChangeName: String

  /** Used to query topology state and/or upload new transactions */
  def referenceNode: InstanceReference

  /** This method is run before the main retry loop is started. This callback can be used for
    * example to ensure that a node is started or to upload the identity topology transactions for
    * onboarding a node later on.
    */
  def preChange()(implicit @nowarn errorLoggingContext: ErrorLoggingContext): Future[Unit] =
    Future.unit

  /** This is the list of nodes that need to propose a topology change
    */
  def nodesThatAffectChange(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Seq[InstanceReference]

  /** This method implements the main action for affecting a topology change, eg. submitting a
    * proposal to onboard a sequencer.
    *
    * This method is called multiple times in a retry loop.
    *
    * @param node
    *   the node that needs to take an action
    */
  def affectPartialChange(node: InstanceReference)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Future[Unit]

  /** After a node has executed their main action via affectPartialChange, it needs to validate that
    * it can observe the effect of its action, eg. check that it sees the onboarded sequencer either
    * in the latest fully authorized transaction, or that the latest proposal contains the node's
    * signature.
    *
    * This method is called multiple times in a retry loop.
    *
    * @param node
    *   the node to validate that the action has been successfully performed
    * @return
    *   true if the partial change was observed, false otherwise
    */
  def validatePartialChange(node: InstanceReference)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Boolean

  /** After all nodes returned by `nodesThatAffectChange` have performed their individual change, it
    * is time to check whether the expected final effect of the change can be observed. Eg. The
    * sequencerToOnboard is observed in the latest fully authorized SequencerSynchronizerState
    * transaction.
    * @return
    *   true if the full effect has been observed, false otherwise
    */
  def validateFullChange()(implicit errorLoggingContext: ErrorLoggingContext): Boolean

  /** This is a callback that can be useful for running an action after the topology change has been
    * successfully performed. Eg. to initialize a sequencer with an onboarding snapshot after it has
    * been successfully added to the SequencerSynchronizerState transaction.
    */
  def postChange()(implicit @nowarn errorLoggingContext: ErrorLoggingContext): Future[Unit] =
    Future.unit

  /** Performs
    *   1. preChange 2. mainLoop (validatePartialChange, validateFullChange, affectPartialChange) in
    *      retry loops 3. postChange
    */
  final def run()(implicit errorLoggingContext: ErrorLoggingContext): Future[Unit] =
    TraceContext.withNewTraceContext("test") { traceContext =>
      implicit val freshErrorLoggingContext = ErrorLoggingContext(
        errorLoggingContext.logger,
        errorLoggingContext.properties,
        traceContext,
      )
      for {
        _ <- clueF(topologyChangeName)(s"preChange")(preChange()(freshErrorLoggingContext))(
          freshErrorLoggingContext,
          env,
        )
        _ <- clueF(topologyChangeName)("main loop")(mainLoop()(freshErrorLoggingContext))(
          freshErrorLoggingContext,
          env,
        )
        _ <- clueF(topologyChangeName)("postChange")(postChange()(freshErrorLoggingContext))(
          freshErrorLoggingContext,
          env,
        )
      } yield {
        ()
      }
    }

  private def mainLoop()(implicit errorLoggingContext: ErrorLoggingContext) = {
    val iterationCounter = new AtomicInteger(0)
    retryWithClue(topologyChangeName, "main loop", maxRetries = 10, delay = 1.second)(
      if (validateFullChange()) {
        // the intended change is already in effect => there's nothing to do
        Future.successful(true)
      } else {
        nodesThatAffectChange
          .parTraverse(node => loopPerNode(node, iterationCounter.incrementAndGet()))
          .flatMap(partialResults => validatePartialResults(partialResults))
      }
    )
  }

  private def validatePartialResults(partialValidationResults: Seq[Boolean])(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Future[Boolean] =
    if (partialValidationResults.forall(identity)) {
      retryWithClue(
        topologyChangeName,
        s"validating full change",
        maxRetries = 10,
        delay = 1.second,
      )(
        Future.successful(validateFullChange())
      )
    } else Future.successful(false)

  private def loopPerNode(node: InstanceReference, iteration: Int)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Future[Boolean] =
    // Use Future { ... }.flatten to guarantee that the outer traverse isn't held up by any blocking calls
    // while constructing the future
    Future {
      if (
        !clue(topologyChangeName)(s"pre-check for ${node.name} #$iteration")(
          validatePartialChange(node)
        )
      ) {
        for {
          // perform the actual topology change
          changeResult <- retryWithClue(
            topologyChangeName,
            s"${node.name} affects change iteration #$iteration",
            maxRetries = 20,
            delay = 500.millis,
          )(affectPartialChange(node).map(_ => true))

          validationResult <-
            if (!changeResult) {
              // there was an issue performing the topology change, we don't even need to validate it
              Future.successful(false)
            } else {
              //
              retryWithClue(
                topologyChangeName,
                s"${node.name} validating partial change #$iteration",
                maxRetries = 20,
                delay = 500.millis,
              )(
                Future.successful(validatePartialChange(node))
              )
            }
        } yield validationResult
      } else Future.successful(true)
    }.flatten
}
