// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.data.EitherT
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, blocking}
import scala.util.{Failure, Success, Try}

/** Utility class to make BFT-style operations.
  */
object BftSender {

  /** Returned when the request fails to reach the required threshold
    * @param successes The operators that successfully performed the request, grouped by hash of the result
    * @param failures The operators that failed to perform the request
    * @tparam K type of the value hash
    * @tparam I type of the identity of operators
    * @tparam E error type of the operation performed
    */
  final case class FailedToReachThreshold[K, I, E](
      successes: Map[K, Set[I]],
      failures: Map[I, Either[Throwable, E]],
  )

  /** Make a request to multiple operators and aggregate the responses such as the final result will be successful
    * only if "threshold" responses were identical.
    * As soon as the threshold is reached, this method returns. It will also return with an error as soon as it is guaranteed that
    * it cannot possibly gather sufficiently identical requests to meet the threshold.
    * @param description description of the request
    * @param threshold minimum value of identical results that need to be received for the request to be successful (inclusive)
    * @param operators operators to use for the request. The request will be performed via every operator.
    * @param performRequest request to be performed.
    * @param resultHashKey function to provide a hash from a result. This is what determine whether 2 responses are identical.
    * @tparam I key of the operator, typically and ID
    * @tparam E Error type of performRequest
    * @tparam O operator type: object with which the performRequest function will be called
    * @tparam A type of the result
    * @tparam K type of the result hash
    * @return The result of performRequest if sufficiently many responses were identical from the operators.
    */
  def makeRequest[I, E, O, A, K](
      description: String,
      futureSupervisor: FutureSupervisor,
      logger: TracedLogger,
      operators: Map[I, O],
      threshold: PositiveInt,
      performRequest: O => EitherT[FutureUnlessShutdown, E, A],
      resultHashKey: A => K,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, FailedToReachThreshold[K, I, E], A] = {
    implicit val elc: ErrorLoggingContext = ErrorLoggingContext.fromTracedLogger(logger)
    // Keeps track of successful responses in a hashmap so we can easily count how many identical ones we get
    val successfulResults = TrieMap.empty[K, Set[I]]
    // Separately keep track of the total number of responses received for fast comparison
    val responsesCount = new AtomicInteger(0)
    // We don't technically need the failures, but keep them around so we can log and return them if we never reach the threshold
    val failedResults = TrieMap.empty[I, Either[Throwable, E]]
    // Promise that provide the result for this method
    val promise = new PromiseUnlessShutdown[Either[FailedToReachThreshold[K, I, E], A]](
      description,
      futureSupervisor,
    )
    // Provides an object on which to synchronize to avoid concurrency issues when checking results
    val lock = new Object

    def addResult(operatorId: I, result: Try[UnlessShutdown[Either[E, A]]]): Unit = blocking {
      lock.synchronized {
        if (promise.isCompleted) {
          logger.debug(
            s"Ignoring response $result from $operatorId for $description since the threshold has already been reached or the request has failed."
          )
        } else {
          val responsesReceived = responsesCount.incrementAndGet()

          // If there's not enough missing responses left to get to the threshold, we can fail immediately
          def checkIfStillPossibleToReachThreshold(): Unit = {
            val missingResponses = operators.size - responsesReceived
            val bestChanceOfReachingThreshold =
              successfulResults.values.map(_.size).foldLeft(0)(_.max(_))
            if (bestChanceOfReachingThreshold + missingResponses < threshold.value) {
              logger.info(
                s"Cannot reach threshold for $description. Threshold = ${threshold.value}, failed results: $failedResults, successful results: $successfulResults"
              )
              promise.outcome(
                Left(FailedToReachThreshold[K, I, E](successfulResults.toMap, failedResults.toMap))
              )
            }
          }

          // Checks that the operator has not provided a result yet (successful or not)
          ErrorUtil.requireState(
            !(successfulResults.values.toList.flatten ++ failedResults.keySet).contains(operatorId),
            s"Operator $operatorId has already provided a result. Please report this as a bug",
          )

          result match {
            case Success(Outcome(Right(value))) =>
              val updated = successfulResults.updateWith(resultHashKey(value)) {
                case Some(operators) => Some(operators ++ Set(operatorId))
                case None => Some(Set(operatorId))
              }
              // If we've reached the threshold we can stop
              if (updated.map(_.size).getOrElse(0) >= threshold.value) promise.outcome(Right(value))
            case Success(Outcome(Left(error))) =>
              failedResults.put(operatorId, Right(error)).discard
            case Failure(ex) =>
              failedResults.put(operatorId, Left(ex)).discard
            case Success(AbortedDueToShutdown) =>
              promise.shutdown()
          }

          if (!promise.isCompleted) checkIfStillPossibleToReachThreshold()
        }
      }
    }

    operators.foreach { case (operatorId, operator) =>
      FutureUtil.doNotAwaitUnlessShutdown(
        performRequest(operator).value
          .thereafter(addResult(operatorId, _))
          .map(_ => ()),
        s"$description failed for $operatorId",
      )
    }

    EitherT(promise.futureUS)
  }
}
