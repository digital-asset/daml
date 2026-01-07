// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block

import cats.syntax.functor.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.{SubmissionRequest, SubmissionRequestType}
import com.digitalasset.canton.synchronizer.metrics.{CircuitBreakerMetrics, SequencerMetrics}
import com.digitalasset.canton.synchronizer.sequencer.BlockSequencerConfig.{
  CircuitBreakerConfig,
  IndividualCircuitBreakerConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.block.BlockSequencerCircuitBreaker.IndividualCircuitBreaker
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import org.apache.pekko.actor.Scheduler
import org.apache.pekko.pattern.CircuitBreaker
import org.apache.pekko.stream.Materializer

import java.time
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.*

/** Circuit breaker used for stopping accepting requests when the sequencer is overloaded and
  * falling behind on processing blocks. It works by keeping track of the lastTs on each block
  * process and computing a delay between that timestamp and current time. If the delay is larger
  * than allowedBlockDelay, a failure is registered.
  *
  * Following the circuit breaker logic described in
  * https://doc.akka.io/libraries/akka-core/current/common/circuitbreaker.html, after maxFailures
  * consecutive failures, requests will no longer be accepted (state goes from closed to open).
  *
  * After a resetTimeout amount of time has passed in the open state, the circuit goes to half-open
  * state, where requests are accepted, and if the next block delay registered is in the accepted
  * interval, the circuit breaker closes again and requests are accepted, otherwise, it goes back to
  * being open (and requests not accepted). In this case, the resetTimeout is multiplied by the
  * exponentialBackoffFactor to compute the next timeout (up until at most maxResetTimeout).
  *
  * It is possible to configure separate config breaker parameter per message type.
  */
class BlockSequencerCircuitBreaker(
    config: CircuitBreakerConfig,
    clock: Clock,
    metrics: SequencerMetrics,
    scheduler: Scheduler,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with AutoCloseable {

  private val enabled: AtomicBoolean = new AtomicBoolean(config.enabled)
  private val previousTimestamp: AtomicReference[CantonTimestamp] =
    new AtomicReference(CantonTimestamp.MinValue)

  def registerLastBlockTimestamp(lastTs: Traced[CantonTimestamp]): Unit = {
    val timestamp = lastTs.value
    val blockDelay = clock.now - timestamp
    metrics.block.delay.updateValue(blockDelay.toMillis)

    // Ignore timestamps that are equal to the previous one. That happens when the block is empty.
    // We do that in order to avoid that a series of empty blocks cause the circuit breaker to think that
    // it is falling behind (because the block delay would continue to grow with each empty block).
    if (enabled.get() && timestamp != previousTimestamp.get())
      pekkoCircuitBreakers.values.foreach(_.registerBlockDelay(blockDelay))

    previousTimestamp.set(timestamp)
  }

  def shouldRejectRequests(submissionRequestType: SubmissionRequestType): Boolean = {
    val subTypeKey = submissionRequestType match {
      case SubmissionRequestType.Unexpected(_) =>
        BlockSequencerCircuitBreaker.unexpectedSubmissionRequestTypeKey
      case x => x
    }
    enabled.get() && pekkoCircuitBreakers
      .get(subTypeKey)
      .forall(_.shouldRejectRequests(submissionRequestType.name))
  }

  def shouldRejectRequests(submissionRequest: SubmissionRequest): Boolean =
    shouldRejectRequests(submissionRequest.requestType)

  def shouldRejectAcknowledgements: Boolean =
    enabled.get() && acknowledgmentPekkoCircuitBreaker.shouldRejectRequests("acknowledgment")

  def enable(): Unit = enabled.set(true)
  def disable(): Unit = enabled.set(false)

  private val (
    acknowledgmentPekkoCircuitBreaker: IndividualCircuitBreaker,
    pekkoCircuitBreakers: Map[SubmissionRequestType, IndividualCircuitBreaker],
  ) = {
    val messages = config.messages
    val configToCircuitBreaker: Map[IndividualCircuitBreakerConfig, IndividualCircuitBreaker] = Seq(
      messages.confirmationResponse -> SubmissionRequestType.ConfirmationResponse.name,
      messages.confirmationRequest -> SubmissionRequestType.ConfirmationRequest.name,
      messages.verdict -> SubmissionRequestType.Verdict.name,
      messages.commitment -> SubmissionRequestType.Commitment.name,
      messages.topUp -> SubmissionRequestType.TopUp.name,
      messages.topUp -> SubmissionRequestType.TopUpMed.name,
      messages.topology -> SubmissionRequestType.TopologyTransaction.name,
      messages.timeProof -> SubmissionRequestType.TimeProof.name,
      messages.unexpected -> BlockSequencerCircuitBreaker.unexpectedSubmissionRequestTypeKey.name,
      messages.acknowledgement -> "acknowledgment",
    ).groupBy(_._1).map { case (config, group) =>
      val messageNames = group.map(_._2)
      config -> new IndividualCircuitBreaker(
        config,
        messageNames,
        scheduler,
        metrics,
        loggerFactory,
      )(ec, TraceContext.createNew("sequencer-circuit-breaker"))
    }
    (
      configToCircuitBreaker(messages.acknowledgement),
      Map[SubmissionRequestType, IndividualCircuitBreakerConfig](
        SubmissionRequestType.ConfirmationResponse -> messages.confirmationResponse,
        SubmissionRequestType.ConfirmationRequest -> messages.confirmationRequest,
        SubmissionRequestType.Verdict -> messages.verdict,
        SubmissionRequestType.Commitment -> messages.commitment,
        SubmissionRequestType.TopUp -> messages.topUp,
        SubmissionRequestType.TopUpMed -> messages.topUp,
        SubmissionRequestType.TopologyTransaction -> messages.topology,
        SubmissionRequestType.TimeProof -> messages.timeProof,
        BlockSequencerCircuitBreaker.unexpectedSubmissionRequestTypeKey -> messages.unexpected,
      ).fmap(configToCircuitBreaker(_)),
    )
  }

  override def close(): Unit = pekkoCircuitBreakers.values.foreach(_.close())
}

object BlockSequencerCircuitBreaker {
  private val unexpectedSubmissionRequestTypeKey = SubmissionRequestType.Unexpected("unexpected")

  class IndividualCircuitBreaker(
      config: IndividualCircuitBreakerConfig,
      messageNames: Seq[String],
      scheduler: Scheduler,
      metrics: SequencerMetrics,
      override val loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext, traceContext: TraceContext)
      extends NamedLogging
      with AutoCloseable {

    private val allowedBlockDelay = config.allowedBlockDelay.underlying

    private val metricsPerMessageName = messageNames
      .map(msgName =>
        msgName -> new CircuitBreakerMetrics(
          msgName,
          metrics.prefix,
          metrics.openTelemetryMetricsFactory,
        )
      )
      .toMap

    def registerBlockDelay(blockDelay: time.Duration): Unit =
      if (blockDelay.compareTo(allowedBlockDelay.toJava) > 0) {
        metricsPerMessageName.values.foreach(_.failures.inc())
        pekkoCircuitBreaker.fail()
      } else
        pekkoCircuitBreaker.succeed()

    def shouldRejectRequests(name: String): Boolean = {
      metricsPerMessageName(name).messages.inc()
      val shouldReject = pekkoCircuitBreaker.isOpen
      if (shouldReject) metricsPerMessageName(name).rejections.inc()
      shouldReject
    }

    private val pekkoCircuitBreaker = {
      val maxFailures = config.maxFailures
      val resetTimeout = config.resetTimeout.underlying
      val exponentialBackoffFactor = config.exponentialBackoffFactor
      val maxResetTimeout = config.maxResetTimeout.underlying

      new CircuitBreaker(
        scheduler,
        maxFailures = maxFailures,
        resetTimeout = resetTimeout,
        exponentialBackoffFactor = exponentialBackoffFactor,
        maxResetTimeout = maxResetTimeout,
        // callTimeout is not used, because we are calling fail() explicitly instead of withCircuitBreaker()
        callTimeout = 0.seconds,
      ).onOpen {
        metricsPerMessageName.values.foreach(_.state.updateValue(1d))
        logger.info(
          s"Sequencer not accepting requests momentarily for ${messageNames.mkString(" ,")} messages, after $maxFailures consecutive blocks behind more than $allowedBlockDelay"
        )
      }.onHalfOpen {
        metricsPerMessageName.values.foreach(_.state.updateValue(0.5d))
        logger.debug(
          s"Sequencer temporarily taking requests for ${messageNames.mkString(" ,")} messages while assessing situation"
        )
      }.onClose {
        metricsPerMessageName.values.foreach(_.state.updateValue(0d))
        logger.info(
          s"Sequencer now accepting requests again for ${messageNames.mkString(" ,")} messages, after seeing a block with delay below $allowedBlockDelay"
        )
      }
    }

    override def close(): Unit = metricsPerMessageName.values.foreach(_.close())
  }

  def apply(
      config: CircuitBreakerConfig,
      clock: Clock,
      metrics: SequencerMetrics,
      materializer: Materializer,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext) = new BlockSequencerCircuitBreaker(
    config,
    clock,
    metrics,
    materializer.system.scheduler,
    loggerFactory,
  )
}
