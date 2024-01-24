// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client

import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse.Update
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.error.DecodedRpcStatus
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.{NoTracing, Spanning}
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.digitalasset.canton.util.TryUtil.ForFailedOps
import com.digitalasset.canton.util.retry.RetryUtil.AllExnRetryable
import com.digitalasset.canton.util.{FutureUtil, retry}
import io.grpc.StatusRuntimeException
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

/** Resilient ledger subscriber, which keeps continuously
  * re-subscribing (on failure) to the Ledger API transaction stream
  * and applies the received transactions to the `processTransaction` function.
  *
  * `processTransaction` must not throw. If it does, it must be idempotent
  * (i.e. allow re-processing the same transaction twice).
  */
class ResilientLedgerSubscription[S, T](
    makeSource: ParticipantOffset => Source[S, NotUsed],
    consumingFlow: Flow[S, T, ?],
    subscriptionName: String,
    startOffset: ParticipantOffset,
    extractOffset: S => Option[ParticipantOffset],
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    resubscribeIfPruned: Boolean = false,
)(implicit
    ec: ExecutionContextExecutor,
    materializer: Materializer,
) extends FlagCloseableAsync
    with NamedLogging
    with Spanning
    with NoTracing {
  private val offsetRef = new AtomicReference[ParticipantOffset](startOffset)
  private implicit val policyRetry: retry.Success[Any] = retry.Success.always
  private val ledgerSubscriptionRef = new AtomicReference[Option[LedgerSubscription]](None)
  private[client] val subscriptionF = retry
    .Backoff(
      logger = logger,
      flagCloseable = this,
      maxRetries = retry.Forever,
      initialDelay = 1.second,
      maxDelay = 5.seconds,
      operationName = s"restartable-$subscriptionName",
    )
    .apply(resilientSubscription(), AllExnRetryable)

  runOnShutdown_(new RunOnShutdown {
    override def name: String = s"$subscriptionName-shutdown"

    override def done: Boolean = {
      // Use isClosing to avoid task eviction at the beginning (see runOnShutdown)
      isClosing && ledgerSubscriptionRef.get().forall(_.completed.isCompleted)
    }

    override def run(): Unit =
      ledgerSubscriptionRef.getAndSet(None).foreach(Lifecycle.close(_)(logger))
  })

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    val name = s"wait-for-$subscriptionName-completed"
    Seq(
      AsyncCloseable(
        name,
        subscriptionF.recover { error =>
          logger.warn(s"$name finished with an error", error)
          ()
        },
        timeouts.closing,
      )
    )
  }

  private def resilientSubscription(): Future[Unit] =
    FutureUtil.logOnFailure(
      future = {
        val newSubscription = createLedgerSubscription()
        ledgerSubscriptionRef.set(Some(newSubscription))
        // Check closing again to ensure closing of the new subscription
        // in case the shutdown happened before the after the first closing check
        // but before the previous reference update
        if (isClosing) {
          ledgerSubscriptionRef.getAndSet(None).foreach(closeSubscription)
          newSubscription.completed.map(_ => ())
        } else {
          newSubscription.completed
            .map(_ => ())
            .thereafter { result =>
              // This closing races with the one from runOnShutdown so use getAndSet
              // to ensure calling close only once on a subscription
              ledgerSubscriptionRef.getAndSet(None).foreach(closeSubscription)
              result.forFailed(handlePrunedDataAccessed)
            }
        }
      },
      failureMessage = s"${subject(capitalized = true)} failed with an error",
      level = Level.WARN,
    )

  private def closeSubscription(ledgerSubscription: LedgerSubscription): Unit =
    Try(ledgerSubscription.close()) match {
      case Failure(exception) =>
        logger.warn(
          s"${subject(capitalized = true)} [$ledgerSubscription] failed to close successfully",
          exception,
        )
      case Success(_) =>
        logger.info(
          s"Successfully closed ${subject(capitalized = false)} [$ledgerSubscription] closed successfully"
        )
    }

  private def subject(capitalized: Boolean) =
    s"${if (capitalized) "Ledger" else "ledger"} subscription $subscriptionName"

  private def createLedgerSubscription(): LedgerSubscription = {
    val currentOffset = offsetRef.get()
    logger.debug(
      s"Creating new transactions ${subject(capitalized = false)} starting at offset $currentOffset"
    )
    LedgerSubscription.makeSubscription(
      makeSource(currentOffset),
      Flow[S]
        .map { item =>
          extractOffset(item).foreach(offsetRef.set)
          item
        }
        .via(consumingFlow),
      subscriptionName,
      timeouts,
      loggerFactory,
    )
  }

  private def handlePrunedDataAccessed: Throwable => Unit = {
    case sre: StatusRuntimeException =>
      DecodedRpcStatus
        .fromStatusRuntimeException(sre)
        .filter(_.id == RequestValidationErrors.ParticipantPrunedDataAccessed.id)
        .flatMap(_.context.get(LedgerApiErrors.EarliestOffsetMetadataKey))
        .foreach { earliestOffset =>
          if (resubscribeIfPruned) {
            logger.warn(
              s"Setting the ${subject(capitalized = false)} offset to a later offset [$earliestOffset] due to pruning. Some commands might timeout or events might become stale."
            )
            offsetRef.set(ParticipantOffset(ParticipantOffset.Value.Absolute(earliestOffset)))
          } else {
            logger.error(
              s"Connection ${subject(capitalized = false)} failed to resubscribe from  ${offsetRef
                  .get()}, while earliest offset is [$earliestOffset] due to pruning."
            )
            close()
          }
        }
    case _ =>
      // Do nothing for other errors
      ()
  }
}

object ResilientLedgerSubscription {
  def extractOffsetFromGetUpdateResponse(response: GetUpdatesResponse): Option[ParticipantOffset] =
    (response.update match {
      case Update.Transaction(value) =>
        Some(value.offset)
      case Update.Reassignment(value) =>
        Some(value.offset)
      case Update.Empty => None
    }).map(off => ParticipantOffset(ParticipantOffset.Value.Absolute(off)))

}
