// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.util.PekkoUtil
import io.grpc.StatusRuntimeException
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.pekko.stream.{KillSwitches, Materializer}
import org.apache.pekko.{Done, NotUsed}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait LedgerSubscription extends FlagCloseableAsync with NamedLogging {
  val completed: Future[Done]
}

object LedgerSubscription {

  def makeSubscription[S, T](
      source: Source[S, NotUsed],
      consumingFlow: Flow[S, T, ?],
      subscriptionName: String,
      processingTimeouts: ProcessingTimeout,
      namedLoggerFactory: NamedLoggerFactory,
  )(implicit materializer: Materializer, executionContext: ExecutionContext): LedgerSubscription =
    new LedgerSubscription {
      override protected def timeouts: ProcessingTimeout = processingTimeouts

      import com.digitalasset.canton.tracing.TraceContext.Implicits.Empty.*

      val (killSwitch, completed) = PekkoUtil.runSupervised(
        logger.error("Fatally failed to handle transaction", _),
        source
          // we place the kill switch before the map operator, such that
          // we can shut down the operator quickly and signal upstream to cancel further sending
          .viaMat(KillSwitches.single)(Keep.right)
          .viaMat(consumingFlow)(Keep.left)
          // and we get the Future[Done] as completed from the sink so we know when the last message
          // was processed
          .toMat(Sink.ignore)(Keep.both),
      )
      override val loggerFactory: NamedLoggerFactory =
        if (subscriptionName.isEmpty)
          namedLoggerFactory
        else
          namedLoggerFactory.appendUnnamedKey(
            "subscription",
            subscriptionName,
          )

      override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
        import com.digitalasset.canton.tracing.TraceContext.Implicits.Empty.*
        List[AsyncOrSyncCloseable](
          SyncCloseable(s"killSwitch.shutdown $subscriptionName", killSwitch.shutdown()),
          AsyncCloseable(
            s"graph.completed $subscriptionName",
            completed.transform {
              case Success(v) => Success(v)
              case Failure(_: StatusRuntimeException) =>
                // don't fail to close if there was a grpc status runtime exception
                // this can happen (i.e. server not available etc.)
                Success(Done)
              case Failure(ex) => Failure(ex)
            },
            processingTimeouts.shutdownShort,
          ),
        )
      }
    }

}
