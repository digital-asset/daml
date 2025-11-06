// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.synchronizer.block.{
  RawLedgerBlock,
  SequencerDriver,
  SequencerDriverHealthStatus,
}
import com.digitalasset.canton.synchronizer.sequencer.Sequencer.SenderSigned
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import io.grpc.ServerServiceDefinition
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

import BlockSequencerFactory.OrderingTimeFixMode

class DriverBlockOrderer(
    driver: SequencerDriver,
    override val orderingTimeFixMode: OrderingTimeFixMode,
)(implicit executionContext: ExecutionContext)
    extends BlockOrderer {

  override def firstBlockHeight: Long = driver.firstBlockHeight

  override def subscribe()(implicit
      traceContext: TraceContext
  ): Source[Traced[RawLedgerBlock], KillSwitch] =
    // The trace context being assigned to blocks in this subscription is not correct.
    // A different one should be assigned to each block as part of the driver's implementation.
    // But because drivers will be discontinued soon, there are no plans to properly implement that.
    driver.subscribe().map(Traced(_))

  override def send(
      signedSubmissionRequest: SenderSigned[SubmissionRequest]
  )(implicit traceContext: TraceContext): EitherT[Future, SequencerDeliverError, Unit] = {
    val submissionRequest = signedSubmissionRequest.content
    // The driver API doesn't provide error reporting, so we don't attempt to translate the exception
    EitherT.right(
      driver.send(
        signedOrderingRequest = signedSubmissionRequest.toByteString,
        submissionId = submissionRequest.messageId.toProtoPrimitive,
        senderId = submissionRequest.sender.toProtoPrimitive,
      )
    )
  }

  override def acknowledge(signedAcknowledgeRequest: SignedContent[AcknowledgeRequest])(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    driver.acknowledge(signedAcknowledgeRequest.toByteString)

  override def health(implicit
      traceContext: TraceContext
  ): Future[SequencerDriverHealthStatus] =
    driver.health

  override def close(): Unit =
    driver.close()

  override def adminServices: Seq[ServerServiceDefinition] = driver.adminServices

  override def sequencerSnapshotAdditionalInfo(
      timestamp: CantonTimestamp
  ): EitherT[Future, SequencerError, Option[v30.BftSequencerSnapshotAdditionalInfo]] =
    EitherT.rightT(None)

  override def sequencingTime(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] =
    FutureUnlessShutdown.outcomeF(
      driver.sequencingTime.map(t =>
        t.map(CantonTimestamp.fromProtoPrimitive)
          .map(_.fold(e => throw new RuntimeException(e.message), identity))
      )
    )
}
