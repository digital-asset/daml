// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block

import cats.data.EitherT
import com.digitalasset.canton.domain.block.{
  RawLedgerBlock,
  SequencerDriver,
  SequencerDriverHealthStatus,
}
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer.SenderSigned
import com.digitalasset.canton.domain.sequencing.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.ServerServiceDefinition
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

class DriverBlockOrderer(
    driver: SequencerDriver,
    override val orderingTimeFixMode: OrderingTimeFixMode,
)(implicit executionContext: ExecutionContext)
    extends BlockOrderer {

  override def firstBlockHeight: Long = driver.firstBlockHeight

  override def subscribe()(implicit
      traceContext: TraceContext
  ): Source[RawLedgerBlock, KillSwitch] =
    driver.subscribe()

  override def send(
      signedSubmissionRequest: SenderSigned[SubmissionRequest]
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncError, Unit] =
    // The driver API doesn't provide error reporting, so we don't attempt to translate the exception
    EitherT.right(
      driver.send(signedSubmissionRequest.toByteString)
    )

  override def acknowledge(signedAcknowledgeRequest: SenderSigned[AcknowledgeRequest])(implicit
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
}
