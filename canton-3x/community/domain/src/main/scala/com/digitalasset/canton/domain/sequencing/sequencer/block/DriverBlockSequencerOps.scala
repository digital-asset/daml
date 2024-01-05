// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block

import cats.data.EitherT
import com.digitalasset.canton.domain.block.{
  RawLedgerBlock,
  SequencerDriver,
  SequencerDriverHealthStatus,
}
import com.digitalasset.canton.domain.sequencing.sequencer.errors.{
  RegisterMemberError,
  SequencerWriteError,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

class DriverBlockSequencerOps(
    driver: SequencerDriver,
    protocolVersion: ProtocolVersion,
)(implicit executionContext: ExecutionContext)
    extends BlockSequencerOps {

  override def subscribe()(implicit
      traceContext: TraceContext
  ): Source[RawLedgerBlock, KillSwitch] =
    driver.subscribe()

  override def send(
      signedSubmission: SignedContent[SubmissionRequest]
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncError, Unit] =
    // The driver API doesn't provide error reporting, so we don't attempt to translate the exception
    EitherT.right(
      driver.send(signedSubmission.toByteString)
    )

  override def register(member: Member)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerWriteError[RegisterMemberError], Unit] =
    // The driver API doesn't provide error reporting, so we don't attempt to translate the exception
    EitherT.right(driver.registerMember(member.toProtoPrimitive))

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
}
