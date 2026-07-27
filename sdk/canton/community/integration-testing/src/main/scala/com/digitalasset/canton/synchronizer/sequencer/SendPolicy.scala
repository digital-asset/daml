// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.sequencing.protocol.{SubmissionRequest, TimeProof}
import com.digitalasset.canton.tracing.TraceContext

/** A send policy determines for each [[SubmissionRequest]] what the [[ProgrammableSequencer]]
  * should do with it.
  *
  * Policies must not try to access resources that might not be available until the end of the test,
  * which includes the shutdown period. In particular, avoid
  * [[com.digitalasset.canton.console.LocalParticipantReference.id]] because the internal gRPC call
  * might fail if the participant is already shutting down.
  */
trait SendPolicy extends (TraceContext => SendPolicyWithoutTraceContext)

/** Version of [[SendPolicy]] that doesn't need the trace context */
trait SendPolicyWithoutTraceContext extends (SubmissionRequest => SendDecision)

object SendPolicy {

  /** Process submissions that look like they are for time proofs and then delegate to the given
    * policy for everything else
    */
  def processTimeProofs(policy: SendPolicy): SendPolicy =
    traceContext =>
      submission =>
        if (TimeProof.isTimeProofSubmission(submission)) SendDecision.Process
        else policy(traceContext)(submission)

  def processTimeProofs_(policy: SendPolicyWithoutTraceContext): SendPolicyWithoutTraceContext =
    submission =>
      if (TimeProof.isTimeProofSubmission(submission)) SendDecision.Process
      else policy(submission)

  /** Process all messages as usual */
  val default: SendPolicy = _ => _ => SendDecision.Process
}
