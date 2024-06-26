// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.daml.lf.data.Ref
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString

import scala.concurrent.Future

/** An interface for uploading and validating packages via a participant. */
trait WritePackagesService {

  /** Upload a DAR to the ledger.
    *
    * This method must be thread-safe, not throw, and not block on IO. It is
    * though allowed to perform significant computation.
    *
    * @param dar           The DAR payload as ByteString.
    * @param submissionId       Submitter chosen submission identifier.
    *
    * @return an async result of a [[com.digitalasset.canton.ledger.participant.state.SubmissionResult]]
    */
  def uploadDar(
      dar: ByteString,
      submissionId: Ref.SubmissionId,
  )(implicit
      traceContext: TraceContext
  ): Future[SubmissionResult]
}
