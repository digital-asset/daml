// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.v2

import com.daml.lf.data.Ref
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString

import java.util.concurrent.CompletionStage

/** An interface for uploading packages via a participant. */
trait WritePackagesService {

  /** Upload a collection of Daml-LF packages to the ledger.
    *
    * This method must be thread-safe, not throw, and not block on IO. It is
    * though allowed to perform significant computation.
    *
    * Successful archives upload will result in a [[com.digitalasset.canton.ledger.participant.state.v2.Update.PublicPackageUpload]]
    * message. See the comments on [[com.digitalasset.canton.ledger.participant.state.v2.ReadService.stateUpdates]] and [[com.digitalasset.canton.ledger.participant.state.v2.Update]] for
    * further details.
    *
    * Note: we accept [[com.daml.daml_lf_dev.DamlLf.Archive]]s rather than parsed packages, because we want
    * to be able to get the byte size of each individual ArchivePayload, which
    * is information that the read / index service need to provide. Moreover
    * this information should be consistent with the payload that the
    * [[com.daml.ledger.api.v1.package_service.GetPackageResponse]]
    * contains. If we were to consume packages we'd have to re-encode them to
    * provide the size, and the size might potentially be different from the
    * original size, which would be quite confusing.
    *
    * @param submissionId       Submitter chosen submission identifier.
    * @param sourceDescription  Description provided by the backing participant
    *   describing where it got the package from, e.g., when, where, or by whom
    *   the packages were uploaded.
    * @param archives           Daml-LF archives to be uploaded to the ledger.
    *    All archives must be valid, i.e., they must successfully decode and pass
    *    Daml engine validation.
    *
    * @return an async result of a [[com.digitalasset.canton.ledger.participant.state.v2.SubmissionResult]]
    */
  def uploadPackages(
      submissionId: Ref.SubmissionId,
      dar: ByteString,
  )(implicit
      traceContext: TraceContext
  ): CompletionStage[SubmissionResult]
}
