// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import java.util.concurrent.CompletionStage

import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.telemetry.TelemetryContext

/** An interface for uploading packages via a participant. */
trait WritePackagesService {

  /** Upload a collection of Daml-LF packages to the ledger.
    *
    * This method must be thread-safe, not throw, and not block on IO. It is
    * though allowed to perform significant computation.
    *
    * Successful archives upload will result in a [[Update.PublicPackageUpload]]
    * message. See the comments on [[ReadService.stateUpdates]] and [[Update]] for
    * further details.
    *
    * Note: we accept [[Archive]]s rather than parsed packages, because we want
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
    * @return an async result of a [[SubmissionResult]]
    */
  def uploadPackages(
      submissionId: Ref.SubmissionId,
      archives: List[Archive],
      sourceDescription: Option[String],
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult]
}
