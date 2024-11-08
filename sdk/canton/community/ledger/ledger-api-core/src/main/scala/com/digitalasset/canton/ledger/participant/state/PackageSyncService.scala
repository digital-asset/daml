// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.daml.error.ContextualizedErrorLogger
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.protocol.PackageDescription
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.archive.DamlLf.Archive
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.google.protobuf.ByteString

import scala.concurrent.Future

/** An interface for uploading and validating packages via a participant. */
trait PackageSyncService {

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

  def getPackageMetadataSnapshot(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): PackageMetadata =
    throw new UnsupportedOperationException()

  def listLfPackages()(implicit
      traceContext: TraceContext
  ): Future[Seq[PackageDescription]] =
    throw new UnsupportedOperationException()

  def getLfArchive(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): Future[Option[Archive]] =
    throw new UnsupportedOperationException()

  def validateDar(
      dar: ByteString,
      darName: String,
  )(implicit
      traceContext: TraceContext
  ): Future[SubmissionResult] =
    throw new UnsupportedOperationException()

}
