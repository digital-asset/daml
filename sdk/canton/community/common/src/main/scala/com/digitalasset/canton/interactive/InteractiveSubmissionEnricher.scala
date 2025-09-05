// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.interactive

import com.digitalasset.canton.interactive.InteractiveSubmissionEnricher.PackageResolver
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.engine.*
import com.digitalasset.daml.lf.language.Ast.Package
import com.digitalasset.daml.lf.transaction.{FatContractInstance, Node, VersionedTransaction}

import scala.concurrent.ExecutionContext

object InteractiveSubmissionEnricher {
  type PackageResolver = PackageId => TraceContext => FutureUnlessShutdown[Option[Package]]
}

class InteractiveSubmissionEnricher(engine: Engine, packageResolver: PackageResolver) {
  private lazy val enricher = new Enricher(
    engine,
    // TODO(i21582) Because we do not hash suffixed CIDs, we need to disable validation of suffixed CIDs otherwise enrichment
    // will fail
    forbidLocalContractIds = false,
    // Add type info for the pretty-printed prepared transaction
    addTypeInfo = true,
    // Add field names for the pretty-printed prepared transaction
    addFieldNames = true,
    // Do not add trailing none fields to stay consistent between the submitted transaction and the re-interpreted transaction
    // Without this, conformance checking will fail.
    addTrailingNoneFields = false,
  )

  /** Enrich versioned transaction with type info and labels. Leave out trailing none fields.
    */
  def enrichVersionedTransaction(versionedTransaction: VersionedTransaction)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[VersionedTransaction] =
    consumeEnricherResult(enricher.enrichVersionedTransaction(versionedTransaction))

  /** Enrich FCI with type info and labels. Leave out trailing none fields.
    */
  def enrichContract(contract: FatContractInstance)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[FatContractInstance] =
    consumeEnricherResult(enricher.enrichContract(contract))

  /** Enrich create node with type info and labels. Leave out trailing none fields.
    */
  def enrichCreateNode(create: Node.Create)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Node.Create] =
    consumeEnricherResult(enricher.enrichCreate(create))

  private[this] def consumeEnricherResult[V](
      result: Result[V]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[V] =
    result match {
      case ResultDone(r) => FutureUnlessShutdown.pure(r)
      case ResultError(e) => FutureUnlessShutdown.failed(new RuntimeException(e.message))
      case ResultNeedPackage(packageId, resume) =>
        packageResolver(packageId)(traceContext)
          .flatMap(pkgO => consumeEnricherResult(resume(pkgO)))
      case result =>
        FutureUnlessShutdown.failed(new RuntimeException(s"Unexpected LfEnricher result: $result"))
    }
}
