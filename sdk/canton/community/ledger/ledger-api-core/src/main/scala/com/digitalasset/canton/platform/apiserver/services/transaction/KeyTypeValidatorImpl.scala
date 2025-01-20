// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.transaction

import cats.data.EitherT
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.*
import com.daml.lf.language.Ast.Package
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.digitalasset.canton.ledger.api.validation.EventQueryServiceRequestValidator.KeyTypeValidator
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexPackagesService
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.localstore.PackageMetadataStore
import com.digitalasset.canton.platform.packages.DeduplicatingPackageLoader

import scala.concurrent.{ExecutionContext, Future}

class KeyTypeValidatorImpl(
    engine: Engine,
    packagesService: IndexPackagesService,
    packageMetadataStore: PackageMetadataStore,
    metrics: Metrics,
)(implicit executionContext: ExecutionContext)
    extends KeyTypeValidator {

  private val loader = new DeduplicatingPackageLoader()

  private def loadPackage(
      packageId: PackageId
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[Package]] =
    loader.loadPackage(
      packageId = packageId,
      delegate = packagesService.getLfArchive,
      metric = metrics.daml.execution.getLfPackage,
    )

  private def consume(
      result: Result[GlobalKey]
  )(implicit loggingContext: LoggingContextWithTrace): EitherT[Future, String, GlobalKey] =
    result match {
      case ResultDone(result) => EitherT.pure(result)
      case ResultNeedPackage(packageId, resume) =>
        EitherT.liftF(loadPackage(packageId)).flatMap(p => consume(resume(p)))
      case ResultError(err) => EitherT.fromEither(Left(err.toString))
      case unexpected =>
        throw new IllegalStateException(s"Did not expect engine to return $unexpected")
    }

  private def applyT(typeConRef: Ref.TypeConRef, key: Value)(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[Future, String, GlobalKey] =
    for {
      id <- EitherT.fromEither[Future](typeConRef.pkgRef match {
        case id: Ref.PackageRef.Id => Right(id.id)
        case name: Ref.PackageRef.Name =>
          packageMetadataStore.getSnapshot.getUpgradablePackagePreferenceMap
            .get(name.name)
            .toRight(s"Could not resolve package $name")
      })
      identifier = Ref.Identifier(id, typeConRef.qName)
      result = engine.buildGlobalKey(identifier, key)
      kValue <- consume(result)
    } yield kValue

  override def apply(
      typeConRef: Ref.TypeConRef,
      key: Value,
      loggingContext: LoggingContextWithTrace,
  ): Future[Either[String, GlobalKey]] =
    applyT(typeConRef, key)(loggingContext).value

}
