// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.validation

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.engine.DeprecatedIdentifier
import com.digitalasset.daml.lf.lfpackage.Ast
import com.digitalasset.daml.lf.data.{Ref => LfRef}
import com.digitalasset.daml.lf.lfpackage.Ast.Package
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.platform.common.util.DirectExecutionContext.implicitEC
import com.digitalasset.platform.server.api.validation.ErrorFactories
import com.digitalasset.platform.server.api.validation.FieldValidations.requireNonEmptyString
import io.grpc.StatusRuntimeException
import com.github.ghik.silencer.silent

import scala.concurrent.Future

object IdentifierValidator {

  def validateIdentifier(
      identifier: Identifier,
      packageResolver: PackageId => Future[Option[Package]]): Future[domain.Identifier] =
    for {
      packageId <- lift(requireNonEmptyString(identifier.packageId, "package_id"))
      name <- lift(validateSplitIdentifier(identifier)).recoverWith {
        case error: StatusRuntimeException =>
          fromDeprecatedIdentifier(identifier, error, packageResolver)
      }
      (moduleName, entityName) = name
    } yield domain.Identifier(domain.PackageId(packageId), moduleName, entityName)

  // Validating the new identifier message with split module and entity name
  private def validateSplitIdentifier(
      identifier: Identifier): Either[StatusRuntimeException, (String, String)] =
    for {
      mn <- requireNonEmptyString(identifier.moduleName, "module_name")
      en <- requireNonEmptyString(identifier.entityName, "entity_name")
    } yield (mn, en)

  // in case the identifier uses the old format with a single string,
  // we check the deprecated `name` field and do a heuristic separation of module and entity name
  //
  // suppress deprecation warnings because we _need_ to use the deprecated .name here -- the entire
  // point of this method is to process it.
  @silent
  private def fromDeprecatedIdentifier(
      identifier: Identifier,
      error: StatusRuntimeException,
      packageResolver: PackageId => Future[Option[Package]]) =
    for {
      // if `name` is not empty, we give back the error from validating the non-deprecated fields
      name <- lift(requireNonEmptyString(identifier.name, "name")).transform(identity, _ => error)
      packageId <- liftS(LfRef.PackageId.fromString(identifier.packageId)): Future[LfRef.PackageId]
      pkgOpt <- packageResolver(packageId)
      pkg <- pkgOpt
        .map(Future.successful)
        .getOrElse(
          Future.failed[Ast.Package](
            ErrorFactories.notFound(s"packageId: ${identifier.packageId}")))
      result <- lift(
        DeprecatedIdentifier
          .lookup(pkg, identifier.name)
          .map(qn => (qn.module.toString(), qn.name.toString()))
          .left
          .map(ErrorFactories.invalidArgument))
    } yield result

  private def lift[A](value: Either[StatusRuntimeException, A]): Future[A] =
    Future.fromTry(value.toTry)

  private def liftS[A](value: Either[String, A]): Future[A] = value match {
    case Left(error) => Future.failed(new IllegalStateException(error))
    case Right(a) => Future.successful(a)
  }

}
