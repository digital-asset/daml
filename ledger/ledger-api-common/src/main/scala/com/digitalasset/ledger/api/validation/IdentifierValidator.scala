// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.validation

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.DeprecatedIdentifier
import com.digitalasset.daml.lf.language.Ast.Package
import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.platform.common.util.DirectExecutionContext.implicitEC
import com.digitalasset.platform.server.api.validation.ErrorFactories
import com.digitalasset.platform.server.api.validation.FieldValidations._
import com.github.ghik.silencer.silent
import io.grpc.StatusRuntimeException

import scala.concurrent.Future

object IdentifierValidator {

  def validateIdentifier(
      identifier: Identifier,
      packageResolver: Ref.PackageId => Future[Option[Package]]): Future[Ref.Identifier] =
    lift(validateNewStyleIdentifier(identifier)).recoverWith {
      case error: StatusRuntimeException =>
        fromDeprecatedIdentifier(identifier, error, packageResolver)
    }

  def validateNewStyleIdentifier(
      identifier: Identifier): Either[StatusRuntimeException, Ref.Identifier] =
    for {
      packageId <- requirePackageId(identifier.packageId, "package_id")
      name <- validateSplitIdentifier(identifier)
    } yield Ref.Identifier(packageId, name)

  // Validating the new identifier message with split module and entity name
  private def validateSplitIdentifier(identifier: Identifier) =
    for {
      mn <- requireDottedName(identifier.moduleName, "module_name")
      en <- requireDottedName(identifier.entityName, "entity_name")
    } yield Ref.QualifiedName(mn, en)

  // in case the identifier uses the old format with a single string,
  // we check the deprecated `name` field and look it up through the package resolver, since we
  // cannot know for sure which dot in the name actually splits the module name from the entity name
  //
  // suppress deprecation warnings because we _need_ to use the deprecated .name here -- the entire
  // point of this method is to process it.
  @silent
  private def fromDeprecatedIdentifier(
      identifier: Identifier,
      error: StatusRuntimeException,
      packageResolver: Ref.PackageId => Future[Option[Package]]): Future[Ref.Identifier] =
    for {
      // if `name` is not empty, we give back the error from validating the non-deprecated fields
      _ <- lift(requireNonEmptyString(identifier.name, "name")).transform(identity, _ => error)
      packageId <- lift(requirePackageId(identifier.packageId, "package_id"))
      pkg <- packageResolver(packageId).map(
        _.getOrElse(throw ErrorFactories.notFound(s"packageId: ${identifier.packageId}")))
      result <- lift(
        DeprecatedIdentifier
          .lookup(pkg, identifier.name)
          .left
          .map(ErrorFactories.invalidArgument))
    } yield Ref.Identifier(packageId, result)

  private def lift[A](value: Either[StatusRuntimeException, A]): Future[A] =
    Future.fromTry(value.toTry)

  private def liftS[A](value: Either[String, A]): Future[A] = value match {
    case Left(error) => Future.failed(new IllegalStateException(error))
    case Right(a) => Future.successful(a)
  }

}
