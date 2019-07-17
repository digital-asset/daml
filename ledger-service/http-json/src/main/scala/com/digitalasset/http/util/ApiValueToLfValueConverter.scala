// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.util

import com.digitalasset.daml.lf
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.validation.CommandsValidator
import com.digitalasset.ledger.api.{domain, v1 => lav1}
import com.digitalasset.ledger.service.LedgerReader.PackageStore
import com.digitalasset.platform.server.api.validation.{ErrorFactories, IdentifierResolverLike}
import io.grpc.StatusRuntimeException
import scalaz.{Show, \/}

object ApiValueToLfValueConverter {
  final case class Error(cause: StatusRuntimeException)

  object Error {
    implicit val ErrorShow: Show[Error] = new Show[Error] {
      override def shows(e: Error): String =
        s"ApiValueToLfValueConverter.Error: ${e.cause.getMessage}"
    }
  }

  type ApiValueToLfValue =
    lav1.value.Value => Error \/ lf.value.Value[lf.value.Value.AbsoluteContractId]

  def apiValueToLfValue(ledgerId: lar.LedgerId, packageStore: PackageStore): ApiValueToLfValue = {
    val commandsValidator =
      new CommandsValidator(domainLedgerId(ledgerId), new ApiIdentifierResolver(packageStore))

    a: lav1.value.Value =>
      \/.fromEither(commandsValidator.validateValue(a)).leftMap(e => Error(e))
  }

  private def domainLedgerId(a: lar.LedgerId): domain.LedgerId =
    domain.LedgerId(lar.LedgerId.unwrap(a))
}

class ApiIdentifierResolver(packageStore: PackageStore)
    extends IdentifierResolverLike
    with ErrorFactories {

  override def resolveIdentifier(
      apiId: lav1.value.Identifier): Either[StatusRuntimeException, lf.data.Ref.Identifier] = {

    val a: Option[lf.data.Ref.Identifier] = for {
      interface <- packageStore.get(apiId.packageId)
      module <- lf.data.Ref.DottedName.fromString(apiId.moduleName).toOption
      name <- lf.data.Ref.DottedName.fromString(apiId.entityName).toOption
      qualifiedName = lf.data.Ref.QualifiedName(module, name)
      _ <- interface.typeDecls.get(qualifiedName)
      packageId <- lf.data.Ref.PackageId.fromString(apiId.packageId).toOption
    } yield lf.data.Ref.Identifier(packageId, qualifiedName)

    a.toRight(notFound(apiId.toString))
  }
}
