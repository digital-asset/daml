// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.validation

import com.digitalasset.daml.lf.archive.LanguageVersion
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.lfpackage.Ast
import com.digitalasset.daml.lf.lfpackage.Ast.Package
import com.digitalasset.ledger.api.DomainMocks
import com.digitalasset.ledger.api.v1.value.Identifier

import io.grpc.Status.Code.INVALID_ARGUMENT
import org.scalatest.AsyncWordSpec

import scala.concurrent.Future

class DefinitionRefValidatorTest extends AsyncWordSpec with ValidatorTestUtils {

  object api {
    val identifier = Identifier("package", moduleName = "module", entityName = "entity")
    val deprecatedIdentifier = Identifier("package", name = "module.entity")
  }

  val noneResolver: PackageId => Future[Option[Package]] = _ => Future.successful(None)

  val sut = IdentifierValidator

  "validating identifiers" should {
    "convert a valid identifier" in {
      sut.validateIdentifier(api.identifier, noneResolver).map(_ shouldEqual DomainMocks.identifier)
    }

    "not allow missing package ids" in {
      requestMustFailWith(
        sut.validateIdentifier(api.identifier.withPackageId(""), noneResolver),
        INVALID_ARGUMENT,
        """Invalid field package_id: string "" does not match regex "[a-zA-Z0-9\-_ ]+""""
      )
    }

    "not allow missing names" in {
      requestMustFailWith(
        sut.validateIdentifier(api.identifier.withModuleName("").withEntityName(""), noneResolver),
        INVALID_ARGUMENT,
        "Invalid field module_name: Expected a non-empty string"
      )
    }

    "convert a valid deprecated identifier" in {
      val recordType = Ast.DDataType(true, ImmArray.empty, Ast.DataRecord(ImmArray.empty, None))
      val moduleName = Ref.ModuleName.assertFromString("module")
      val module =
        Ast.Module(
          moduleName,
          Map(Ref.DottedName.assertFromString("entity") -> recordType),
          LanguageVersion.default,
          Ast.FeatureFlags.default)
      val pkg = Ast.Package(Map(moduleName -> module))
      sut
        .validateIdentifier(api.deprecatedIdentifier, _ => Future.successful(Some(pkg)))
        .map(_ shouldEqual DomainMocks.identifier)
    }
  }
}
