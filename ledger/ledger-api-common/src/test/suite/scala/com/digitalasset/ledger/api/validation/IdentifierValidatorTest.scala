// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.api.validation.FieldValidations
import com.daml.lf.data.Ref
import io.grpc.Status.Code.INVALID_ARGUMENT
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AsyncWordSpec

class IdentifierValidatorTest extends AsyncWordSpec with ValidatorTestUtils with MockitoSugar {

  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger = NoLogging

  object api {
    val identifier = Identifier("package", moduleName = "module", entityName = "entity")
  }

  object Mocks {
    val identifier = Ref.Identifier(
      Ref.PackageId.assertFromString("package"),
      Ref.QualifiedName.assertFromString("module:entity"),
    )
  }

  "validating identifiers" should {
    "convert a valid identifier" in {
      FieldValidations.validateIdentifier(api.identifier) shouldEqual Right(Mocks.identifier)
    }

    "not allow missing package ids" in {
      requestMustFailWith(
        FieldValidations.validateIdentifier(api.identifier.withPackageId("")),
        code = INVALID_ARGUMENT,
        description =
          "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: package_id",
        metadata = Map.empty,
      )
    }

    "not allow missing names" in {
      requestMustFailWith(
        request =
          FieldValidations.validateIdentifier(api.identifier.withModuleName("").withEntityName("")),
        code = INVALID_ARGUMENT,
        description =
          "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field module_name: Expected a non-empty string",
        metadata = Map.empty,
      )
    }
  }

}
