// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.ledger.api.DomainMocks
import com.daml.ledger.api.v1.value.Identifier
import com.daml.platform.server.api.validation.{ErrorFactories, FieldValidations}
import io.grpc.Status.Code.INVALID_ARGUMENT
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AsyncWordSpec

class IdentifierValidatorTest extends AsyncWordSpec with ValidatorTestUtils with MockitoSugar {

  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger = NoLogging

  private val errorFactories_mock = mock[ErrorFactories]
  private val fieldValidations = FieldValidations(errorFactories_mock)
  private val fixture = new ValidatorFixture(() => FieldValidations(ErrorFactories()))

  object api {
    val identifier = Identifier("package", moduleName = "module", entityName = "entity")
  }

  "validating identifiers" should {
    "convert a valid identifier" in {
      fieldValidations.validateIdentifier(api.identifier) shouldEqual Right(DomainMocks.identifier)
      verifyZeroInteractions(errorFactories_mock)
      succeed
    }

    "not allow missing package ids" in {
      fixture.testRequestFailure(
        _.validateIdentifier(api.identifier.withPackageId("")),
        expectedCode = INVALID_ARGUMENT,
        expectedDescription =
          "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: package_id",
      )
    }

    "not allow missing names" in {
      fixture.testRequestFailure(
        _.validateIdentifier(api.identifier.withModuleName("").withEntityName("")),
        expectedCode = INVALID_ARGUMENT,
        expectedDescription =
          "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field module_name: Expected a non-empty string",
      )
    }
  }

}
