// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.validation

import com.digitalasset.ledger.api.DomainMocks
import com.digitalasset.ledger.api.v1.value.Identifier
import io.grpc.Status.Code.INVALID_ARGUMENT
import org.scalatest.AsyncWordSpec
import com.digitalasset.platform.server.api.validation.FieldValidations._

class IdentifierValidatorTest extends AsyncWordSpec with ValidatorTestUtils {

  object api {
    val identifier = Identifier("package", moduleName = "module", entityName = "entity")
  }

  "validating identifiers" should {
    "convert a valid identifier" in {
      validateIdentifier(api.identifier) shouldEqual Right(DomainMocks.identifier)
    }

    "not allow missing package ids" in {
      requestMustFailWith(
        validateIdentifier(api.identifier.withPackageId("")),
        INVALID_ARGUMENT,
        """Missing field: package_id"""
      )
    }

    "not allow missing names" in {
      requestMustFailWith(
        validateIdentifier(api.identifier.withModuleName("").withEntityName("")),
        INVALID_ARGUMENT,
        "Invalid field module_name: Expected a non-empty string"
      )
    }
  }

}
