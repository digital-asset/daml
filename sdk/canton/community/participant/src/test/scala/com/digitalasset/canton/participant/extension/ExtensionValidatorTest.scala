// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.participant.config.{
  EngineExtensionsConfig,
  ExtensionFunctionDeclaration,
  ExtensionServiceConfig,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

class ExtensionValidatorTest extends AsyncWordSpec with BaseTest with HasExecutionContext {

  private implicit val tc: TraceContext = TraceContext.empty

  private def createConfig(
      name: String,
      declaredFunctions: Seq[ExtensionFunctionDeclaration] = Seq.empty,
  ): ExtensionServiceConfig = {
    ExtensionServiceConfig(
      name = name,
      host = "localhost",
      port = Port.tryCreate(8080),
      useTls = false,
      declaredFunctions = declaredFunctions,
    )
  }

  "ExtensionValidator" should {

    "validate missing extension" in {
      val manager = ExtensionServiceManager.empty(loggerFactory)
      val validator = new ExtensionValidator(manager, loggerFactory)

      val result = validator.validateExtensionRequirement(
        extensionId = "nonexistent",
        functionId = "test",
        configHash = "abc123",
      )

      result shouldBe a[ExtensionRequirementValidation.MissingExtension]
      result.message should include("nonexistent")
    }

    "validate function not declared" in {
      val configs = Map(
        "oracle" -> createConfig("Price Oracle"),
      )

      val manager = new ExtensionServiceManager(
        configs,
        EngineExtensionsConfig.default,
        loggerFactory,
      )
      val validator = new ExtensionValidator(manager, loggerFactory)

      val result = validator.validateExtensionRequirement(
        extensionId = "oracle",
        functionId = "unknown-function",
        configHash = "abc123",
      )

      result shouldBe a[ExtensionRequirementValidation.FunctionNotDeclared]
      result.message should include("unknown-function")
    }

    "validate config hash mismatch" in {
      val configs = Map(
        "oracle" -> createConfig(
          "Price Oracle",
          declaredFunctions = Seq(
            ExtensionFunctionDeclaration("get-price", "correcthash")
          ),
        ),
      )

      val manager = new ExtensionServiceManager(
        configs,
        EngineExtensionsConfig.default,
        loggerFactory,
      )
      val validator = new ExtensionValidator(manager, loggerFactory)

      val result = validator.validateExtensionRequirement(
        extensionId = "oracle",
        functionId = "get-price",
        configHash = "wronghash",
      )

      result shouldBe a[ExtensionRequirementValidation.ConfigHashMismatch]
      result.message should include("wronghash")
      result.message should include("correcthash")
    }

    "validate matching extension requirement" in {
      val configs = Map(
        "oracle" -> createConfig(
          "Price Oracle",
          declaredFunctions = Seq(
            ExtensionFunctionDeclaration("get-price", "abc123")
          ),
        ),
      )

      val manager = new ExtensionServiceManager(
        configs,
        EngineExtensionsConfig.default,
        loggerFactory,
      )
      val validator = new ExtensionValidator(manager, loggerFactory)

      val result = validator.validateExtensionRequirement(
        extensionId = "oracle",
        functionId = "get-price",
        configHash = "abc123",
      )

      result shouldBe ExtensionRequirementValidation.Valid
    }

    "log validation results without throwing when failOnError is false" in {
      val manager = ExtensionServiceManager.empty(loggerFactory)
      val validator = new ExtensionValidator(manager, loggerFactory)

      val results = Seq(
        ExtensionRequirementValidation.MissingExtension("missing"),
        ExtensionRequirementValidation.FunctionNotDeclared("ext", "func"),
      )

      // Should not throw
      noException should be thrownBy {
        validator.logValidationResults(results, failOnError = false)
      }
    }

    "throw when failOnError is true and there are errors" in {
      val manager = ExtensionServiceManager.empty(loggerFactory)
      val validator = new ExtensionValidator(manager, loggerFactory)

      val results = Seq(
        ExtensionRequirementValidation.MissingExtension("missing"),
      )

      an[IllegalStateException] should be thrownBy {
        validator.logValidationResults(results, failOnError = true)
      }
    }

    "not throw for warnings even when failOnError is true" in {
      val manager = ExtensionServiceManager.empty(loggerFactory)
      val validator = new ExtensionValidator(manager, loggerFactory)

      val results = Seq(
        ExtensionRequirementValidation.FunctionNotDeclared("ext", "func"),
      )

      // FunctionNotDeclared is a warning, not an error
      noException should be thrownBy {
        validator.logValidationResults(results, failOnError = true)
      }
    }
  }
}
