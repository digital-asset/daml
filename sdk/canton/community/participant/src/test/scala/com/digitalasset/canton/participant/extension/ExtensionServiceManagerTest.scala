// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.config.{
  EngineExtensionsConfig,
  ExtensionFunctionDeclaration,
  ExtensionServiceConfig,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

class ExtensionServiceManagerTest extends AsyncWordSpec with BaseTest with HasExecutionContext {

  private implicit val tc: TraceContext = TraceContext.empty

  private def createConfig(
      name: String,
      host: String = "localhost",
      port: Int = 8080,
      declaredFunctions: Seq[ExtensionFunctionDeclaration] = Seq.empty,
  ): ExtensionServiceConfig = {
    ExtensionServiceConfig(
      name = name,
      host = host,
      port = Port.tryCreate(port),
      useTls = false,
      declaredFunctions = declaredFunctions,
    )
  }

  "ExtensionServiceManager" should {

    "create an empty manager" in {
      val manager = ExtensionServiceManager.empty(loggerFactory)

      manager.hasExtensions shouldBe false
      manager.extensionIds shouldBe empty
      manager.getClient("nonexistent") shouldBe None
    }

    "manage multiple extensions" in {
      val configs = Map(
        "oracle" -> createConfig("Price Oracle"),
        "kyc" -> createConfig("KYC Service", port = 8081),
      )

      val manager = new ExtensionServiceManager(
        configs,
        EngineExtensionsConfig.default,
        loggerFactory,
      )

      manager.hasExtensions shouldBe true
      manager.extensionIds shouldBe Set("oracle", "kyc")
      manager.getClient("oracle") shouldBe defined
      manager.getClient("kyc") shouldBe defined
      manager.getClient("nonexistent") shouldBe None
    }

    "return error for unconfigured extension" in {
      val manager = ExtensionServiceManager.empty(loggerFactory)

      val result = manager.handleExternalCall(
        extensionId = "nonexistent",
        functionId = "test",
        configHash = "abc123",
        input = "00112233",
        mode = "submission",
      ).futureValueUS

      result.isLeft shouldBe true
      result.left.getOrElse(fail()).statusCode shouldBe 404
      result.left.getOrElse(fail()).message should include("not configured")
    }

    "run in echo mode when enabled" in {
      val configs = Map(
        "echo" -> createConfig("Echo Service"),
      )

      val manager = new ExtensionServiceManager(
        configs,
        EngineExtensionsConfig(echoMode = true),
        loggerFactory,
      )

      val result = manager.handleExternalCall(
        extensionId = "echo",
        functionId = "test",
        configHash = "abc123",
        input = "00112233",
        mode = "submission",
      ).futureValueUS

      // In echo mode, input is returned as output
      result shouldBe Right("00112233")
    }

    "validate extensions when enabled" in {
      val manager = ExtensionServiceManager.empty(loggerFactory)

      // With no extensions, validation should return empty map
      val result = manager.validateAllExtensions().futureValueUS

      result shouldBe empty
    }

    "skip validation when disabled" in {
      val configs = Map(
        "oracle" -> createConfig("Price Oracle"),
      )

      val manager = new ExtensionServiceManager(
        configs,
        EngineExtensionsConfig(validateExtensionsOnStartup = false),
        loggerFactory,
      )

      val result = manager.validateAllExtensions().futureValueUS

      // When validation is disabled, should return empty map
      result shouldBe empty
    }
  }
}
