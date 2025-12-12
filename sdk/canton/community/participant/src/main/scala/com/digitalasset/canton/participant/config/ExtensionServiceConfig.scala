// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.config

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port, PositiveInt}
import com.digitalasset.canton.config.{CantonConfigValidator, NonNegativeFiniteDuration}
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation

import java.nio.file.Path
import scala.concurrent.duration.DurationInt

/** Configuration for a single engine extension service.
  *
  * Extension services allow Daml contracts to make external calls to deterministic
  * services. The participant manages connection pooling and HTTP calls on behalf
  * of the engine.
  *
  * @param name Human-readable name for the extension
  * @param host Hostname of the extension service
  * @param port Port of the extension service
  * @param useTls Whether to use TLS for the connection
  * @param tlsInsecure If true, skip TLS certificate validation (dev only)
  * @param jwt JWT token for authentication
  * @param jwtFile Path to file containing JWT token
  * @param connectTimeout Connection timeout
  * @param requestTimeout Request timeout for individual HTTP requests
  * @param maxTotalTimeout Maximum total time for the entire operation including retries
  * @param maxRetries Maximum number of retry attempts
  * @param retryInitialDelay Initial delay before first retry
  * @param retryMaxDelay Maximum delay between retries
  * @param requestIdHeader Name of the request ID header
  * @param declaredFunctions Functions that this extension provides, for validation
  */
final case class ExtensionServiceConfig(
    name: String,
    host: String,
    port: Port,
    useTls: Boolean = true,
    tlsInsecure: Boolean = false,
    jwt: Option[String] = None,
    jwtFile: Option[Path] = None,
    connectTimeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMillis(500),
    requestTimeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(8),
    maxTotalTimeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(25),
    maxRetries: NonNegativeInt = NonNegativeInt.tryCreate(3),
    retryInitialDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMillis(1000),
    retryMaxDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(10),
    requestIdHeader: String = "X-Request-Id",
    declaredFunctions: Seq[ExtensionFunctionDeclaration] = Seq.empty,
)

object ExtensionServiceConfig {
  implicit val extensionServiceConfigCantonConfigValidator
      : CantonConfigValidator[ExtensionServiceConfig] =
    CantonConfigValidatorDerivation[ExtensionServiceConfig]
}

/** Declaration of a function provided by an extension service.
  *
  * Used for validation on startup to ensure DARs have matching extensions configured.
  *
  * @param functionId The function identifier
  * @param configHash Expected configuration hash for version validation
  */
final case class ExtensionFunctionDeclaration(
    functionId: String,
    configHash: String,
)

object ExtensionFunctionDeclaration {
  implicit val extensionFunctionDeclarationCantonConfigValidator
      : CantonConfigValidator[ExtensionFunctionDeclaration] =
    CantonConfigValidatorDerivation[ExtensionFunctionDeclaration]
}

/** Configuration for engine extensions in the participant.
  *
  * @param validateExtensionsOnStartup Whether to validate extension configurations on startup
  * @param failOnExtensionValidationError Whether to fail startup if extension validation fails
  * @param echoMode If true, external calls return the input as output (for testing)
  */
final case class EngineExtensionsConfig(
    validateExtensionsOnStartup: Boolean = true,
    failOnExtensionValidationError: Boolean = true,
    echoMode: Boolean = false,
)

object EngineExtensionsConfig {
  val default: EngineExtensionsConfig = EngineExtensionsConfig()

  implicit val engineExtensionsConfigCantonConfigValidator
      : CantonConfigValidator[EngineExtensionsConfig] =
    CantonConfigValidatorDerivation[EngineExtensionsConfig]
}
