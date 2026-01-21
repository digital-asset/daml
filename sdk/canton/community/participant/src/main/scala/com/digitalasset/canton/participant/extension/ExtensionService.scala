// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tracing.TraceContext

/** Error information from external call failures */
sealed trait ExtensionCallError {
  def statusCode: Int
  def message: String
  def requestId: Option[String]
}

object ExtensionCallError {
  /** Create a simple ExtensionCallError without retry information */
  def apply(statusCode: Int, message: String, requestId: Option[String]): ExtensionCallError =
    ExtensionCallErrorSimple(statusCode, message, requestId)

  /** Unapply for pattern matching */
  def unapply(e: ExtensionCallError): Some[(Int, String, Option[String])] =
    Some((e.statusCode, e.message, e.requestId))
}

/** Simple extension call error without retry information */
final case class ExtensionCallErrorSimple(
    statusCode: Int,
    message: String,
    requestId: Option[String],
) extends ExtensionCallError

/** Extension call error with retry-after header support */
final case class ExtensionCallErrorWithRetry(
    statusCode: Int,
    message: String,
    requestId: Option[String],
    retryAfterSeconds: Option[Int],
) extends ExtensionCallError

/** Result of validating an extension service configuration */
sealed trait ExtensionValidationResult
object ExtensionValidationResult {
  case object Valid extends ExtensionValidationResult
  final case class Invalid(errors: Seq[String]) extends ExtensionValidationResult
}

/** Trait for extension service clients.
  *
  * Each configured extension service has its own client that handles
  * HTTP communication with connection pooling managed at the participant level.
  */
trait ExtensionServiceClient {

  /** The extension identifier (from Canton config) */
  def extensionId: String

  /** Make an external call to the extension service.
    *
    * @param functionId Function identifier within the extension
    * @param configHash Configuration hash (hex) for version validation
    * @param input Input data (hex)
    * @param mode Execution mode ("submission" or "validation")
    * @return Either an error or the response body (hex)
    */
  def call(
      functionId: String,
      configHash: String,
      input: String,
      mode: String,
  )(implicit tc: TraceContext): FutureUnlessShutdown[Either[ExtensionCallError, String]]

  /** Get the declared config hash for a function, if declared.
    *
    * Used for DAR-extension validation on startup.
    */
  def getDeclaredConfigHash(functionId: String): Option[String]

  /** Validate that the extension service is reachable and properly configured.
    *
    * This is called on participant startup if validation is enabled.
    */
  def validateConfiguration()(implicit tc: TraceContext): FutureUnlessShutdown[ExtensionValidationResult]
}
