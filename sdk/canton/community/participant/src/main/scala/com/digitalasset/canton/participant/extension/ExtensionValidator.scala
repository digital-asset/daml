// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext

/** Validates that DAR extension requirements are satisfied by configured extensions.
  *
  * This validator is called on participant startup to ensure that all DARs
  * have their extension requirements satisfied by the participant's configuration.
  *
  * @param extensionManager The extension service manager
  * @param loggerFactory Logger factory
  */
class ExtensionValidator(
    extensionManager: ExtensionServiceManager,
    override protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  /** Validate that an extension requirement can be satisfied.
    *
    * @param extensionId The required extension ID
    * @param functionId The required function ID
    * @param configHash The expected configuration hash
    * @return Validation result
    */
  def validateExtensionRequirement(
      extensionId: String,
      functionId: String,
      configHash: String,
  )(implicit tc: TraceContext): ExtensionRequirementValidation = {
    extensionManager.getClient(extensionId) match {
      case None =>
        ExtensionRequirementValidation.MissingExtension(extensionId)

      case Some(client) =>
        client.getDeclaredConfigHash(functionId) match {
          case None =>
            // Function not declared in config, but extension exists
            // This is a warning, not an error - the extension might still work
            ExtensionRequirementValidation.FunctionNotDeclared(extensionId, functionId)

          case Some(declaredHash) if declaredHash != configHash =>
            ExtensionRequirementValidation.ConfigHashMismatch(
              extensionId,
              functionId,
              expectedHash = configHash,
              actualHash = declaredHash,
            )

          case Some(_) =>
            ExtensionRequirementValidation.Valid
        }
    }
  }

  /** Log validation results.
    *
    * @param results Validation results to log
    * @param failOnError Whether to throw on validation errors
    */
  def logValidationResults(
      results: Seq[ExtensionRequirementValidation],
      failOnError: Boolean,
  )(implicit tc: TraceContext): Unit = {
    val errors = results.collect {
      case e: ExtensionRequirementValidation.MissingExtension => e
      case e: ExtensionRequirementValidation.ConfigHashMismatch => e
    }

    val warnings = results.collect {
      case w: ExtensionRequirementValidation.FunctionNotDeclared => w
    }

    warnings.foreach { w =>
      logger.warn(s"Extension validation warning: ${w.message}")
    }

    if (errors.nonEmpty) {
      val errorMessages = errors.map(_.message).mkString("; ")
      if (failOnError) {
        throw new IllegalStateException(s"Extension validation failed: $errorMessages")
      } else {
        logger.error(s"Extension validation errors (not failing startup): $errorMessages")
      }
    }
  }
}

/** Result of validating a single extension requirement */
sealed trait ExtensionRequirementValidation {
  def message: String
}

object ExtensionRequirementValidation {

  case object Valid extends ExtensionRequirementValidation {
    override def message: String = "Valid"
  }

  final case class MissingExtension(extensionId: String) extends ExtensionRequirementValidation {
    override def message: String = s"Extension '$extensionId' is not configured"
  }

  final case class FunctionNotDeclared(
      extensionId: String,
      functionId: String,
  ) extends ExtensionRequirementValidation {
    override def message: String =
      s"Function '$functionId' is not declared in extension '$extensionId' configuration"
  }

  final case class ConfigHashMismatch(
      extensionId: String,
      functionId: String,
      expectedHash: String,
      actualHash: String,
  ) extends ExtensionRequirementValidation {
    override def message: String =
      s"Config hash mismatch for '$extensionId/$functionId': expected $expectedHash, got $actualHash"
  }
}
