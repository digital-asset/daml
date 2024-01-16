// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.error.CantonErrorGroups.ConfigErrorGroup
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.typesafe.config.ConfigException
import org.slf4j.event.Level
import pureconfig.error.ConfigReaderFailures

import java.io.File
import scala.collection.mutable
import scala.reflect.{ClassTag, classTag}

/** * Trait which acts as a wrapper around
  *  1. `lightbend ConfigException`s which are caught when attempting to read or parse a configuration file
  *  2. `pureconfig ConfigReaderFailures` which are returned when attempting to convert a given
  *  [[com.typesafe.config.Config]] instance (basically a valid HOCON-file)
  *    to one of the Canton configs
  */
object ConfigErrors extends ConfigErrorGroup {

  sealed abstract class ConfigErrorCode(id: String)
      extends ErrorCode(id, ErrorCategory.InvalidIndependentOfSystemState) {
    // we classify ConfigErrors as ERROR so they are shown directly to the user when he attempts to start Canton
    // via canton -c <config-file>
    override def logLevel: Level = Level.ERROR
    override def errorConveyanceDocString: Option[String] =
      Some(
        "Config errors are logged and output to stdout if starting Canton with a given configuration fails"
      )
  }

  abstract class CantonConfigError(
      override val cause: String,
      override val throwableO: Option[Throwable] = None,
  )(implicit override val code: ErrorCode)
      extends CantonError {}

  sealed abstract class ExceptionBasedConfigError(
      override val cause: String,
      override val throwableO: Option[Throwable] = None,
  )(implicit override val code: ErrorCode)
      extends CantonConfigError(cause, throwableO)(code) {

    def exceptions: Seq[ConfigException]

    override def log(): Unit = {
      super.log()

      exceptions.foreach { e =>
        loggingContext.logger.debug(
          code.toMsg(
            s"Received the following exception while attempting to parse the Canton config files",
            loggingContext.traceContext.traceId,
          ),
          e,
        )(loggingContext.traceContext)
      }
    }
  }

  final case object NoConfigFiles extends ConfigErrorCode("NO_CONFIG_FILES") {
    final case class Error()(implicit override val loggingContext: ErrorLoggingContext)
        extends CantonConfigError(
          "No config files were given to Canton. We require at least one config file given via --config or a key:value pair given via -C."
        )
  }

  @Resolution(""" In general, this can be one of many errors since this is the 'miscellaneous category' of configuration errors.
      | One of the more common errors in this category is an 'unknown key' error. This error usually means that
      |  a keyword that is not valid (e.g. it may have a typo 'bort' instead of 'port'), or that a valid keyword
      |  at the wrong part of the configuration hierarchy was used (e.g. to enable database replication for a participant, the correct configuration
      |  is `canton.participants.participant2.replication.enabled = true` and not `canton.participants.replication.enabled = true`).
      |  Please refer to the scaladoc of either `CantonEnterpriseConfig` or `CantonCommunityConfig` (depending on whether the community or enterprise version is used) to find the valid configuration keywords and the correct position in the configuration hierarchy.
      |""")
  final case object GenericConfigError extends ConfigErrorCode("GENERIC_CONFIG_ERROR") {
    final case class Error(override val cause: String)(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonConfigError(cause)
  }

  @Explanation(
    "This error is usually thrown when Canton can't find a given configuration file."
  )
  @Resolution(
    "Make sure that the path and name of all configuration files is correctly specified. "
  )
  final case object CannotReadFilesError extends ConfigErrorCode("CANNOT_READ_CONFIG_FILES") {
    final case class Error(unreadableFiles: Seq[File])(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonConfigError("At least one configuration file could not be read.")
  }

  @Explanation(
    "This error is usually thrown because a config file doesn't contain configs in valid HOCON format. " +
      "The most common cause of an invalid HOCON format is a forgotten bracket."
  )
  @Resolution("Make sure that all files are in valid HOCON format.")
  final case object CannotParseFilesError extends ConfigErrorCode("CANNOT_PARSE_CONFIG_FILES") {
    final case class Error(override val exceptions: Seq[ConfigException])(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends ExceptionBasedConfigError(
          s"Received an exception (full stack trace has been logged at DEBUG level) while attempting to parse ${exceptions.length} .conf-file(s)."
        )
  }

  @Resolution(
    "A common cause of this error is attempting to use an environment variable that isn't defined within a config-file. "
  )
  final case object SubstitutionError extends ConfigErrorCode("CONFIG_SUBSTITUTION_ERROR") {
    final case class Error(override val exceptions: Seq[ConfigException])(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends ExceptionBasedConfigError(
          s"Received an exception (full stack trace has been logged at DEBUG level) while attempting to parse ${exceptions.length} .conf-file(s)."
        )
  }

  final case object ValidationError extends ConfigErrorCode("CONFIG_VALIDATION_ERROR") {
    final case class Error(causes: Seq[String])(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends CantonConfigError(
          s"Failed to validate the configuration due to: ${causes.mkString("\n")}"
        )
  }

  def getMessage[ConfClass: ClassTag](failures: ConfigReaderFailures): String = {
    val linesBuffer = mutable.Buffer.empty[String]
    linesBuffer += s"Cannot convert configuration to a config of ${classTag[ConfClass].runtimeClass}. Failures are:"
    linesBuffer += failures.prettyPrint(1)
    linesBuffer += ""
    linesBuffer.mkString(System.lineSeparator())
  }
}
