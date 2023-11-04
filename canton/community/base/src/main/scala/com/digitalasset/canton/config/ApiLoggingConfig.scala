// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.logging.pretty.CantonPrettyPrinter

/** Control logging of the ApiRequestLogger
  *
  * Every GRPC service invocation is logged through the ApiRequestLogger. This allows
  * to monitor all incoming traffic to a node (ledger API, sequencer API, admin API).
  *
  * @param messagePayloads Indicates whether to log message payloads. (To be disabled in production!)
  *                          Also applies to metadata. None is equivalent to false.
  * @param maxMethodLength indicates how much to abbreviate the name of the called method.
  *                        E.g. "com.digitalasset.canton.MyMethod" may get abbreviated to "c.d.c.MyMethod".
  *                        The last token will never get abbreviated.
  * @param maxMessageLines maximum number of lines to log for a message
  * @param maxStringLength maximum number of characters to log for a string within a message
  * @param maxMetadataSize maximum size of metadata
  * @param warnBeyondLoad If API logging is turned on, emit a warning on each request if the load exceeds this threshold.
  */
final case class ApiLoggingConfig(
    // TODO(#15221) change to boolean (breaking change)
    messagePayloads: Option[Boolean] = None,
    maxMethodLength: Int = ApiLoggingConfig.defaultMaxMethodLength,
    maxMessageLines: Int = ApiLoggingConfig.defaultMaxMessageLines,
    maxStringLength: Int = ApiLoggingConfig.defaultMaxStringLength,
    maxMetadataSize: Int = ApiLoggingConfig.defaultMaxMetadataSize,
    warnBeyondLoad: Option[Int] = ApiLoggingConfig.defaultWarnBeyondLoad,
) {

  def logMessagePayloads: Boolean = messagePayloads.getOrElse(false)

  /** Pretty printer for logging event details */
  lazy val printer = new CantonPrettyPrinter(maxStringLength, maxMessageLines)

}

object ApiLoggingConfig {
  val defaultMaxMethodLength: Int = 30
  val defaultMaxMessageLines: Int = 20
  val defaultMaxStringLength: Int = 250
  val defaultMaxMetadataSize: Int = 200
  val defaultWarnBeyondLoad: Option[Int] = Some(5)
}
