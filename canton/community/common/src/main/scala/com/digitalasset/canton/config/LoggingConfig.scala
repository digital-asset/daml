// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

/** Detailed logging configurations
  *
  * This section allows to configure additional data such as transaction details to be logged to the standard logback system
  *
  * @param api Configuration settings for the ApiRequestLogger
  * @param eventDetails If set to true, we will log substantial details of internal messages being processed. To be disabled in production!
  * @param logConfigOnStartup If set to true (default), it will log the config on startup (omitting sensitive details)
  * @param logConfigWithDefaults If set to true (default false), the default values of the config will be included
  */
final case class LoggingConfig(
    api: ApiLoggingConfig = ApiLoggingConfig(),
    eventDetails: Boolean = false,
    logConfigOnStartup: Boolean = true,
    logConfigWithDefaults: Boolean = false,
)
