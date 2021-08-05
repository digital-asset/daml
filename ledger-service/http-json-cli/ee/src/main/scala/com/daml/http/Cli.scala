// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
import com.daml.http.dbbackend.DBConfig

object Cli extends CliBase {
  override protected def configParser(getEnvVar: String => Option[String])(implicit
      supportedJdbcDriverNames: DBConfig.SupportedJdbcDriverNames
  ): OptionParser =
    new OptionParser(getEnvVar) with NonRepudiationOptions
}
