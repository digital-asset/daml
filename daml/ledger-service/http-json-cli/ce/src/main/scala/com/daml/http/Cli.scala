// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.dbutils.DBConfig.JdbcConfigDefaults

object Cli extends CliBase {
  override protected def configParser(getEnvVar: String => Option[String])(implicit
      jcd: JdbcConfigDefaults
  ): OptionParser =
    new OptionParser(getEnvVar)
}
