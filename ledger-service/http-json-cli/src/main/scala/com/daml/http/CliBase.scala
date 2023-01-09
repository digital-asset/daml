// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.dbutils.DBConfig

trait CliBase {

  private[http] def parseConfig(
      args: collection.Seq[String],
      supportedJdbcDriverNames: Set[String],
      getEnvVar: String => Option[String] = sys.env.get,
  ): Option[Config] = {
    implicit val jcd: DBConfig.JdbcConfigDefaults =
      DBConfig.JdbcConfigDefaults(supportedJdbcDriverNames)
    configParser(getEnvVar).parse(args, JsonApiCli.Default).flatMap(_.loadConfig)
  }

  protected[this] def configParser(getEnvVar: String => Option[String])(implicit
      jcd: DBConfig.JdbcConfigDefaults
  ): OptionParser

}
