// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.typesafe.config.{Config => TypesafeConfig, ConfigFactory}

class HoconCli(programName: String) {

  val argParser = new scopt.OptionParser[HoconCli.Cli](programName) {
    opt[Map[String, String]]('C', "config key-value's")
      .text(
        "Set configuration key value pairs directly. Can be useful for providing simple short config info."
      )
      .valueName("<key1>=<value1>,<key2>=<value2>")
      .unbounded()
      .action { (map, cli) =>
        cli.copy(configMap = map ++ cli.configMap)
      }
  }

  def parse(args: Array[String]): Option[HoconCli.Cli] = {
    argParser.parse(args, HoconCli.Cli())
  }

}

object HoconCli {
  case class Cli(
      configMap: Map[String, String] = Map()
  )

  def mergeConfigs(firstConfig: TypesafeConfig, otherConfigs: Seq[TypesafeConfig]): TypesafeConfig =
    otherConfigs.foldLeft(firstConfig)((combined, config) => config.withFallback(combined))

  def loadConfigWithOverrides(programName: String, args: Array[String]): TypesafeConfig = {
    val cliOptions = new HoconCli(programName).parse(args).getOrElse(sys.exit(1))
    val configFromMap = {
      import scala.jdk.CollectionConverters._
      ConfigFactory.parseMap(cliOptions.configMap.asJava)
    }
    ConfigFactory.invalidateCaches()
    val config = ConfigFactory.load()
    mergeConfigs(config, Seq(configFromMap))
  }
}
