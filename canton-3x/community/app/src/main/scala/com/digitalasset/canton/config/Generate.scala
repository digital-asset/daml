// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import better.files.{File as BFile}
import com.digitalasset.canton.cli.Command
import com.digitalasset.canton.environment.Environment
import pureconfig.ConfigWriter

object Generate {

  private def write[A](name: String, prefix: String, config: A)(implicit
      configWriter: ConfigWriter[A]
  ): Unit = {
    val _ = BFile(s"remote-${name}.conf")
      .write(
        s"canton.remote-${prefix}.${name} {" + System.lineSeparator() + configWriter
          .to(config)
          .render(CantonConfig.defaultConfigRenderer) + System.lineSeparator() + "}" + System
          .lineSeparator()
      )
  }

  def process[E <: Environment](command: Command.Generate.Target, config: E#Config): Unit =
    command match {
      case Command.Generate.RemoteConfig =>
        val writers = new CantonConfig.ConfigWriters(confidential = false)
        import writers.*

        config.participantsByStringX.map(x => (x._1, x._2.toRemoteConfig)).foreach {
          case (name, config) => write(name, "participants-x", config)
        }
        config.sequencersByStringX.map(x => (x._1, x._2.toRemoteConfig)).foreach {
          case (name, config) => write(name, "sequencers-x", config)
        }
        config.mediatorsByStringX.map(x => (x._1, x._2.toRemoteConfig)).foreach {
          case (name, config) => write(name, "mediators-x", config)
        }

        // legacy
        config.participantsByString.map(x => (x._1, x._2.toRemoteConfig)).foreach {
          case (name, config) => write(name, "participants", config)
        }
        config.domainsByString.map(x => (x._1, x._2.toRemoteConfig)).foreach {
          case (name, config) => write(name, "domains", config)
        }
    }

}
