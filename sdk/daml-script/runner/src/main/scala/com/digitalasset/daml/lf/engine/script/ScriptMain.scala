// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

object ScriptMain {

  def main(args: Array[String]): Unit = {
    ScriptConfig.parse(args) match {
      case None => sys.exit(1)
      case Some(config) =>
        config.toCommand() match {
          case Left(err) =>
            System.err.println(err)
            sys.exit(1)
          case Right(cmd) =>
            cmd match {
              case ScriptCommand.RunOne(conf) =>
                RunnerMain.main(conf)
              case ScriptCommand.RunAll(conf) =>
                TestMain.main(conf)
            }
        }
    }
  }
}
