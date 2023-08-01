// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

object ScriptMain {
  def main(args: Array[String]): Unit = {
    RunnerMainConfig.parse(args) match {
      case None => sys.exit(1)
      case Some(config) => RunnerMain.main(config)
    }
  }
}
