// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.sdk

import com.daml.codegen.{CodegenMain => Codegen}
import com.digitalasset.daml.lf.engine.script.{ScriptMain => Script}

object SdkMain {
  def main(args: Array[String]): Unit = {
    val command = args(0)
    val rest = args.drop(1)
    command match {
      case "script" => Script.main(rest)
      case "codegen" => Codegen.main(rest)
      case _ => sys.exit(1)
    }
  }
}
