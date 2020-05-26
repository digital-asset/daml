// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.sdk

import com.daml.codegen.{CodegenMain => Codegen}
import com.daml.lf.engine.script.{RunnerMain => Script, TestMain => TestScript}
import com.daml.lf.engine.trigger.{RunnerMain => Trigger}
import com.daml.extractor.{Main => Extractor}
import com.daml.http.{Main => JsonApi}
import com.daml.navigator.{NavigatorBackend => Navigator}
import com.daml.platform.sandbox.{SandboxMain => SandboxClassic}
import com.daml.platform.sandboxnext.{Main => Sandbox}

object SdkMain {
  def main(args: Array[String]): Unit = {
    val command = args(0)
    val rest = args.drop(1)
    command match {
      case "trigger" => Trigger.main(rest)
      case "script" => Script.main(rest)
      case "test-script" => TestScript.main(rest)
      case "codegen" => Codegen.main(rest)
      case "extractor" => Extractor.main(rest)
      case "json-api" => JsonApi.main(rest)
      case "navigator" => Navigator.main(rest)
      case "sandbox" => Sandbox.main(rest)
      case "sandbox-classic" => SandboxClassic.main(rest)
      case _ => sys.exit(1)
    }
  }
}
