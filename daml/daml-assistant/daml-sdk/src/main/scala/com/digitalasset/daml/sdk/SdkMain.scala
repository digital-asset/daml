// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.sdk

import com.daml.codegen.{CodegenMain => Codegen}
import com.daml.lf.engine.script.{ScriptMain => Script}
import com.daml.lf.engine.trigger.{RunnerMain => Trigger}
import com.daml.lf.engine.trigger.{ServiceMain => TriggerService}
import com.daml.auth.middleware.oauth2.{Main => Oauth2Middleware}
import com.daml.http.{Main => JsonApi}
import com.daml.navigator.{NavigatorBackend => Navigator}
import com.daml.script.export.{Main => Export}

object SdkMain {
  def main(args: Array[String]): Unit = {
    val command = args(0)
    val rest = args.drop(1)
    command match {
      case "trigger" => Trigger.main(rest)
      case "script" => Script.main(rest)
      case "export" => Export.main(rest)
      case "codegen" => Codegen.main(rest)
      case "json-api" => JsonApi.main(rest)
      case "trigger-service" => TriggerService.main(rest)
      case "oauth2-middleware" => Oauth2Middleware.main(rest)
      case "navigator" => Navigator.main(rest)
      case _ => sys.exit(1)
    }
  }
}
