package com.digitalasset.daml.sdk
import com.digitalasset.daml.lf.engine.trigger.{RunnerMain => Trigger}
import com.digitalasset.daml.lf.engine.script.{RunnerMain => Script}
import com.digitalasset.codegen.{CodegenMain => Codegen}
import com.digitalasset.extractor.{Main => Extractor}
import com.digitalasset.http.{Main => JsonApi}

object SdkMain {
    def main(args: Array[String]): Unit = {
        val command = args(0)
        val rest = args.drop(1)
        command match {
            case "trigger" => Trigger.main(rest)
            case "script" => Script.main(rest)
            case "codegen" => Codegen.main(rest)
            case "extractor" => Extractor.main(rest)
            case "json-api" => JsonApi.main(rest)
            case _ => sys.exit(1)
        }
    }
}
