package com.digitalasset.http.json

import akka.util.ByteString
import scalaz.\/
import spray.json.{JsValue, JsonReader, _}

import scala.util.Try

object SprayJson {
  def parse[A: JsonReader](str: ByteString): String \/ A =
    Try {
      val jsonAst: JsValue = str.utf8String.parseJson
      jsonAst.convertTo[A]
    } fold (t => \/.left(s"JSON parser error: ${t.getMessage}"), a => \/.right(a))
}
