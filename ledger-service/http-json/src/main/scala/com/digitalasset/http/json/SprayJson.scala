// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import akka.util.ByteString
import scalaz.{Show, \/}
import spray.json.{JsValue, JsonReader, _}

object SprayJson {
  case class Error(value: String, message: String)

  object Error {
    implicit val show: Show[Error] = new Show[Error] {
      override def shows(f: Error): String = f.message
    }
  }

  def parse[A: JsonReader](str: ByteString): Error \/ A =
    parse(str.utf8String)

  def parse[A: JsonReader](str: String): Error \/ A =
    for {
      jsValue <- \/.fromTryCatchNonFatal(str.parseJson)
        .leftMap(e => Error(str, e.getMessage)): Error \/ JsValue
      a <- parse(jsValue)
    } yield a

  def parse[A: JsonReader](a: JsValue): Error \/ A =
    \/.fromTryCatchNonFatal(a.convertTo[A]).leftMap(e => Error(a.toString, e.getMessage))
}
