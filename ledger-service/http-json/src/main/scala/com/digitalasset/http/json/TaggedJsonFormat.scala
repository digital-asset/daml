// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import scalaz.{@@, Tag}
import spray.json.{JsValue, JsonFormat}

object TaggedJsonFormat {

  def taggedJsonFormat[A: JsonFormat, T]: JsonFormat[A @@ T] = new JsonFormat[A @@ T] {

    private val jsonFormat = implicitly[JsonFormat[A]]

    override def write(a: A @@ T): JsValue = {
      jsonFormat.write(Tag.unwrap(a))
    }

    override def read(json: JsValue): A @@ T = {
      val a: A = jsonFormat.read(json)
      Tag.apply[A, T](a)
    }
  }
}
