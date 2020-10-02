// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.value.json

import spray.json.{JsObject, JsString, JsValue}

object JsonVariant {
  def apply(tag: String, body: JsValue): JsObject =
    JsObject("tag" -> JsString(tag), "value" -> body)

  def unapply(o: JsObject): Option[(String, JsValue)] =
    (o.fields.size, o.fields.get("tag"), o.fields.get("value")) match {
      case (2, Some(JsString(tag)), Some(nv)) => Some((tag, nv))
      case _ => None
    }
}
