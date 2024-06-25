// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.daml.lf.value.json

import com.digitalasset.daml.lf.data.Ref
import spray.json.{JsObject, JsString, JsValue}

object JsonVariant {
  def apply(tag: String, body: JsValue): JsObject =
    JsObject(tagKey -> JsString(tag), valueKey -> body)

  def withoutValue(tag: String): JsObject =
    JsObject(tagKey -> JsString(tag))

  val tagKey = "tag"
  val valueKey: Ref.Name = Ref.Name assertFromString "value"

  def unapply(o: JsObject): Option[(String, JsValue)] =
    (o.fields.size, o.fields.get(tagKey), o.fields.get(valueKey)) match {
      case (2, Some(JsString(tag)), Some(nv)) => Some((tag, nv))
      case _ => None
    }
}
