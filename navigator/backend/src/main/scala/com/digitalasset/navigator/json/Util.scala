// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.json

import spray.json._

/**
  * JSON encoding utils
  */
object Util {
  def strField(obj: JsValue, name: String, as: String): String =
    asObject(obj, as).fields.get(name) match {
      case Some(JsString(v)) => v
      case Some(_) =>
        deserializationError(s"Can't read ${obj.prettyPrint} as $as, field '$name' is not a string")
      case None =>
        deserializationError(s"Can't read ${obj.prettyPrint} as $as, missing field '$name'")
    }

  def intField(obj: JsValue, name: String, as: String): Long =
    asObject(obj, as).fields.get(name) match {
      case Some(JsNumber(v)) => v.toLongExact
      case Some(_) =>
        deserializationError(
          s"Can't read ${obj.prettyPrint} as $as, field '$name' is not an integer")
      case None =>
        deserializationError(s"Can't read ${obj.prettyPrint} as $as, missing field '$name'")
    }

  def boolField(obj: JsValue, name: String, as: String): Boolean =
    asObject(obj, as).fields.get(name) match {
      case Some(JsBoolean(v)) => v
      case Some(_) =>
        deserializationError(
          s"Can't read ${obj.prettyPrint} as $as, field '$name' is not a boolean")
      case None =>
        deserializationError(s"Can't read ${obj.prettyPrint} as $as, missing field '$name'")
    }

  def arrayField(obj: JsValue, name: String, as: String): List[JsValue] =
    asObject(obj, as).fields.get(name) match {
      case Some(JsArray(v)) => v.toList
      case Some(_) =>
        deserializationError(s"Can't read ${obj.prettyPrint} as $as, field '$name' is not an array")
      case None =>
        deserializationError(s"Can't read ${obj.prettyPrint} as $as, missing field '$name'")
    }

  def objectField(obj: JsValue, name: String, as: String): Map[String, JsValue] =
    asObject(obj, as).fields.get(name) match {
      case Some(JsObject(v)) => v
      case Some(_) =>
        deserializationError(s"Can't read ${obj.prettyPrint} as $as, field '$name' is not an array")
      case None =>
        deserializationError(s"Can't read ${obj.prettyPrint} as $as, missing field '$name'")
    }

  def anyField(obj: JsValue, name: String, as: String): JsValue =
    asObject(obj, as).fields.get(name) match {
      case Some(v: JsValue) => v
      case _ => deserializationError(s"Can't read ${obj.prettyPrint} as $as, missing field '$name'")
    }

  def asObject(value: JsValue, as: String): JsObject = value match {
    case obj: JsObject => obj
    case _ =>
      deserializationError(s"Can't read ${value.prettyPrint} as $as, value is not an object")
  }

  def asString(value: JsValue, as: String): String = value match {
    case v: JsString => v.value
    case _ => deserializationError(s"Can't read ${value.prettyPrint} as $as, value is not a string")
  }
}
