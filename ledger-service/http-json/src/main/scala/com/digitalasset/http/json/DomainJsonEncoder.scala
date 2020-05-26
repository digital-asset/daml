// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.json

import com.daml.http.domain
import com.daml.ledger.api.{v1 => lav1}
import scalaz.\/
import scalaz.syntax.bitraverse._
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import spray.json.{JsObject, JsValue, JsonWriter}

class DomainJsonEncoder(
    val apiRecordToJsObject: lav1.value.Record => JsonError \/ JsObject,
    val apiValueToJsValue: lav1.value.Value => JsonError \/ JsValue
) {

  import com.daml.http.util.ErrorOps._

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def encodeExerciseCommand(
      cmd: domain.ExerciseCommand[lav1.value.Value, domain.ContractLocator[lav1.value.Value]])(
      implicit ev: JsonWriter[domain.ExerciseCommand[JsValue, domain.ContractLocator[JsValue]]])
    : JsonError \/ JsValue =
    for {
      x <- cmd.bitraverse(
        arg => apiValueToJsValue(arg),
        ref => ref.traverse(a => apiValueToJsValue(a))
      ): JsonError \/ domain.ExerciseCommand[JsValue, domain.ContractLocator[JsValue]]

      y <- SprayJson.encode(x).liftErr(JsonError)

    } yield y

  object implicits {
    implicit val ApiValueJsonWriter: JsonWriter[lav1.value.Value] = (obj: lav1.value.Value) =>
      apiValueToJsValue(obj).valueOr(e => spray.json.serializationError(e.shows))

    implicit val ApiRecordJsonWriter: JsonWriter[lav1.value.Record] = (obj: lav1.value.Record) =>
      apiRecordToJsObject(obj).valueOr(e => spray.json.serializationError(e.shows))
  }
}
