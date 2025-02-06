// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.daml.ledger.api.v2 as lav2
import com.digitalasset.canton.http
import scalaz.\/
import scalaz.syntax.bitraverse.*
import scalaz.syntax.show.*
import scalaz.syntax.traverse.*
import spray.json.{JsObject, JsValue, JsonWriter}

class ApiJsonEncoder(
    val apiRecordToJsObject: lav2.value.Record => JsonError \/ JsObject,
    val apiValueToJsValue: lav2.value.Value => JsonError \/ JsValue,
) {

  import com.digitalasset.canton.http.util.ErrorOps.*

  def encodeExerciseCommand(
      cmd: http.ExerciseCommand.RequiredPkg[lav2.value.Value, http.ContractLocator[
        lav2.value.Value
      ]]
  )(implicit
      ev: JsonWriter[http.ExerciseCommand.RequiredPkg[JsValue, http.ContractLocator[JsValue]]]
  ): JsonError \/ JsValue =
    for {
      x <- cmd.bitraverse(
        arg => apiValueToJsValue(arg),
        ref => ref.traverse(a => apiValueToJsValue(a)),
      ): JsonError \/ http.ExerciseCommand.RequiredPkg[JsValue, http.ContractLocator[JsValue]]

      y <- SprayJson.encode(x).liftErr(JsonError)

    } yield y

  def encodeCreateCommand[CtId](
      cmd: http.CreateCommand[lav2.value.Record, CtId]
  )(implicit
      ev: JsonWriter[http.CreateCommand[JsValue, CtId]]
  ): JsonError \/ JsValue =
    for {
      x <- cmd.traversePayload(
        apiRecordToJsObject(_)
      ): JsonError \/ http.CreateCommand[JsValue, CtId]
      y <- SprayJson.encode(x).liftErr(JsonError)

    } yield y

  def encodeCreateAndExerciseCommand[CtId, IfceId](
      cmd: http.CreateAndExerciseCommand[
        lav2.value.Record,
        lav2.value.Value,
        CtId,
        IfceId,
      ]
  )(implicit
      ev: JsonWriter[http.CreateAndExerciseCommand[JsValue, JsValue, CtId, IfceId]]
  ): JsonError \/ JsValue =
    for {
      jsCmd <- cmd.traversePayloadsAndArgument(apiRecordToJsObject, apiValueToJsValue)
      y <- SprayJson
        .encode(jsCmd: http.CreateAndExerciseCommand[JsValue, JsValue, CtId, IfceId])
        .liftErr(JsonError)

    } yield y

  object implicits {
    implicit val ApiValueJsonWriter: JsonWriter[lav2.value.Value] = (obj: lav2.value.Value) =>
      apiValueToJsValue(obj).valueOr(e => spray.json.serializationError(e.shows))

    implicit val ApiRecordJsonWriter: JsonWriter[lav2.value.Record] = (obj: lav2.value.Record) =>
      apiRecordToJsObject(obj).valueOr(e => spray.json.serializationError(e.shows))
  }
}
