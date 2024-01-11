// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.digitalasset.canton.http.domain
import com.daml.ledger.api.{v1 as lav1}
import scalaz.\/
import scalaz.syntax.bitraverse.*
import scalaz.syntax.show.*
import scalaz.syntax.traverse.*
import spray.json.{JsObject, JsValue, JsonWriter}

class DomainJsonEncoder(
    val apiRecordToJsObject: lav1.value.Record => JsonError \/ JsObject,
    val apiValueToJsValue: lav1.value.Value => JsonError \/ JsValue,
) {

  import com.digitalasset.canton.http.util.ErrorOps.*

  def encodeExerciseCommand(
      cmd: domain.ExerciseCommand.OptionalPkg[lav1.value.Value, domain.ContractLocator[
        lav1.value.Value
      ]]
  )(implicit
      ev: JsonWriter[domain.ExerciseCommand.OptionalPkg[JsValue, domain.ContractLocator[JsValue]]]
  ): JsonError \/ JsValue =
    for {
      x <- cmd.bitraverse(
        arg => apiValueToJsValue(arg),
        ref => ref.traverse(a => apiValueToJsValue(a)),
      ): JsonError \/ domain.ExerciseCommand.OptionalPkg[JsValue, domain.ContractLocator[JsValue]]

      y <- SprayJson.encode(x).liftErr(JsonError)

    } yield y

  def encodeCreateCommand[CtId](
      cmd: domain.CreateCommand[lav1.value.Record, CtId]
  )(implicit
      ev: JsonWriter[domain.CreateCommand[JsValue, CtId]]
  ): JsonError \/ JsValue =
    for {
      x <- cmd.traversePayload(
        apiRecordToJsObject(_)
      ): JsonError \/ domain.CreateCommand[JsValue, CtId]
      y <- SprayJson.encode(x).liftErr(JsonError)

    } yield y

  def encodeCreateAndExerciseCommand[CtId, IfceId](
      cmd: domain.CreateAndExerciseCommand[
        lav1.value.Record,
        lav1.value.Value,
        CtId,
        IfceId,
      ]
  )(implicit
      ev: JsonWriter[domain.CreateAndExerciseCommand[JsValue, JsValue, CtId, IfceId]]
  ): JsonError \/ JsValue =
    for {
      jsCmd <- cmd.traversePayloadsAndArgument(apiRecordToJsObject, apiValueToJsValue)
      y <- SprayJson
        .encode(jsCmd: domain.CreateAndExerciseCommand[JsValue, JsValue, CtId, IfceId])
        .liftErr(JsonError)

    } yield y

  object implicits {
    implicit val ApiValueJsonWriter: JsonWriter[lav1.value.Value] = (obj: lav1.value.Value) =>
      apiValueToJsValue(obj).valueOr(e => spray.json.serializationError(e.shows))

    implicit val ApiRecordJsonWriter: JsonWriter[lav1.value.Record] = (obj: lav1.value.Record) =>
      apiRecordToJsObject(obj).valueOr(e => spray.json.serializationError(e.shows))
  }
}
