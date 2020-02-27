// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import com.digitalasset.http.domain
import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.syntax.bitraverse._
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.{Traverse, \/}
import spray.json.{JsObject, JsValue, JsonWriter}

import scala.language.higherKinds

class DomainJsonEncoder(
    val apiRecordToJsObject: lav1.value.Record => JsonError \/ JsObject,
    val apiValueToJsValue: lav1.value.Value => JsonError \/ JsValue) {

  import com.digitalasset.http.util.ErrorOps._

  def encodeR[F[_]](fa: F[lav1.value.Record])(
      implicit ev1: Traverse[F],
      ev2: JsonWriter[F[JsObject]]): JsonError \/ JsObject =
    for {
      a <- encodeUnderlyingRecord(fa)
      b <- SprayJson.encode[F[JsObject]](a)(ev2).liftErr(JsonError)
      c <- SprayJson.mustBeJsObject(b)
    } yield c

  // encode underlying values
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def encodeUnderlyingRecord[F[_]: Traverse](fa: F[lav1.value.Record]): JsonError \/ F[JsObject] =
    fa.traverse(a => apiRecordToJsObject(a))

  def encodeV[F[_]](fa: F[lav1.value.Value])(
      implicit ev1: Traverse[F],
      ev2: JsonWriter[F[JsValue]]): JsonError \/ JsValue =
    for {
      a <- encodeUnderlyingValue(fa)
      b <- SprayJson.encode[F[JsValue]](a)(ev2).liftErr(JsonError)
    } yield b

  // encode underlying values
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def encodeUnderlyingValue[F[_]: Traverse](fa: F[lav1.value.Value]): JsonError \/ F[JsValue] =
    fa.traverse(a => apiValueToJsValue(a))

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def encodeExerciseCommand(
      cmd: domain.ExerciseCommand[lav1.value.Value, domain.ContractLocator[lav1.value.Value]])(
      implicit ev: JsonWriter[domain.ExerciseCommand[JsValue, domain.ContractLocator[JsValue]]])
    : JsonError \/ JsValue =
    for {
      x <- cmd.bitraverse(
        arg => apiValueToJsValue(arg),
        ref => encodeContractLocatorUnderlyingValue(ref)
      )

      y <- SprayJson.encode(x).liftErr(JsonError)

    } yield y

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def encodeContractLocatorUnderlyingValue(
      fa: domain.ContractLocator[lav1.value.Value]): JsonError \/ domain.ContractLocator[JsValue] =
    fa.traverse(a => apiValueToJsValue(a))

  // TODO(Leo) see if you can get get rid of the above boilerplate and rely on the JsonWriters defined below
  object implicits {
    implicit val ApiValueJsonWriter: JsonWriter[lav1.value.Value] = (obj: lav1.value.Value) =>
      apiValueToJsValue(obj).valueOr(e => spray.json.serializationError(e.shows))

    implicit val ApiRecordJsonWriter: JsonWriter[lav1.value.Record] = (obj: lav1.value.Record) =>
      apiRecordToJsObject(obj).valueOr(e => spray.json.serializationError(e.shows))
  }
}
