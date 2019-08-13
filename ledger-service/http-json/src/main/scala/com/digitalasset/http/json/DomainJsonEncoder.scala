// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.{Traverse, \/}
import spray.json.{JsObject, JsValue, JsonWriter}

import scala.language.higherKinds

class DomainJsonEncoder(
    apiRecordToJsObject: lav1.value.Record => JsonError \/ JsObject,
    apiValueToJsValue: lav1.value.Value => JsonError \/ JsValue) {

  def encodeR[F[_]](fa: F[lav1.value.Record])(
      implicit ev1: Traverse[F],
      ev2: JsonWriter[F[JsObject]]): JsonError \/ JsObject =
    for {
      a <- encodeUnderlyingRecord(fa)
      b <- SprayJson.encode[F[JsObject]](a)(ev2).leftMap(e => JsonError(e.shows))
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
      b <- SprayJson.encode[F[JsValue]](a)(ev2).leftMap(e => JsonError(e.shows))
    } yield b

  // encode underlying values
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def encodeUnderlyingValue[F[_]: Traverse](fa: F[lav1.value.Value]): JsonError \/ F[JsValue] =
    fa.traverse(a => apiValueToJsValue(a))
}
