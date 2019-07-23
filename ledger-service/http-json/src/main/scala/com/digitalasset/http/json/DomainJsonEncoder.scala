// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.{Traverse, \/}
import spray.json.{JsValue, JsonWriter}

import scala.language.higherKinds

class DomainJsonEncoder(apiValueToJsValue: lav1.value.Value => JsonError \/ JsValue) {
  def encode[F[_]](fa: F[lav1.value.Value])(
      implicit ev1: Traverse[F],
      ev2: JsonWriter[F[JsValue]]): JsonError \/ JsValue =
    for {
      a <- encodeValues(fa)
      b <- SprayJson.toJson[F[JsValue]](a)(ev2).leftMap(e => JsonError(e.shows))
    } yield b

  // encode underlying values
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def encodeValues[F[_]: Traverse](fa: F[lav1.value.Value]): JsonError \/ F[JsValue] =
    fa.traverse(a => apiValueToJsValue(a))
}
