// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.digitalasset.canton.topology.SynchronizerId
import scalaz.{@@, Tag}
import spray.json.{JsNumber, JsString, JsValue, JsonFormat, deserializationError}

import java.time.Instant

trait ExtraFormats {

  def taggedJsonFormat[A: JsonFormat, T]: JsonFormat[A @@ T] = Tag.subst(implicitly[JsonFormat[A]])

  implicit val InstantFormat: JsonFormat[java.time.Instant] = new JsonFormat[Instant] {
    override def write(obj: Instant): JsValue = JsNumber(obj.toEpochMilli)

    override def read(json: JsValue): Instant = json match {
      case JsNumber(a) => java.time.Instant.ofEpochMilli(a.toLongExact)
      case _ => deserializationError("java.time.Instant must be epoch millis")
    }
  }

  implicit val synchronizerIdFormat: JsonFormat[SynchronizerId] = new JsonFormat[SynchronizerId] {
    override def write(obj: SynchronizerId): JsValue = JsString(obj.toProtoPrimitive)

    override def read(json: JsValue): SynchronizerId = json match {
      case JsString(stringSynchronizerId) =>
        SynchronizerId.fromString(stringSynchronizerId) match {
          case Left(err) => deserializationError(err)
          case Right(synchronizerId) => synchronizerId
        }
      case _ => deserializationError("Synchronizer id must be a string")
    }
  }
}
