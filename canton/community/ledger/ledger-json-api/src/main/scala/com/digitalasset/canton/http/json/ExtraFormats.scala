// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.digitalasset.canton.topology.DomainId

import java.time.Instant
import scalaz.{@@, Tag}
import spray.json.{JsNumber, JsString, JsValue, JsonFormat, deserializationError}

trait ExtraFormats {

  def taggedJsonFormat[A: JsonFormat, T]: JsonFormat[A @@ T] = Tag.subst(implicitly[JsonFormat[A]])

  implicit val InstantFormat: JsonFormat[java.time.Instant] = new JsonFormat[Instant] {
    override def write(obj: Instant): JsValue = JsNumber(obj.toEpochMilli)

    override def read(json: JsValue): Instant = json match {
      case JsNumber(a) => java.time.Instant.ofEpochMilli(a.toLongExact)
      case _ => deserializationError("java.time.Instant must be epoch millis")
    }
  }

  implicit val domainIdFormat: JsonFormat[DomainId] = new JsonFormat[DomainId] {
    override def write(obj: DomainId): JsValue = JsString(obj.toProtoPrimitive)

    override def read(json: JsValue): DomainId = json match {
      case JsString(stringDomainId) =>
        DomainId.fromString(stringDomainId) match {
          case Left(err) => deserializationError(err)
          case Right(domainId) => domainId
        }
      case _ => deserializationError("Domain ID must be a string")
    }
  }
}
