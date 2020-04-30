// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.lf.data.Ref.{DottedName, Identifier, PackageId, QualifiedName}
import spray.json.DefaultJsonProtocol._
import spray.json.{JsString, JsValue, JsonFormat, deserializationError}

object Request {
  case class TriggerParams(identifier: Identifier, party: String)
  implicit object IdentifierFormat extends JsonFormat[Identifier] {
    def read(value: JsValue) = value match {
      case JsString(s) => {
        val components = s.split(":")
        if (components.length == 3) {
          val parsed = for {
            pkgId <- PackageId.fromString(components(0))
            mod <- DottedName.fromString(components(1))
            entity <- DottedName.fromString(components(2))
          } yield Identifier(pkgId, QualifiedName(mod, entity))
          parsed match {
            case Left(e) => deserializationError(e)
            case Right(id) => id
          }
        } else {
          deserializationError(s"Expected trigger identifier of the form pkgid:mod:name but got $s")
        }
      }
      case _ => deserializationError("Expected trigger identifier of the form pkgid:mod:name")
    }
    def write(id: Identifier) = JsString(s"${id.packageId}:${id.qualifiedName}")
  }

  implicit val triggerParamsFormat = jsonFormat2(TriggerParams)

  case class ListParams(party: String) // May also need an auth token later
  implicit val listParamsFormat = jsonFormat1(ListParams)
}
