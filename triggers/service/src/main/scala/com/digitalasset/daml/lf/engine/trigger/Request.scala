// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.lf.data.Ref.{DottedName, Identifier, PackageId, QualifiedName}
import scalaz.Tag
import spray.json.DefaultJsonProtocol._
import spray.json.{JsString, JsValue, JsonFormat, deserializationError}

object Request {
  implicit object IdentifierFormat extends JsonFormat[Identifier] {
    def read(value: JsValue): Identifier = value match {
      case JsString(s) =>
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
      case _ => deserializationError("Expected trigger identifier of the form pkgid:mod:name")
    }
    def write(id: Identifier): JsValue = JsString(id.toString)
  }

  implicit val PartyFormat: JsonFormat[Party] = Tag.subst(implicitly[JsonFormat[String]])

  case class StartParams(triggerName: Identifier, party: Party)
  implicit val startParamsFormat = jsonFormat2(StartParams)
  case class ListParams(party: Party)
  implicit val listParamsFormat = jsonFormat1(ListParams)
}
