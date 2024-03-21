// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.lf.data.Ref.Identifier
import scalaz.Tag
import spray.json.DefaultJsonProtocol._
import spray.json.{JsString, JsValue, JsonFormat, RootJsonFormat, deserializationError}

object Request {
  implicit object IdentifierFormat extends JsonFormat[Identifier] {
    def read(value: JsValue): Identifier = value match {
      case JsString(s) =>
        Identifier.fromString(s).fold(deserializationError(_), identity)
      case _ => deserializationError("Expected trigger identifier of the form pkgid:mod:name")
    }
    def write(id: Identifier): JsValue = JsString(id.toString)
  }

  private[trigger] implicit val PartyFormat: JsonFormat[Party] =
    Tag.subst(implicitly[JsonFormat[String]])

  final case class StartParams(
      triggerName: Identifier,
      party: Party,
      applicationId: Option[ApplicationId],
      readAs: Option[List[Party]],
  )
  object StartParams {
    implicit val applicationIdFormat: JsonFormat[ApplicationId] =
      Tag.subst(implicitly[JsonFormat[String]])
    implicit val startParamsFormat: RootJsonFormat[StartParams] = jsonFormat4(StartParams.apply)
  }

  final case class ListParams(party: Party)
  object ListParams {
    implicit val listParamsFormat: RootJsonFormat[ListParams] = jsonFormat1(ListParams.apply)
  }
}
