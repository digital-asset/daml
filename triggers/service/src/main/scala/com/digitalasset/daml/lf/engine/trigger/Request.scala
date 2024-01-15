// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Identifier
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

  private[this] def subStringFormat[X <: String](f: String => X): JsonFormat[X] =
    new JsonFormat[X] {
      override def write(obj: X): JsValue = StringJsonFormat.write(obj)
      override def read(json: JsValue): X = f(StringJsonFormat.read(json))
    }

  private[trigger] implicit val PartyFormat: JsonFormat[Ref.Party] = subStringFormat(
    Ref.Party.assertFromString
  )

  final case class StartParams(
      triggerName: Identifier,
      party: Ref.Party,
      applicationId: Option[Ref.ApplicationId],
      readAs: Option[List[Ref.Party]],
  )
  object StartParams {
    implicit val applicationIdFormat: JsonFormat[Ref.ApplicationId] = subStringFormat(
      Ref.ApplicationId.assertFromString
    )
    implicit val startParamsFormat: RootJsonFormat[StartParams] = jsonFormat4(StartParams.apply)
  }

  final case class ListParams(party: Ref.Party)
  object ListParams {
    implicit val listParamsFormat: RootJsonFormat[ListParams] = jsonFormat1(ListParams.apply)
  }
}
