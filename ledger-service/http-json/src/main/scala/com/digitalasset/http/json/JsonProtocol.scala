// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import spray.json._
import com.digitalasset.http.domain

object JsonProtocol extends DefaultJsonProtocol {

  implicit val JwtPayloadFormat: RootJsonFormat[domain.JwtPayload] = jsonFormat3(domain.JwtPayload)

  implicit def TemplateIdFormat[A: JsonFormat]: RootJsonFormat[domain.TemplateId[A]] =
    jsonFormat3(domain.TemplateId.apply[A])

  implicit val GetActiveContractsRequestFormat: RootJsonFormat[domain.GetActiveContractsRequest] =
    jsonFormat1(domain.GetActiveContractsRequest)

  // sigh @ induction
  implicit def SeqJsonWriter[A: JsonWriter]: JsonWriter[Seq[A]] =
    as => JsArray(as.iterator.map(_.toJson).toVector)

  implicit val GetActiveContractsResponseFormat
    : JsonWriter[domain.GetActiveContractsResponse[JsValue]] =
    gacr => JsString(gacr.toString) // TODO actual format
}
