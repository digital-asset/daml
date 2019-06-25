// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import spray.json.{DefaultJsonProtocol, RootJsonFormat}
import com.digitalasset.http.domain

object JsonProtocol extends DefaultJsonProtocol {

  implicit val JwtPayloadFormat: RootJsonFormat[domain.JwtPayload] = jsonFormat3(domain.JwtPayload)

  implicit val TemplateIdFormat: RootJsonFormat[domain.TemplateId] = jsonFormat3(domain.TemplateId)

  implicit val GetActiveContractsRequestFormat: RootJsonFormat[domain.GetActiveContractsRequest] =
    jsonFormat1(domain.GetActiveContractsRequest)
}
