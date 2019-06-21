// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import spray.json.{DefaultJsonProtocol, RootJsonFormat}
import com.digitalasset.http.domain

object JsonProtocol extends DefaultJsonProtocol {

  implicit val TemplateId: RootJsonFormat[domain.TemplateId] = jsonFormat3(domain.TemplateId)

  implicit val GetActiveContractsRequest: RootJsonFormat[domain.GetActiveContractsRequest] =
    jsonFormat3(domain.GetActiveContractsRequest)
}
