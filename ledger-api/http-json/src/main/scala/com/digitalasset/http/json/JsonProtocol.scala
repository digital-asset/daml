package com.digitalasset.http.json

import spray.json.{DefaultJsonProtocol, RootJsonFormat}
import com.digitalasset.http.domain

object JsonProtocol extends DefaultJsonProtocol {

  implicit val TemplateId: RootJsonFormat[domain.TemplateId] = jsonFormat3(domain.TemplateId)

  implicit val GetActiveContractsRequest: RootJsonFormat[domain.GetActiveContractsRequest] =
    jsonFormat2(domain.GetActiveContractsRequest)
}
