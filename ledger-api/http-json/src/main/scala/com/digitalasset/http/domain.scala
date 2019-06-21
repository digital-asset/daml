package com.digitalasset.http

object domain {

  case class TemplateId(packageId: Option[String], moduleName: String, entityName: String)

  case class GetActiveContractsRequest(ledgerId: Option[String], filter: Map[String, TemplateId])
}
