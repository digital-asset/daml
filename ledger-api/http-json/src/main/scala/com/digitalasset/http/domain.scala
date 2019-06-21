package com.digitalasset.http

object domain {

  case class JwtPayload(party: String)

  case class TemplateId(packageId: Option[String], moduleName: String, entityName: String)

  case class GetActiveContractsRequest(
      ledgerId: Option[String],
      party: Option[String],
      templateIds: List[TemplateId])
}
