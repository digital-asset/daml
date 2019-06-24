package com.digitalasset.platform.apitesting

import scala.util.Random

trait TestIdsGenerator {
  self: MultiLedgerFixture =>

  lazy val runSuffix = Random.alphanumeric.take(10).mkString
  lazy val runCommandSuffix = if (config.uniqueCommandIdentifiers) "-" + runSuffix else ""
  lazy val runPartySuffix = if (config.uniquePartyIdentifiers) "-" + runSuffix else ""
  def partyNameUnifier(partyText: String) = partyText + runPartySuffix
  def commandIdUnifier(commandId: String) = commandId + runCommandSuffix
}
