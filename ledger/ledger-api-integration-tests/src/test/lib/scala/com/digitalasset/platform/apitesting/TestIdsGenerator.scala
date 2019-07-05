// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import com.digitalasset.platform.PlatformApplications

import scala.util.Random

/**
  * Provides a mechanism to randomize various identifiers used in testing per run of a process.
  *
  * It enables Ledger API Test Tool to be run against a persistent Ledger API endpoint repeatedly by avoiding
  * duplication of identifiers.
  */
class TestIdsGenerator(config: PlatformApplications.Config) {
  lazy val runSuffix = Random.alphanumeric.take(10).mkString
  lazy val runCommandSuffix = if (config.uniqueCommandIdentifiers) "-" + runSuffix else ""
  // Using runCommandSuffix, since there does not seem to be a need to treat workflow ids differently than command ids.
  // Yet lets wrap those identifiers into an appropriate function so we could do it in the future.
  lazy val runWorkflowSuffix = runCommandSuffix
  lazy val runPartySuffix = if (config.uniquePartyIdentifiers) "-" + runSuffix else ""

  def testPartyName(partyText: String) = partyText + runPartySuffix
  def testCommandId(commandId: String) = commandId + runCommandSuffix
  def testWorkflowId(workflowId: String) = workflowId + runWorkflowSuffix

  def untestPartyName(partyText: String) =
    if (config.uniquePartyIdentifiers) partyText.stripSuffix(runPartySuffix) else partyText
  def untestCommandId(commandId: String) =
    if (config.uniqueCommandIdentifiers) commandId.stripSuffix(runCommandSuffix)
    else commandId
  def untestWorkflowId(workflowId: String) = workflowId.stripSuffix(runWorkflowSuffix)
}
