// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.admin.api.client.commands.LedgerApiTypeWrappers
import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.config
import com.digitalasset.canton.config.ConsoleCommandTimeout
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.examples.java.trailingnone.TrailingNone
import com.digitalasset.canton.topology.PartyId

/** Adds the ability to create TrailingNone instances to integration tests.
  */
trait HasTrailingNoneUtils[TCE <: TestConsoleEnvironment] {
  this: BaseIntegrationTest[TCE] =>

  def createTrailingNoneContract(
      participant: ParticipantReference,
      partyId: PartyId,
      commandId: String = "",
      optTimeout: Option[config.NonNegativeDuration] = Some(
        ConsoleCommandTimeout.defaultLedgerCommandsTimeout
      ),
  ): Unit = {
    if (participant.packages.find_by_module("TrailingNone").isEmpty) {
      participant.dars.upload(CantonExamplesPath)
    }
    val cmd =
      TrailingNone.create(partyId.toProtoPrimitive, java.util.Optional.empty()).commands.loneElement
    participant.ledger_api.javaapi.commands
      .submit(Seq(partyId), Seq(cmd), commandId = commandId, optTimeout = optTimeout)
  }

  def trailingNoneAcsWithBlobs(
      participant: ParticipantReference,
      party: PartyId,
  ): Seq[LedgerApiTypeWrappers.WrappedContractEntry] =
    participant.ledger_api.state.acs
      .of_party(
        party,
        filterTemplates = Seq(TemplateId.fromJavaIdentifier(TrailingNone.TEMPLATE_ID)),
        includeCreatedEventBlob = true,
        verbose = false,
      )
}
