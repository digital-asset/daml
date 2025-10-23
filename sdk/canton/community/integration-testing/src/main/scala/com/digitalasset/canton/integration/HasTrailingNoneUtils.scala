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

import scala.jdk.CollectionConverters.MapHasAsScala

/** Adds the ability to create TrailingNone instances to integration tests.
  */
trait HasTrailingNoneUtils {
  this: BaseIntegrationTest =>

  def createTrailingNoneContract(
      participant: ParticipantReference,
      partyId: PartyId,
      commandId: String = "",
      optTimeout: Option[config.NonNegativeDuration] = Some(
        ConsoleCommandTimeout.defaultLedgerCommandsTimeout
      ),
  ): LedgerApiTypeWrappers.WrappedContractEntry = {
    if (participant.packages.find_by_module("TrailingNone").isEmpty) {
      participant.dars.upload(CantonExamplesPath)
    }
    val cmd =
      TrailingNone.create(partyId.toProtoPrimitive, java.util.Optional.empty()).commands.loneElement
    val contractId = participant.ledger_api.javaapi.commands
      .submit(Seq(partyId), Seq(cmd), commandId = commandId, optTimeout = optTimeout)
      .getEventsById
      .asScala
      .headOption
      .value
      ._2
      .getContractId
    trailingNoneAcsWithBlobs(participant, partyId)
      .find(_.contractId == contractId)
      .value
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
