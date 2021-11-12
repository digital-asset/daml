// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.WorkflowConfig.StreamConfig
import com.daml.ledger.api.benchtool.submission.CommandSubmitter
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.test.model.Foo.{Foo1, Foo2, Foo3}
import com.daml.ledger.client.binding.Primitive
import scalaz.syntax.tag._

object DescriptorConverter {

  def streamDescriptorToConfig(
      descriptor: StreamDescriptor,
      submissionSummary: Option[CommandSubmitter.SubmissionSummary],
  ): StreamConfig = {
    def identifierToFullyQualifiedString(id: Identifier) =
      s"${id.packageId}:${id.moduleName}:${id.entityName}"
    def fullyQualifiedTemplateId(template: String): String =
      template match {
        case "Foo1" => identifierToFullyQualifiedString(Foo1.id.unwrap)
        case "Foo2" => identifierToFullyQualifiedString(Foo2.id.unwrap)
        case "Foo3" => identifierToFullyQualifiedString(Foo3.id.unwrap)
        case other => other
      }

    def convertedParty(party: String): String =
      submissionSummary match {
        case None => party
        case Some(summary) => partyFromObservers(party, summary.observers)
      }

    def partyFromObservers(party: String, observers: List[Primitive.Party]): String =
      observers
        .map(_.unwrap)
        .find(_.contains(party))
        .getOrElse(throw new RuntimeException(s"Observer not found: $party"))

    val filters = descriptor.filters.map { filter =>
      WorkflowConfig.StreamConfig.PartyFilter(
        party = convertedParty(filter.party),
        templates = filter.templates.map(fullyQualifiedTemplateId),
      )
    }

    descriptor.streamType match {
      case StreamDescriptor.StreamType.ActiveContracts =>
        WorkflowConfig.StreamConfig.ActiveContractsStreamConfig(
          name = descriptor.name,
          filters = filters,
        )
      case invalid =>
        throw new RuntimeException(s"Invalid stream type: $invalid")
    }
  }

}
