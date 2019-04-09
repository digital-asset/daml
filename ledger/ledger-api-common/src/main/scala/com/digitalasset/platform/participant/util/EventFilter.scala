// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.participant.util

import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.{Identifier, Party, TransactionFilter}
import com.digitalasset.ledger.api.v1.event.Event.Event.{Archived, Created}
import com.digitalasset.ledger.api.v1.event._
import com.digitalasset.ledger.api.v1.value

import scala.collection.mutable.ArrayBuffer
import scala.collection.{breakOut, immutable}

object EventFilter {

  /**
    * Creates a filter which lets only such events through, where the template id is equal to the given one
    * and the interested party is affected.
    **/
  def byTemplates(transactionFilter: TransactionFilter): TemplateAwareFilter =
    TemplateAwareFilter(transactionFilter)

  final case class TemplateAwareFilter(transactionFilter: TransactionFilter) {

    def isSubmitterSubscriber(submitterParty: String): Boolean =
      transactionFilter.filtersByParty.contains(Party(submitterParty))

    private val subscribersByTemplateId: Map[Identifier, Set[Party]] = {
      val (specificSubscriptions, globalSubscriptions) = getSpecificAndGlobalSubscriptions(
        transactionFilter)
      specificSubscriptions
        .groupBy(_._1)
        .map { // Intentionally not using .mapValues to fully materialize the map
          case (templateId, pairs) =>
            val setOfParties: Set[Party] = pairs.map(_._2)(breakOut)
            templateId -> (setOfParties union globalSubscriptions)
        }
        .withDefaultValue(globalSubscriptions)
    }

    def filterEvent(event: Event): Option[Event] = {
      val servedEvent = event.event match {
        case Created(CreatedEvent(_, _, Some(templateId), _, _)) =>
          applyRequestingWitnesses(event, templateId)

        case Archived(ArchivedEvent(_, _, Some(templateId), _)) =>
          applyRequestingWitnesses(event, templateId)
        case _ => None
      }
      servedEvent
    }

    private def applyRequestingWitnesses(
        event: Event,
        templateId: value.Identifier): Option[Event] = {
      import com.digitalasset.platform.api.v1.event.EventOps._
      val tid = Identifier(
        domain.PackageId(templateId.packageId),
        templateId.moduleName,
        templateId.entityName)
      val requestingWitnesses = event.witnesses.filter(subscribersByTemplateId(tid))
      if (requestingWitnesses.nonEmpty)
        Some(event.withWitnesses(requestingWitnesses))
      else
        None
    }

  }

  private def getSpecificAndGlobalSubscriptions(
      transactionFilter: TransactionFilter): (ArrayBuffer[(Identifier, Party)], Set[Party]) = {
    val specificSubscriptions = new ArrayBuffer[(Identifier, Party)]
    val globalSubscriptions = immutable.Set.newBuilder[Party]
    transactionFilter.filtersByParty.foreach {
      case (party, filters) =>
        filters.inclusive.fold[Unit] {
          globalSubscriptions += party
        } { inclusive =>
          inclusive.templateIds.foreach { tid =>
            specificSubscriptions += tid -> party
          }
        }
    }
    (specificSubscriptions, globalSubscriptions.result())
  }
}
