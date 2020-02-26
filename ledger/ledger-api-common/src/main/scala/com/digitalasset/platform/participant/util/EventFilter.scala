// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.participant.util

import com.daml.ledger.participant.state.index.v2.AcsUpdateEvent
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.ledger.api.domain.{InclusiveFilters, TransactionFilter}
import com.digitalasset.ledger.api.v1.event.Event
import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.platform.api.v1.event.EventOps.EventOps

import scala.collection.breakOut

object EventFilter {

  /**
    * Creates a filter which lets only such events through, where the template id is equal to the given one
    * and the interested party is affected.
    **/
  def byTemplates(transactionFilter: TransactionFilter): TemplateAwareFilter =
    TemplateAwareFilter(transactionFilter)

  private def toApiIdentifier(id: Ref.Identifier): Identifier =
    Identifier(id.packageId, id.qualifiedName.module.toString, id.qualifiedName.name.toString)

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.JavaSerializable"))
  final case class TemplateAwareFilter(transactionFilter: TransactionFilter) {

    def isSubmitterSubscriber(submitterParty: Party): Boolean =
      transactionFilter.filtersByParty.contains(submitterParty)

    lazy val (apiSpecificSubscriptions, apiGlobalSubscriptions) = {
      val specific = List.newBuilder[(Identifier, String)]
      val global = Set.newBuilder[String]
      for ((party, filters) <- transactionFilter.filtersByParty) {
        filters.inclusive match {
          case Some(InclusiveFilters(templateIds)) =>
            specific ++= templateIds.map(id => toApiIdentifier(id) -> party)
          case None => global += party
        }
      }
      (specific.result(), global.result())
    }

    lazy val apiSubscribersByTemplateId: Map[Identifier, Set[String]] = {
      apiSpecificSubscriptions
        .groupBy(_._1)
        .map { // Intentionally not using .mapValues to fully materialize the map
          case (templateId, pairs) =>
            val setOfParties: Set[String] = pairs.map(_._2)(breakOut)
            templateId -> (setOfParties union apiGlobalSubscriptions)
        }
        .withDefaultValue(apiGlobalSubscriptions)
    }

    lazy val (specificSubscriptions, globalSubscriptions) = {
      val specific = List.newBuilder[(Ref.Identifier, Party)]
      val global = Set.newBuilder[Party]
      for ((party, filters) <- transactionFilter.filtersByParty) {
        filters.inclusive match {
          case Some(InclusiveFilters(templateIds)) => specific ++= templateIds.map(_ -> party)
          case None => global += party
        }
      }
      (specific.result(), global.result())
    }

    lazy val subscribersByTemplateId: Map[Ref.Identifier, Set[Party]] = {
      specificSubscriptions
        .groupBy(_._1)
        .map { // Intentionally not using .mapValues to fully materialize the map
          case (templateId, pairs) =>
            val setOfParties: Set[Party] = pairs.map(_._2)(breakOut)
            templateId -> (setOfParties union globalSubscriptions)
        }
        .withDefaultValue(globalSubscriptions)
    }
  }

  def filterEventWitnesses(filter: TemplateAwareFilter, event: Event): Option[Event] = {
    val requestingWitnesses =
      event.witnessParties.filter(filter.apiSubscribersByTemplateId(event.templateId))
    if (requestingWitnesses.nonEmpty)
      Some(event.witnessParties(requestingWitnesses))
    else
      None
  }

  def filterActiveContractWitnesses(
      filter: TemplateAwareFilter,
      ac: AcsUpdateEvent.Create): Option[AcsUpdateEvent.Create] = {
    val requestingWitnesses =
      ac.stakeholders.filter(e =>
        filter.subscribersByTemplateId(ac.templateId).contains(Party.assertFromString(e)))
    if (requestingWitnesses.nonEmpty)
      Some(ac.copy(stakeholders = requestingWitnesses))
    else
      None
  }
}
