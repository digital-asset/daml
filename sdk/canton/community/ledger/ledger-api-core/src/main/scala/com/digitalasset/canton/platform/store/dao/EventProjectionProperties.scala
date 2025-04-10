// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.ledger.api.{CumulativeFilter, EventFormat, TemplateWildcardFilter}
import com.digitalasset.canton.platform.index.IndexServiceImpl.InterfaceViewPackageUpgrade
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties.Projection
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.*
import com.google.common.annotations.VisibleForTesting

import scala.collection.View
import scala.concurrent.Future

/** This class encapsulates the logic of how contract arguments and interface views are being
  * projected to the consumer based on the filter criteria and the relation between interfaces and
  * templates implementing them.
  *
  * @param verbose
  *   enriching in verbose mode
  * @param templateWildcardWitnesses
  *   all the parties for which contract arguments will be populated for all the templates,if None
  *   then contract arguments for all the parties and for all the templates will be populated
  * @param witnessTemplateProjections
  *   per witness party, per template projections
  * @param templateWildcardCreatedEventBlobParties
  *   parties for which the created event blob will be populated for all the templates, if None then
  *   blobs for all the parties and all the templates will be populated
  * @param interfaceViewPackageUpgrade
  *   computes which interface instance version should be used for rendering an interface view for a
  *   given interface instance
  */
final case class EventProjectionProperties(
    verbose: Boolean,
    templateWildcardWitnesses: Option[Set[String]],
    // Map((witness or wildcard) -> Map(template -> projection)), where a None key denotes a party wildcard
    witnessTemplateProjections: Map[Option[String], Map[Identifier, Projection]] = Map.empty,
    templateWildcardCreatedEventBlobParties: Option[Set[String]] = Some(
      Set.empty
    ), // TODO(#19364) fuse with the templateWildcardWitnesses into a templateWildcard Projection and potentially include it into the following Map
)(
    // Note: including this field in a separate argument list to the case class to not affect the deep equality check of the
    //       regular argument list
    val interfaceViewPackageUpgrade: InterfaceViewPackageUpgrade
) {
  def render(witnesses: Set[String], templateId: Identifier): Projection =
    (witnesses.iterator.map(Some(_))
      ++ Iterator(None)) // for the party-wildcard template specific projections)
      .flatMap(witnessTemplateProjections.get(_).iterator)
      .flatMap(_.get(templateId).iterator)
      .foldLeft(
        Projection(
          contractArguments = templateWildcardWitnesses.fold(witnesses.nonEmpty)(parties =>
            witnesses.exists(parties)
          ),
          createdEventBlob = templateWildcardCreatedEventBlobParties
            .fold(witnesses.nonEmpty)(parties => witnesses.exists(parties)),
        )
      )(_ append _)
}

object EventProjectionProperties {

  final case class Projection(
      interfaces: Set[Identifier] = Set.empty,
      createdEventBlob: Boolean = false,
      contractArguments: Boolean = false,
  ) {
    def append(other: Projection): Projection =
      Projection(
        interfaces ++ other.interfaces,
        createdEventBlob || other.createdEventBlob,
        contractArguments || other.contractArguments,
      )
  }

  /** @param eventFormat
    *   EventFormat as defined by the consumer of the API.
    * @param interfaceImplementedBy
    *   The relation between an interface id and template id. If template has no relation to the
    *   interface, an empty Set must be returned.
    * @param alwaysPopulateArguments
    *   If this flag is set, the witnessTemplate filter will be populated with all the parties, so
    *   that rendering of contract arguments and contract keys is always true.
    */
  def apply(
      eventFormat: EventFormat,
      interfaceImplementedBy: Identifier => Set[Identifier],
      resolveTypeConRef: TypeConRef => Set[Identifier],
      interfaceViewPackageUpgrade: InterfaceViewPackageUpgrade,
      alwaysPopulateArguments: Boolean,
  ): EventProjectionProperties =
    EventProjectionProperties(
      verbose = eventFormat.verbose,
      templateWildcardWitnesses = templateWildcardWitnesses(eventFormat, alwaysPopulateArguments),
      templateWildcardCreatedEventBlobParties =
        templateWildcardCreatedEventBlobParties(eventFormat),
      witnessTemplateProjections = witnessTemplateProjections(
        eventFormat,
        interfaceImplementedBy,
        resolveTypeConRef,
      ),
    )(
      interfaceViewPackageUpgrade = interfaceViewPackageUpgrade
    )

  @VisibleForTesting
  val UseOriginalViewPackageId: InterfaceViewPackageUpgrade =
    (_: Ref.ValueRef, originalTemplateImplementation: Ref.ValueRef) =>
      Future.successful(originalTemplateImplementation)

  @VisibleForTesting
  def apply(
      eventFormat: EventFormat,
      interfaceImplementedBy: Identifier => Set[Identifier],
      resolveTypeConRef: TypeConRef => Set[Identifier],
      alwaysPopulateArguments: Boolean,
  ): EventProjectionProperties =
    EventProjectionProperties(
      eventFormat = eventFormat,
      interfaceImplementedBy = interfaceImplementedBy,
      resolveTypeConRef = resolveTypeConRef,
      alwaysPopulateArguments = alwaysPopulateArguments,
      interfaceViewPackageUpgrade = (_: Ref.ValueRef, _: Ref.ValueRef) =>
        Future.failed(
          new UnsupportedOperationException("Not expected to be called in unit tests")
        ),
    )

  private def templateWildcardWitnesses(
      apiTransactionFilter: EventFormat,
      alwaysPopulateArguments: Boolean,
  ): Option[Set[String]] =
    if (alwaysPopulateArguments) {
      apiTransactionFilter.filtersForAnyParty match {
        case Some(_) => None
        // filters for any party (party-wildcard) is not defined, getting the template wildcard witnesses from the filters by party
        case None =>
          Some(
            apiTransactionFilter.filtersByParty.keysIterator.toSet
          )
      }
    } else
      apiTransactionFilter.filtersForAnyParty match {
        case Some(cumulative) if cumulative.templateWildcardFilter.isDefined => None
        // filters for any party (party-wildcard) not defined at all or defined but for specific templates, getting the template wildcard witnesses from the filters by party
        case _ =>
          Some(
            apiTransactionFilter.filtersByParty.iterator.collect {
              case (party, cumulative) if cumulative.templateWildcardFilter.isDefined =>
                party
            }.toSet
          )
      }

  private def templateWildcardCreatedEventBlobParties(
      apiTransactionFilter: EventFormat
  ): Option[Set[String]] =
    apiTransactionFilter.filtersForAnyParty match {
      case Some(CumulativeFilter(_, _, Some(TemplateWildcardFilter(true)))) =>
        None // include blobs for all templates and all parties
      // filters for any party (party-wildcard) not defined at all or defined but for specific templates, getting the template wildcard witnesses from the filters by party
      case _ =>
        Some(
          apiTransactionFilter.filtersByParty.iterator
            .collect {
              case (
                    party,
                    CumulativeFilter(_, _, Some(TemplateWildcardFilter(true))),
                  ) =>
                party
            }
            .map(_.toString)
            .toSet
        )
    }

  private def witnessTemplateProjections(
      apiEventFormat: EventFormat,
      interfaceImplementedBy: Identifier => Set[Identifier],
      resolveTypeConRef: TypeConRef => Set[Identifier],
  ): Map[Option[String], Map[Identifier, Projection]] = {
    val partyFilterPairs =
      apiEventFormat.filtersByParty.view.map { case (p, f) =>
        (Some(p), f)
      } ++
        apiEventFormat.filtersForAnyParty.toList.view.map((None, _))
    (for {
      (partyO, cumulativeFilter) <- partyFilterPairs
    } yield {
      val interfaceFilterProjections = for {
        interfaceFilter <- cumulativeFilter.interfaceFilters.view
        interfaceId <- resolveTypeConRef(interfaceFilter.interfaceTypeRef)
        implementor <- interfaceImplementedBy(interfaceId).view
      } yield implementor -> Projection(
        interfaces = if (interfaceFilter.includeView) Set(interfaceId) else Set.empty,
        createdEventBlob = interfaceFilter.includeCreatedEventBlob,
        contractArguments = false,
      )
      val templateProjections = getTemplateProjections(cumulativeFilter, resolveTypeConRef)
      val projectionsForParty =
        (interfaceFilterProjections ++ templateProjections)
          .groupMap(_._1)(_._2)
          .view
          .mapValues(_.foldLeft(Projection())(_ append _))
          .toMap

      partyO -> projectionsForParty
    }).toMap
  }

  private def getTemplateProjections(
      cumulativeFilter: CumulativeFilter,
      resolveTypeConRef: TypeConRef => Set[Identifier],
  ): View[(Identifier, Projection)] =
    for {
      templateFilter <- cumulativeFilter.templateFilters.view
      templateId <- resolveTypeConRef(templateFilter.templateTypeRef).view
    } yield templateId -> Projection(
      interfaces = Set.empty,
      createdEventBlob = templateFilter.includeCreatedEventBlob,
      contractArguments = true,
    )
}
