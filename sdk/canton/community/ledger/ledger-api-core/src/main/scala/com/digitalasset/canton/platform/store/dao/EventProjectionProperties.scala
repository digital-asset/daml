// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import cats.syntax.option.*
import com.digitalasset.canton.ledger.api.{CumulativeFilter, EventFormat}
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
  * @param witnessTemplateProjections
  *   per witness party, per template projections
  * @param interfaceViewPackageUpgrade
  *   computes which interface instance version should be used for rendering an interface view for a
  *   given interface instance
  */
final case class EventProjectionProperties(
    verbose: Boolean,
    // Map((witness or wildcard) -> Map(template -> projection)), where a None key denotes a party wildcard
    witnessTemplateProjections: Map[Option[String], Map[Option[Ref.NameTypeConRef], Projection]] =
      Map.empty,
)(
    // Note: including this field in a separate argument list to the case class to not affect the deep equality check of the
    //       regular argument list
    val interfaceViewPackageUpgrade: InterfaceViewPackageUpgrade
) {
  def render(witnesses: Set[String], templateId: NameTypeConRef): Projection = {
    require(witnesses.nonEmpty)
    (witnesses.iterator.map(Some(_))
      ++ Iterator(None)) // for the party-wildcard template specific projections)
      .flatMap(witnessTemplateProjections.get(_).iterator)
      .flatMap(templateMap => Iterator(Some(templateId), None).flatMap(templateMap.get))
      .foldLeft(
        Projection()
      )(_ append _)
  }
}

object EventProjectionProperties {

  final case class Projection(
      interfaces: Set[FullIdentifier] = Set.empty,
      createdEventBlob: Boolean = false,
  ) {
    def append(other: Projection): Projection =
      Projection(
        interfaces = interfaces ++ other.interfaces,
        createdEventBlob = createdEventBlob || other.createdEventBlob,
      )
  }

  /** @param eventFormat
    *   EventFormat as defined by the consumer of the API.
    * @param interfaceImplementedBy
    *   The relation between an interface id and template id. If template has no relation to the
    *   interface, an empty Set must be returned.
    */
  def apply(
      eventFormat: EventFormat,
      interfaceImplementedBy: FullIdentifier => Set[FullIdentifier],
      resolveTypeConRef: TypeConRef => Set[FullIdentifier],
      interfaceViewPackageUpgrade: InterfaceViewPackageUpgrade,
  ): EventProjectionProperties =
    EventProjectionProperties(
      verbose = eventFormat.verbose,
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
      Future.successful(Right(originalTemplateImplementation))

  @VisibleForTesting
  def apply(
      eventFormat: EventFormat,
      interfaceImplementedBy: FullIdentifier => Set[FullIdentifier],
      resolveTypeConRef: TypeConRef => Set[FullIdentifier],
  ): EventProjectionProperties =
    EventProjectionProperties(
      eventFormat = eventFormat,
      interfaceImplementedBy = interfaceImplementedBy,
      resolveTypeConRef = resolveTypeConRef,
      interfaceViewPackageUpgrade = (_: Ref.ValueRef, _: Ref.ValueRef) =>
        Future.failed(
          new UnsupportedOperationException("Not expected to be called in unit tests")
        ),
    )

  private def witnessTemplateProjections(
      apiEventFormat: EventFormat,
      interfaceImplementedBy: FullIdentifier => Set[FullIdentifier],
      resolveTypeConRef: TypeConRef => Set[FullIdentifier],
  ): Map[Option[String], Map[Option[NameTypeConRef], Projection]] = {
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
      } yield implementor.toNameTypeConRef -> Projection(
        interfaces = if (interfaceFilter.includeView) Set(interfaceId) else Set.empty,
        createdEventBlob = interfaceFilter.includeCreatedEventBlob,
      )
      val templateProjections = getTemplateProjections(cumulativeFilter, resolveTypeConRef)
      val wildcardTemplateProjectionsForParty =
        if (cumulativeFilter.templateWildcardFilter.exists(_.includeCreatedEventBlob))
          Map(None -> Projection(createdEventBlob = true))
        else Map.empty
      val projectionsForParty =
        (interfaceFilterProjections ++ templateProjections)
          .groupMap(t => t._1.some)(_._2)
          .view
          .mapValues(_.foldLeft(Projection())(_ append _))
          .toMap

      partyO -> (projectionsForParty ++ wildcardTemplateProjectionsForParty)
    }).toMap
  }

  private def getTemplateProjections(
      cumulativeFilter: CumulativeFilter,
      resolveTypeConRef: TypeConRef => Set[FullIdentifier],
  ): View[(NameTypeConRef, Projection)] =
    for {
      templateFilter <- cumulativeFilter.templateFilters.view
      templateId <- resolveTypeConRef(templateFilter.templateTypeRef).view
    } yield templateId.toNameTypeConRef -> Projection(
      interfaces = Set.empty,
      createdEventBlob = templateFilter.includeCreatedEventBlob,
    )
}
