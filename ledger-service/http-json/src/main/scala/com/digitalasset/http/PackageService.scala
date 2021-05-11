// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref
import com.daml.lf.iface
import com.daml.http.domain.{Choice, TemplateId}
import com.daml.http.util.IdentifierConverters
import com.daml.http.util.Logging.{CorrelationID}
import com.daml.ledger.service.LedgerReader.PackageStore
import com.daml.ledger.service.{LedgerReader, TemplateIds}
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import scalaz.Scalaz._
import scalaz._

import scala.collection.compat._
import scala.concurrent.{ExecutionContext, Future}

private class PackageService(reloadPackageStoreIfChanged: PackageService.ReloadPackageStore) {

  private[this] val logger = ContextualizedLogger.get(getClass)

  import PackageService._

  private case class State(
      packageIds: Set[String],
      templateIdMap: TemplateIdMap,
      choiceTypeMap: ChoiceTypeMap,
      keyTypeMap: KeyTypeMap,
      packageStore: PackageStore,
  ) {

    def append(diff: PackageStore): State = {
      val newPackageStore = this.packageStore ++ diff
      State(
        newPackageStore.keySet,
        getTemplateIdMap(newPackageStore),
        getChoiceTypeMap(newPackageStore),
        getKeyTypeMap(newPackageStore),
        newPackageStore,
      )
    }
  }

  // volatile, reading threads don't need synchronization
  @volatile private var state: State =
    State(Set.empty, TemplateIdMap.Empty, Map.empty, Map.empty, Map.empty)

  // synchronized, so two threads cannot reload it concurrently
  def reload(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[CorrelationID],
  ): Future[Error \/ Unit] =
    synchronized {
      reloadPackageStoreIfChanged(state.packageIds).map {
        _.map {
          case Some(diff) =>
            this.state = this.state.append(diff)
            logger.info(s"new package IDs loaded: ${diff.keySet.mkString(", ")}")
            logger.debug(s"loaded diff: $diff")
            ()
          case None =>
            logger.debug(s"new package IDs not found")
            ()
        }
      }
    }

  def packageStore: PackageStore = state.packageStore

  // Do not reduce it to something like `PackageService.resolveTemplateId(state.templateIdMap)`
  // `state.templateIdMap` will be cached in this case.
  def resolveTemplateId: ResolveTemplateId =
    x => PackageService.resolveTemplateId(state.templateIdMap)(x)

  def resolveTemplateRecordType: ResolveTemplateRecordType =
    templateId =>
      \/-(
        iface
          .TypeCon(iface.TypeConName(IdentifierConverters.lfIdentifier(templateId)), ImmArraySeq())
      )

  def allTemplateIds: AllTemplateIds =
    () => state.templateIdMap.all

  // See the above comment
  def resolveChoiceArgType: ResolveChoiceArgType =
    (x, y) => PackageService.resolveChoiceArgType(state.choiceTypeMap)(x, y)

  // See the above comment
  def resolveKeyType: ResolveKeyType =
    x => PackageService.resolveKey(state.keyTypeMap)(x)
}

object PackageService {
  sealed trait Error
  final case class InputError(message: String) extends Error
  final case class ServerError(message: String) extends Error

  object Error {
    implicit val errorShow: Show[Error] = Show shows {
      case InputError(m) => s"PackageService input error: ${m: String}"
      case ServerError(m) => s"PackageService server error: ${m: String}"
    }
  }

  type ReloadPackageStore =
    Set[String] => Future[PackageService.Error \/ Option[LedgerReader.PackageStore]]

  type ResolveTemplateId =
    TemplateId.OptionalPkg => Option[TemplateId.RequiredPkg]

  type ResolveTemplateRecordType =
    TemplateId.RequiredPkg => Error \/ iface.Type

  type AllTemplateIds =
    () => Set[TemplateId.RequiredPkg]

  type ResolveChoiceArgType =
    (TemplateId.RequiredPkg, Choice) => Error \/ iface.Type

  type ResolveKeyType =
    TemplateId.RequiredPkg => Error \/ iface.Type

  case class TemplateIdMap(
      all: Set[TemplateId.RequiredPkg],
      unique: Map[TemplateId.NoPkg, TemplateId.RequiredPkg],
  )

  object TemplateIdMap {
    val Empty: TemplateIdMap = TemplateIdMap(Set.empty, Map.empty)
  }

  type ChoiceTypeMap = Map[(TemplateId.RequiredPkg, Choice), iface.Type]

  type KeyTypeMap = Map[TemplateId.RequiredPkg, iface.Type]

  def getTemplateIdMap(packageStore: PackageStore): TemplateIdMap =
    buildTemplateIdMap(collectTemplateIds(packageStore))

  private def collectTemplateIds(packageStore: PackageStore): Set[TemplateId.RequiredPkg] =
    TemplateIds
      .getTemplateIds(packageStore.values.toSet)
      .map(x => TemplateId(x.packageId, x.moduleName, x.entityName))

  def buildTemplateIdMap(ids: Set[TemplateId.RequiredPkg]): TemplateIdMap = {
    val all: Set[TemplateId.RequiredPkg] = ids
    val unique: Map[TemplateId.NoPkg, TemplateId.RequiredPkg] = filterUniqueTemplateIs(all)
    TemplateIdMap(all, unique)
  }

  private[http] def key2(k: TemplateId.RequiredPkg): TemplateId.NoPkg =
    TemplateId[Unit]((), k.moduleName, k.entityName)

  private def filterUniqueTemplateIs(
      all: Set[TemplateId.RequiredPkg]
  ): Map[TemplateId.NoPkg, TemplateId.RequiredPkg] =
    all
      .groupBy(k => key2(k))
      .collect { case (k, v) if v.sizeIs == 1 => (k, v.head) }

  def resolveTemplateId(
      m: TemplateIdMap
  )(a: TemplateId.OptionalPkg): Option[TemplateId.RequiredPkg] =
    a.packageId match {
      case Some(p) => findTemplateIdByK3(m.all)(TemplateId(p, a.moduleName, a.entityName))
      case None => findTemplateIdByK2(m.unique)(TemplateId((), a.moduleName, a.entityName))
    }

  private def findTemplateIdByK3(m: Set[TemplateId.RequiredPkg])(
      k: TemplateId.RequiredPkg
  ): Option[TemplateId.RequiredPkg] = Some(k).filter(m.contains)

  private def findTemplateIdByK2(m: Map[TemplateId.NoPkg, TemplateId.RequiredPkg])(
      k: TemplateId.NoPkg
  ): Option[TemplateId.RequiredPkg] = m.get(k)

  def resolveChoiceArgType(
      choiceIdMap: ChoiceTypeMap
  )(templateId: TemplateId.RequiredPkg, choice: Choice): Error \/ iface.Type = {
    val k = (templateId, choice)
    choiceIdMap
      .get(k)
      .toRightDisjunction(InputError(s"Cannot resolve Choice Argument type, given: ${k.toString}"))
  }

  def resolveKey(keyTypeMap: KeyTypeMap)(templateId: TemplateId.RequiredPkg): Error \/ iface.Type =
    keyTypeMap
      .get(templateId)
      .toRightDisjunction(
        InputError(s"Cannot resolve Template Key type, given: ${templateId.toString}")
      )

  // TODO (Leo): merge getChoiceTypeMap and getKeyTypeMap, so we build them in one iteration over all templates
  def getChoiceTypeMap(packageStore: PackageStore): ChoiceTypeMap =
    packageStore.flatMap { case (_, interface) => getChoices(interface) }

  private def getChoices(
      interface: iface.Interface
  ): Map[(TemplateId.RequiredPkg, Choice), iface.Type] =
    interface.typeDecls.flatMap {
      case (qn, iface.InterfaceType.Template(_, iface.DefTemplate(choices, _))) =>
        val templateId = TemplateId(interface.packageId, qn.module.toString, qn.name.toString)
        getChoices(choices).map { case (choice, id) => ((templateId, choice), id) }
      case _ => Seq.empty
    }

  private def getChoices(
      choices: Map[Ref.Name, iface.TemplateChoice[iface.Type]]
  ): Seq[(Choice, iface.Type)] = {
    import iface._
    choices.toSeq.collect { case (name, TemplateChoice(choiceType, _, _)) =>
      (Choice(name.toString), choiceType)
    }
  }

  // TODO (Leo): merge getChoiceTypeMap and getKeyTypeMap, so we build them in one iteration over all templates
  private def getKeyTypeMap(packageStore: PackageStore): KeyTypeMap =
    packageStore.flatMap { case (_, interface) => getKeys(interface) }

  private def getKeys(interface: iface.Interface): Map[TemplateId.RequiredPkg, iface.Type] =
    interface.typeDecls.collect {
      case (
            qn,
            iface.InterfaceType
              .Template(_, iface.DefTemplate(_, Some(keyType))),
          ) =>
        val templateId = TemplateId(interface.packageId, qn.module.dottedName, qn.name.dottedName)
        (templateId, keyType)
    }
}
