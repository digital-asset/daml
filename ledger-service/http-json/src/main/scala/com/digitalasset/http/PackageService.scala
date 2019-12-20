// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.iface
import com.digitalasset.http.domain.{Choice, TemplateId}
import com.digitalasset.http.util.IdentifierConverters
import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.ledger.service.LedgerReader.PackageStore
import com.digitalasset.ledger.service.{LedgerReader, TemplateIds}
import com.typesafe.scalalogging.StrictLogging
import scalaz.Scalaz._
import scalaz._

import scala.concurrent.{ExecutionContext, Future}

private class PackageService(reloadPackageStoreIfChanged: PackageService.ReloadPackageStore)
    extends StrictLogging {

  import PackageService._

  private case class State(
      packageIds: Set[String],
      templateIdMap: TemplateIdMap,
      choiceIdMap: ChoiceIdMap,
      keyIdMap: KeyIdMap,
      packageStore: PackageStore) {

    def append(diff: PackageStore): State = {
      val newPackageStore = this.packageStore ++ diff
      State(
        newPackageStore.keySet,
        getTemplateIdMap(newPackageStore),
        getChoiceIdMap(newPackageStore),
        getKeyIdMap(newPackageStore),
        newPackageStore)
    }
  }

  // volatile, reading threads don't need synchronization
  @volatile private var state: State =
    State(Set.empty, TemplateIdMap.Empty, Map.empty, Map.empty, Map.empty)

  // synchronized, so two threads cannot reload it concurrently
  def reload(implicit ec: ExecutionContext): Future[Error \/ Unit] = synchronized {
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

  // See the above comment
  def resolveTemplateIds: ResolveTemplateIds =
    x => PackageService.resolveTemplateIds(state.templateIdMap)(x)

  def allTemplateIds: AllTemplateIds =
    () => state.templateIdMap.all

  // See the above comment
  def resolveChoiceRecordId: ResolveChoiceRecordId =
    (x, y) => PackageService.resolveChoiceRecordId(state.choiceIdMap)(x, y)

  // See the above comment
  def resolveKeyId: ResolveKeyId =
    x => PackageService.resolveKeyId(state.keyIdMap)(x)
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

  type ResolveTemplateIds =
    Set[TemplateId.OptionalPkg] => Error \/ List[TemplateId.RequiredPkg]

  type ResolveTemplateId =
    TemplateId.OptionalPkg => Error \/ TemplateId.RequiredPkg

  type AllTemplateIds =
    () => Set[TemplateId.RequiredPkg]

  type ResolveChoiceRecordId =
    (TemplateId.RequiredPkg, Choice) => Error \/ Identifier

  type ResolveKeyId =
    TemplateId.RequiredPkg => Error \/ Identifier

  case class TemplateIdMap(
      all: Set[TemplateId.RequiredPkg],
      unique: Map[TemplateId.NoPkg, TemplateId.RequiredPkg])

  object TemplateIdMap {
    val Empty: TemplateIdMap = TemplateIdMap(Set.empty, Map.empty)
  }

  type ChoiceIdMap = Map[(TemplateId.RequiredPkg, Choice), Identifier]

  type KeyIdMap = Map[TemplateId.RequiredPkg, Identifier]

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
      all: Set[TemplateId.RequiredPkg]): Map[TemplateId.NoPkg, TemplateId.RequiredPkg] =
    all
      .groupBy(k => key2(k))
      .collect { case (k, v) if v.size == 1 => (k, v.head) }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def resolveTemplateIds(m: TemplateIdMap)(
      as: Set[TemplateId.OptionalPkg]): Error \/ List[TemplateId.RequiredPkg] =
    for {
      bs <- as.toList.traverse(resolveTemplateId(m))
      _ <- validate(as, bs)
    } yield bs

  def resolveTemplateId(m: TemplateIdMap)(
      a: TemplateId.OptionalPkg): Error \/ TemplateId.RequiredPkg =
    a.packageId match {
      case Some(p) => findTemplateIdByK3(m.all)(TemplateId(p, a.moduleName, a.entityName))
      case None => findTemplateIdByK2(m.unique)(TemplateId((), a.moduleName, a.entityName))
    }

  private def findTemplateIdByK3(m: Set[TemplateId.RequiredPkg])(
      k: TemplateId.RequiredPkg): Error \/ TemplateId.RequiredPkg =
    if (m.contains(k)) \/-(k)
    else -\/(InputError(s"Cannot resolve template ID, given: ${k.toString}"))

  private def findTemplateIdByK2(m: Map[TemplateId.NoPkg, TemplateId.RequiredPkg])(
      k: TemplateId.NoPkg): Error \/ TemplateId.RequiredPkg =
    m.get(k).toRightDisjunction(InputError(s"Cannot resolve template ID, given: ${k.toString}"))

  private def validate(
      requested: Set[TemplateId.OptionalPkg],
      resolved: List[TemplateId.RequiredPkg]): Error \/ Unit =
    if (requested.size == resolved.size) \/.right(())
    else
      \/.left(
        ServerError(
          s"Template ID resolution error, the sizes of requested and resolved collections should match. " +
            s"requested: $requested, resolved: $resolved"))

  def resolveChoiceRecordId(choiceIdMap: ChoiceIdMap)(
      templateId: TemplateId.RequiredPkg,
      choice: Choice): Error \/ Identifier = {
    val k = (templateId, choice)
    choiceIdMap
      .get(k)
      .toRightDisjunction(InputError(s"Cannot resolve choice record ID, given: ${k.toString}"))
  }

  def resolveKeyId(keyTypeMap: KeyIdMap)(templateId: TemplateId.RequiredPkg): Error \/ Identifier =
    keyTypeMap
      .get(templateId)
      .toRightDisjunction(
        InputError(s"Cannot resolve Template Key ID, given: ${templateId.toString}"))

  // TODO (Leo): merge getChoiceIdMap and getKeyIdMap, so we build them in one iteration over all templates
  def getChoiceIdMap(packageStore: PackageStore): ChoiceIdMap =
    packageStore.flatMap { case (_, interface) => getChoices(interface) }(collection.breakOut)

  private def getChoices(
      interface: iface.Interface): Map[(TemplateId.RequiredPkg, Choice), Identifier] =
    interface.typeDecls.flatMap {
      case (qn, iface.InterfaceType.Template(_, iface.DefTemplate(choices, _))) =>
        val templateId = TemplateId(interface.packageId, qn.module.toString, qn.name.toString)
        getChoices(choices).map { case (choice, id) => ((templateId, choice), id) }
      case _ => Seq.empty
    }

  private def getChoices(
      choices: Map[Ref.Name, iface.TemplateChoice[iface.Type]]): Seq[(Choice, Identifier)] = {
    import iface._
    choices.toSeq.collect {
      case (name, TemplateChoice(TypeCon(typeConName, _), _, _)) =>
        (Choice(name.toString), IdentifierConverters.apiIdentifier(typeConName.identifier))
    }
  }

  // TODO (Leo): merge getChoiceIdMap and getKeyIdMap, so we build them in one iteration over all templates
  private def getKeyIdMap(packageStore: PackageStore): KeyIdMap =
    packageStore.flatMap { case (_, interface) => getKeys(interface) }(collection.breakOut)

  private def getKeys(interface: iface.Interface): Map[TemplateId.RequiredPkg, Identifier] =
    interface.typeDecls.collect {
      case (
          qn,
          iface.InterfaceType
            .Template(_, iface.DefTemplate(_, Some(iface.TypeCon(typeConName, _))))) =>
        val templateId = TemplateId(interface.packageId, qn.module.toString, qn.name.toString)
        (templateId, IdentifierConverters.apiIdentifier(typeConName.identifier))
    }
}
