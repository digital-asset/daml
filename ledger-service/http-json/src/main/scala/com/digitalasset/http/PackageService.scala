// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.http.domain.TemplateId
import com.digitalasset.ledger.service.LedgerReader.PackageStore
import com.digitalasset.ledger.service.TemplateIds
import scalaz.Scalaz._
import scalaz._

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

  type ResolveTemplateIds =
    Set[domain.TemplateId.OptionalPkg] => Error \/ List[TemplateId.RequiredPkg]

  type ResolveTemplateId =
    domain.TemplateId.OptionalPkg => Error \/ TemplateId.RequiredPkg

  def getTemplateIdMap(packageStore: PackageStore): TemplateIdMap =
    buildTemplateIdMap(collectTemplateIds(packageStore))

  private def collectTemplateIds(packageStore: PackageStore): Set[TemplateId.RequiredPkg] =
    TemplateIds
      .getTemplateIds(packageStore.values.toSet)
      .map(x => TemplateId(x.packageId, x.moduleName, x.entityName))

  case class TemplateIdMap(
      all: Set[TemplateId.RequiredPkg],
      unique: Map[TemplateId.NoPkg, TemplateId.RequiredPkg])

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
    else -\/(InputError(s"Cannot resolve ${k.toString}"))

  private def findTemplateIdByK2(m: Map[TemplateId.NoPkg, TemplateId.RequiredPkg])(
      k: TemplateId.NoPkg): Error \/ TemplateId.RequiredPkg =
    m.get(k).toRightDisjunction(InputError(s"Cannot resolve ${k.toString}"))

  private def validate(
      requested: Set[TemplateId.OptionalPkg],
      resolved: List[TemplateId.RequiredPkg]): Error \/ Unit =
    if (requested.size == resolved.size) \/.right(())
    else
      \/.left(
        ServerError(
          s"Template ID resolution error, the sizes of requested and resolved collections should match. " +
            s"requested: $requested, resolved: $resolved"))
}
