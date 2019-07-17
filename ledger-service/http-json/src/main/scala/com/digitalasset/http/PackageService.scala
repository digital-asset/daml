// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.service.LedgerReader.PackageStore
import com.digitalasset.ledger.service.TemplateIds
import scalaz.Scalaz._
import scalaz._

import scala.collection.breakOut

object PackageService {
  sealed trait Error
  final case class InputError(message: String) extends Error
  final case class ServerError(message: String) extends Error

  object Error {
    implicit val errorShow: Show[Error] = new Show[Error] {
      override def shows(e: Error): String = e match {
        case InputError(m) => s"PackageService input error: ${m: String}"
        case ServerError(m) => s"PackageService server error: ${m: String}"
      }
    }
  }

  def getTemplateIdMap(packageStore: PackageStore): TemplateIdMap =
    buildTemplateIdMap(collectTemplateIds(packageStore))

  private def collectTemplateIds(packageStore: PackageStore): Set[lar.TemplateId] =
    lar.TemplateId.subst(TemplateIds.getTemplateIds(packageStore.values.toSet))

  case class TemplateIdMap(
      all: Map[domain.TemplateId.RequiredPkg, lar.TemplateId],
      unique: Map[domain.TemplateId.NoPkg, lar.TemplateId])

  def buildTemplateIdMap(ids: Set[lar.TemplateId]): TemplateIdMap = {
    val all: Map[domain.TemplateId.RequiredPkg, lar.TemplateId] =
      ids.map(a => key3(a) -> a)(breakOut)
    val unique: Map[domain.TemplateId.NoPkg, lar.TemplateId] = filterUniqueTemplateIs(all)
    TemplateIdMap(all, unique)
  }

  private[http] def key3(a: lar.TemplateId): domain.TemplateId.RequiredPkg = {
    val b = lar.TemplateId.unwrap(a)
    domain.TemplateId[String](b.packageId, b.moduleName, b.entityName)
  }

  private[http] def key2(k: domain.TemplateId.RequiredPkg): domain.TemplateId.NoPkg =
    domain.TemplateId[Unit]((), k.moduleName, k.entityName)

  private def filterUniqueTemplateIs(all: Map[domain.TemplateId.RequiredPkg, lar.TemplateId])
    : Map[domain.TemplateId.NoPkg, lar.TemplateId] =
    all
      .groupBy { case (k, _) => key2(k) }
      .collect { case (k, v) if v.size == 1 => (k, v.values.head) }

  def resolveTemplateIds(m: TemplateIdMap)(
      as: Set[domain.TemplateId.OptionalPkg]): Error \/ List[lar.TemplateId] =
    for {
      bs <- as.toList.traverseU(resolveTemplateId(m))
      _ <- validate(as, bs)
    } yield bs

  def resolveTemplateId(m: TemplateIdMap)(
      a: domain.TemplateId.OptionalPkg): Error \/ lar.TemplateId =
    a.packageId match {
      case Some(p) => findTemplateIdByK3(m.all)(domain.TemplateId(p, a.moduleName, a.entityName))
      case None => findTemplateIdByK2(m.unique)(domain.TemplateId((), a.moduleName, a.entityName))
    }

  private def findTemplateIdByK3(m: Map[domain.TemplateId.RequiredPkg, lar.TemplateId])(
      k: domain.TemplateId.RequiredPkg): Error \/ lar.TemplateId =
    m.get(k).toRightDisjunction(InputError(s"Cannot resolve ${k.toString}"))

  private def findTemplateIdByK2(m: Map[domain.TemplateId.NoPkg, lar.TemplateId])(
      k: domain.TemplateId.NoPkg): Error \/ lar.TemplateId =
    m.get(k).toRightDisjunction(InputError(s"Cannot resolve ${k.toString}"))

  private def validate(
      requested: Set[domain.TemplateId.OptionalPkg],
      resolved: List[lar.TemplateId]): Error \/ Unit =
    if (requested.size == resolved.size) \/.right(())
    else
      \/.left(
        ServerError(
          s"Template ID resolution error, the sizes of requested and resolved collections should match. " +
            s"requested: $requested, resolved: $resolved"))
}
