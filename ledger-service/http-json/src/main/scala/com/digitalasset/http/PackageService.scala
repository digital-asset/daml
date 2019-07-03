// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.ledger.client.services.pkg.PackageClient
import com.digitalasset.ledger.service.{LedgerReader, TemplateIds}
import scalaz.Scalaz._
import scalaz._

import scala.collection.breakOut
import scala.concurrent.{ExecutionContext, Future}

class PackageService(packageClient: PackageClient)(implicit ec: ExecutionContext) {
  import PackageService._

  def getTemplateIdMap(): Future[Error \/ TemplateIdMap] =
    EitherT(LedgerReader.createPackageStore(packageClient))
      .leftMap(e => ServerError(e))
      .map { packageStore =>
        val templateIds = TemplateIds.getTemplateIds(packageStore.values.toSet)
        buildTemplateIdMap(templateIds)
      }
      .run
}

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

  case class TemplateIdMap(
      all: Map[domain.TemplateId.RequiredPkg, Identifier],
      unique: Map[domain.TemplateId.NoPkg, Identifier])

  def buildTemplateIdMap(ids: Set[Identifier]): TemplateIdMap = {
    val all: Map[domain.TemplateId.RequiredPkg, Identifier] = ids.map(a => key3(a) -> a)(breakOut)
    val unique: Map[domain.TemplateId.NoPkg, Identifier] = filterUniqueTemplateIs(all)
    TemplateIdMap(all, unique)
  }

  private[http] def key3(a: Identifier): domain.TemplateId.RequiredPkg =
    domain.TemplateId[String](a.packageId, a.moduleName, a.entityName)

  private[http] def key2(k: domain.TemplateId.RequiredPkg): domain.TemplateId.NoPkg =
    domain.TemplateId[Unit]((), k.moduleName, k.entityName)

  private def filterUniqueTemplateIs(all: Map[domain.TemplateId.RequiredPkg, Identifier])
    : Map[domain.TemplateId.NoPkg, Identifier] =
    all
      .groupBy { case (k, _) => key2(k) }
      .collect { case (k, v) if v.size == 1 => (k, v.values.head) }

  def resolveTemplateIds(m: TemplateIdMap)(
      as: Set[domain.TemplateId.OptionalPkg]): Error \/ List[Identifier] =
    for {
      bs <- as.toList.traverseU(resolveTemplateId(m))
      _ <- validate(as, bs)
    } yield bs

  def resolveTemplateId(m: TemplateIdMap)(a: domain.TemplateId.OptionalPkg): Error \/ Identifier =
    a.packageId match {
      case Some(p) => findTemplateIdByK3(m.all)(domain.TemplateId(p, a.moduleName, a.entityName))
      case None => findTemplateIdByK2(m.unique)(domain.TemplateId((), a.moduleName, a.entityName))
    }

  private def findTemplateIdByK3(m: Map[domain.TemplateId.RequiredPkg, Identifier])(
      k: domain.TemplateId.RequiredPkg): Error \/ Identifier =
    m.get(k).toRightDisjunction(InputError(s"Cannot resolve ${k.toString}"))

  private def findTemplateIdByK2(m: Map[domain.TemplateId.NoPkg, Identifier])(
      k: domain.TemplateId.NoPkg): Error \/ Identifier =
    m.get(k).toRightDisjunction(InputError(s"Cannot resolve ${k.toString}"))

  private def validate(
      requested: Set[domain.TemplateId.OptionalPkg],
      resolved: List[Identifier]): Error \/ Unit =
    if (requested.size == resolved.size) \/.right(())
    else
      \/.left(
        ServerError(
          s"Template ID resolution error, the sizes of requested and resolved collections should match. " +
            s"requested: $requested, resolved: $resolved"))
}
