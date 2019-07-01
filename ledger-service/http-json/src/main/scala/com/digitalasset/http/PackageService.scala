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
    EitherT(LedgerReader.createPackageStore(packageClient)).map { packageStore =>
      val templateIds = TemplateIds.getTemplateIds(packageStore.values.toSet)
      buildTemplateIdMap(templateIds)
    }.run
}

object PackageService {
  type Error = String

  type K3 = (String, String, String)
  type K2 = (String, String)
  case class TemplateIdMap(all: Map[K3, Identifier], unique: Map[K2, Identifier])

  def buildTemplateIdMap(ids: Set[Identifier]): TemplateIdMap = {
    val all: Map[K3, Identifier] = ids.map(a => key3(a) -> a)(breakOut)
    val unique: Map[K2, Identifier] = filterUniqueTemplateIs(all)
    TemplateIdMap(all, unique)
  }

  private[http] def key3(a: Identifier): K3 =
    (a.packageId, a.moduleName, a.entityName)

  private[http] def key2(k: K3): K2 =
    (k._2, k._3)

  private def filterUniqueTemplateIs(all: Map[K3, Identifier]): Map[K2, Identifier] =
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
      case Some(p) => findTemplateId(m.all)((p, a.moduleName, a.entityName))
      case None => findTemplateId(m.unique)((a.moduleName, a.entityName))
    }

  private def findTemplateId(m: Map[K3, Identifier])(k: K3): Error \/ Identifier =
    m.get(k).toRightDisjunction(s"Cannot resolve $k")

  private def findTemplateId(m: Map[K2, Identifier])(k: K2): Error \/ Identifier =
    m.get(k).toRightDisjunction(s"Cannot resolve $k")

  private def validate(
      requested: Set[domain.TemplateId.OptionalPkg],
      resolved: List[Identifier]): Error \/ Unit =
    if (requested.size == resolved.size) \/.right(())
    else
      \/.left(
        s"Template ID resolution error, the sizes of requested and resolved collections should match. " +
          s"requested: $requested, resolved: $resolved")
}
