// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.ledger.client.services.pkg.PackageClient
import com.digitalasset.ledger.service.{LedgerReader, TemplateIds}
import scalaz.Scalaz._
import scalaz._

import scala.concurrent.{ExecutionContext, Future}

class PackageService(packageClient: PackageClient)(implicit ec: ExecutionContext) {
  import PackageService._

  def getTemplateIdMap(): Future[Error \/ (TemplateIdDups, TemplateIdMap)] =
    EitherT(LedgerReader.createPackageStore(packageClient)).map { packageStore =>
      val templateIds = TemplateIds.getTemplateIds(packageStore.values.toSet)
      buildMap(templateIds)
    }.run
}

object PackageService {
  type Error = String
  type TemplateIdDups = Map[(String, String), List[Identifier]]
  type TemplateIdMap = Map[(String, String), Identifier]

  private[http] def buildMap(ids: Set[Identifier]): (TemplateIdDups, TemplateIdMap) = {
    val (dupe, nonDupe) = ids.groupBy(a => (a.moduleName, a.entityName)).partition(_._2.size > 1)
    (dupe transform ((_, as) => as.toList), nonDupe transform { (k, as) =>
      if (as.size == 1) as.head
      else sys.error(s"This should never happen! $k is duplicated in the non-duplicate list")
    })
  }

  def resolveTemplateIds(m: TemplateIdMap)(
      as: Set[domain.TemplateId.OptionalPkg]): Error \/ List[Identifier] =
    for {
      bs <- as.toList.traverseU(resolveTemplateId(m))
      _ <- validate(as, bs)
    } yield bs

  def resolveTemplateId(m: TemplateIdMap)(a: domain.TemplateId.OptionalPkg): Error \/ Identifier =
    a.packageId
      .map { x =>
        Identifier(packageId = x, moduleName = a.moduleName, entityName = a.entityName)
      }
      .toRightDisjunction(())
      .orElse { findTemplateId(m)((a.moduleName, a.entityName)) }

  private def findTemplateId(m: TemplateIdMap)(a: (String, String)): Error \/ Identifier =
    m.get(a).toRightDisjunction(s"Cannot resolve $a")

  private def validate(
      requested: Set[domain.TemplateId.OptionalPkg],
      resolved: List[Identifier]): Error \/ Unit =
    if (requested.size == resolved.size) \/.right(())
    else
      \/.left(
        s"Template ID resolution error, the sizes of requested and resolved collections should match. " +
          s"requested: $requested, resolved: $resolved")

  def fold(dups: TemplateIdDups): Set[Identifier] =
    dups.values.flatten.toSet
}
