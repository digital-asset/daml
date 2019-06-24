// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.ledger.client.services.pkg.PackageClient
import com.digitalasset.ledger.service.{LedgerReader, TemplateIds}
import scalaz._
import Scalaz._

import scala.concurrent.{ExecutionContext, Future}

class PackageService(packageClient: PackageClient)(implicit ec: ExecutionContext) {

  type Error = String
  type TemplateIdMapping = Map[(String, String), Identifier]

  def getTemplateIdMapping(): Future[Error \/ TemplateIdMapping] =
    EitherT(LedgerReader.createPackageStore(packageClient)).map { packageStore =>
      val templateIds = TemplateIds.getTemplateIds(packageStore.values.toSet)
      buildMapping(templateIds)
    }.run

  private def buildMapping(ids: Set[Identifier]): TemplateIdMapping =
    ids.foldLeft(Map.empty[(String, String), Identifier]) { (b, a) =>
      b.updated((a.moduleName, a.entityName), a)
    }
}
