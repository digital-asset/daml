// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.\/

import scala.concurrent.Future

object Services {
  type ResolveTemplateIds =
    Set[domain.TemplateId.OptionalPkg] => PackageService.Error \/ List[lar.TemplateId]

  type ResolveTemplateId =
    domain.TemplateId.OptionalPkg => PackageService.Error \/ lar.TemplateId

  type SubmitAndWaitForTransaction =
    lav1.command_service.SubmitAndWaitRequest => Future[
      lav1.command_service.SubmitAndWaitForTransactionResponse]
}
