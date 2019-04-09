// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.services

import com.daml.ledger.participant.state.index.v1.{AsyncResult, IndexService}
import com.digitalasset.platform.server.api.validation.ErrorFactories

import scala.concurrent.{ExecutionContext, Future}

trait DamlOnXServiceUtils extends ErrorFactories {

  def consumeAsyncResult[T](ar: AsyncResult[T])(
      implicit ec: ExecutionContext): Future[T] = {
    ar.flatMap {
      case Left(IndexService.Err.LedgerIdMismatch(expected, actual)) =>
        Future.failed(ledgerIdMismatch(expected, actual))
      case Right(result) =>
        Future.successful(result)
    }
  }

}
