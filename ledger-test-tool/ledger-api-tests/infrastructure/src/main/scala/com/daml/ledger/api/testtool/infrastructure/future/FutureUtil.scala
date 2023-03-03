// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.future

import scala.concurrent.{ExecutionContext, Future}

object FutureUtil {

  def sequential[T, R](elements: Seq[T])(f: T => Future[R])(implicit
      ec: ExecutionContext
  ): Future[Seq[R]] =
    elements.foldLeft(Future.successful(Seq.empty[R])) { case (results, newElement) =>
      results.flatMap(resultsSoFar => f(newElement).map(resultsSoFar :+ _))
    }

}
