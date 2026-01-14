// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.kernel.Monoid
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.resource.DbStorage.Implicits.monadDBIO
import slick.dbio.{DBIO, DBIOAction, Effect, NoStream}

import scala.concurrent.ExecutionContext

object DBIOUtil {

  /** Creates a lazily-constructed, retry-safe DBIOAction for a batched execution of DBIOActions.
    */
  def batchedSequentialTraverse[X, A: Monoid, E <: Effect](chunkSize: PositiveInt)(xs: Seq[X])(
      processChunk: (Int, Seq[X]) => (DBIOAction[A, NoStream, E])
  )(implicit ec: ExecutionContext): DBIOAction[A, NoStream, E] =
    // This construction is on purpose, so that in case of a retry of the top-level DBIOAction,
    // the resulting DBIOAction AST is re-built with a fresh iterator.
    DBIO.successful(xs).flatMap { chunks =>
      MonadUtil.foldLeftM[DBIOAction[*, NoStream, E], A, (Seq[X], Int)](
        Monoid[A].empty,
        chunks.grouped(chunkSize.value).zipWithIndex,
      ) { case (state, (chunk, chunkIndex)) =>
        processChunk(chunkIndex, chunk).map(Monoid[A].combine(state, _))
      }
    }
}
