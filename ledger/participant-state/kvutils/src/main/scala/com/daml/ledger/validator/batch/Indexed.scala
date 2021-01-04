// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.ledger.validator.batch

import scala.concurrent.{ExecutionContext, Future}

/** Utility for carrying an index with a value that allows performing
  * operations on the underlying value while keeping the index intact. */
private[batch] case class Indexed[T](value: T, index: Long) {
  def map[T2](f: T => T2): Indexed[T2] = {
    Indexed(f(this.value), this.index)
  }
  def mapFuture[T2](f: T => Future[T2])(
      implicit executionContext: ExecutionContext): Future[Indexed[T2]] = {
    f(this.value).map { value2 =>
      Indexed(value2, this.index)
    }
  }
}

private[batch] object Indexed {
  def fromSeq[T](seq: Seq[T]): Seq[Indexed[T]] =
    seq.zipWithIndex.map { case (value, index) => Indexed(value, index.toLong) }
}
