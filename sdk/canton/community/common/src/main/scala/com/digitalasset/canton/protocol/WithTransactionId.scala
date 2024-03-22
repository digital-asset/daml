// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.logging.pretty.Pretty

final case class WithTransactionId[+A](private val x: A, transactionId: TransactionId) {
  def unwrap: A = x

  def map[B](f: A => B): WithTransactionId[B] = this.copy(x = f(x))

  override def toString: String = s"($x -> $transactionId)"
}

object WithTransactionId {
  implicit def prettyWithTransactionId[A: Pretty]: Pretty[WithTransactionId[A]] = {
    import Pretty.*
    prettyOfClass(
      unnamedParam(_.x),
      param("transaction id", _.transactionId),
    )
  }
}
