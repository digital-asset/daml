// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import scala.collection.compat._

package object transaction {

  /** This traversal fails the identity law so is unsuitable for [[scalaz.Traverse]].
    * It is, nevertheless, what is meant sometimes.
    */
  private[transaction] def sequence[A, B, That](
      seq: Iterable[Either[A, B]]
  )(implicit cbf: BuildFrom[seq.type, B, That]): Either[A, That] =
    seq collectFirst { case Left(e) =>
      Left(e)
    } getOrElse {
      val b = cbf.newBuilder(seq)
      seq.foreach {
        case Right(a) => b += a
        case e @ Left(_) => sys.error(s"impossible $e")
      }
      Right(b.result())
    }

  val SubmittedTransaction = DiscriminatedSubtype[Transaction]
  type SubmittedTransaction = SubmittedTransaction.T

  val CommittedTransaction = DiscriminatedSubtype[Transaction]
  type CommittedTransaction = CommittedTransaction.T

  @deprecated("use Transaction", since = "1.18.0")
  type VersionedTransaction = Transaction
  @deprecated("use Transaction", since = "1.18.0")
  val VersionedTransaction = Transaction

}
