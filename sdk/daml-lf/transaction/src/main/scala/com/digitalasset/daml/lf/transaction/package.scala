// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.daml.SafeProto
import com.digitalasset.daml.lf.value.ValueCoder
import com.google.protobuf

import scala.collection.BuildFrom

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

  val SubmittedTransaction = DiscriminatedSubtype[VersionedTransaction]
  type SubmittedTransaction = SubmittedTransaction.T

  val CommittedTransaction = DiscriminatedSubtype[VersionedTransaction]
  type CommittedTransaction = CommittedTransaction.T

  type VersionedGlobalKey = Versioned[GlobalKey]

  private[lf] def ensuresNoUnknownFields(message: protobuf.Message) =
    SafeProto.ensureNoUnknownFields(message).left.map(ValueCoder.DecodeError)

  private[lf] def ensuresNoUnknownFieldsThenDecode[M <: protobuf.Message, A](
      msg: M
  )(decode: M => Either[ValueCoder.DecodeError, A]): Either[ValueCoder.DecodeError, A] =
    for {
      _ <- ensuresNoUnknownFields(msg)
      a <- decode(msg)
    } yield a
}
