// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.digitalasset.daml.lf.value.Versioned

import scala.collection.TraversableLike
import scala.collection.generic.CanBuildFrom

package object transaction {

  /** This traversal fails the identity law so is unsuitable for [[scalaz.Traverse]].
    * It is, nevertheless, what is meant sometimes.
    */
  private[transaction] def sequence[A, B, This, That](seq: TraversableLike[Either[A, B], This])(
      implicit cbf: CanBuildFrom[This, B, That]): Either[A, That] =
    seq collectFirst {
      case Left(e) => Left(e)
    } getOrElse {
      val b = cbf.apply()
      seq.foreach {
        case Right(a) => b += a
        case e @ Left(_) => sys.error(s"impossible $e")
      }
      Right(b.result())
    }

  /**
    * the method VersionedTransaction#typedBy does not recur into the values herein;
    * it is safe to apply [[com.digitalasset.daml.lf.value.Value.VersionedValue#typedBy]]
    * to any subset of this `languageVersions` for all values herein, unlike most
    * [[value.Value]] operations.
    *
    * {{{
    *   val vt2 = vt.typedBy(someVers:_*)
    *   // safe if and only if vx() yields a subset of someVers
    *   vt2.copy(transaction = vt2.transaction
    *              .mapContractIdAndValue(identity, _.typedBy(vx():_*)))
    * }}}
    *
    * However, applying the ''same'' version set is probably not what you mean,
    * because the set of language versions that types a whole transaction is
    * probably not the same set as those language version[s] that type each
    * value, since each value can be typed by different modules.
    */
  type VersionedTransaction[Nid, Cid] =
    Versioned[TransactionVersion, GenTransaction.WithTxValue[Nid, Cid]]

  object VersionedTransaction {
    def apply[Nid, Cid](
        version: TransactionVersion,
        transaction: GenTransaction.WithTxValue[Nid, Cid]): VersionedTransaction[Nid, Cid] =
      Versioned(version, transaction)
  }

  implicit class GenTransactionOps[Nid, Cid](
      transaction: GenTransaction[Nid, Cid, Transaction.Value[Cid]]) {

    def mapContractId[Cid2](f: Cid => Cid2): GenTransaction[Nid, Cid2, Transaction.Value[Cid2]] =
      transaction.mapContractIdAndValue(f, _.mapContractId(f))

  }

}
