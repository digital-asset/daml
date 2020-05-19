// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import akka.stream.scaladsl.Source

/**
  * Type aliases used throughout the package
  */
package object events {

  import com.daml.lf.value.{Value => lfval}
  private[events] type ContractId = lfval.AbsoluteContractId
  private[events] val ContractId = com.daml.lf.value.Value.AbsoluteContractId
  private[events] type Value = lfval.VersionedValue[ContractId]
  private[events] type Contract = lfval.ContractInst[Value]
  private[events] val Contract = lfval.ContractInst

  import com.daml.lf.{transaction => lftx}
  private[events] type NodeId = lftx.Transaction.NodeId
  private[events] type Transaction = lftx.GenTransaction.WithTxValue[NodeId, ContractId]
  private[events] type Node = lftx.Node.GenNode.WithTxValue[NodeId, ContractId]
  private[events] type Create = lftx.Node.NodeCreate.WithTxValue[ContractId]
  private[events] type Exercise = lftx.Node.NodeExercises.WithTxValue[NodeId, ContractId]
  private[events] type Fetch = lftx.Node.NodeFetch.WithTxValue[ContractId]
  private[events] type LookupByKey = lftx.Node.NodeLookupByKey.WithTxValue[ContractId]
  private[events] type Key = lftx.Node.GlobalKey
  private[events] val Key = lftx.Node.GlobalKey

  import com.daml.lf.{data => lfdata}
  private[events] type Party = lfdata.Ref.Party
  private[events] val Party = lfdata.Ref.Party
  private[events] type Identifier = lfdata.Ref.Identifier
  private[events] val Identifier = lfdata.Ref.Identifier
  private[events] type LedgerString = lfdata.Ref.LedgerString
  private[events] val LedgerString = lfdata.Ref.LedgerString
  private[events] type WitnessRelation[A] = lfdata.Relation.Relation[A, Party]
  private[events] type DisclosureRelation = WitnessRelation[NodeId]
  private[events] type DivulgenceRelation = WitnessRelation[ContractId]
  private[events] type FilterRelation = lfdata.Relation.Relation[Party, lfdata.Ref.Identifier]
  private[events] val Relation = lfdata.Relation.Relation

  import com.daml.lf.crypto
  private[events] type Hash = crypto.Hash

  /**
    * Groups together items of type [[A]] that share an attribute [[K]] over a
    * contiguous stretch of the input [[Source]]. Well suited to perform group-by
    * operations of streams where [[K]] attributes are either sorted or at least
    * show up in blocks.
    */
  private[events] def groupContiguous[A, K, Mat](source: Source[A, Mat])(
      by: A => K): Source[Vector[A], Mat] =
    source
      .statefulMapConcat(() => {
        var previousSegmentKey: K = null.asInstanceOf[K]
        entry =>
          {
            val keyForEntry = by(entry)
            val entryWithSplit = entry -> (keyForEntry != previousSegmentKey)
            previousSegmentKey = keyForEntry
            List(entryWithSplit)
          }
      })
      .splitWhen(_._2)
      .map(_._1)
      .fold(Vector.empty[A])(_ :+ _)
      .mergeSubstreams

  // Dispatches the call to either function based on the cardinality of the input
  // This is mostly designed to route requests to queries specialized for single/multi-party subs
  // Callers should ensure that the set is not empty, which in the usage this
  // is designed for should be provided by the Ledger API validation layer
  private[events] def route[A, B](
      set: Set[A],
  )(single: A => B, multi: Set[A] => B): B = {
    assume(set.nonEmpty, "Empty set, unable to dispatch to single/multi implementation")
    set.size match {
      case 1 => single(set.toIterator.next)
      case n if n > 1 => multi(set)
    }
  }

  private[events] def convert(template: Identifier, key: lftx.Node.KeyWithMaintainers[Value]): Key =
    Key.assertBuild(template, key.key.value)

}
