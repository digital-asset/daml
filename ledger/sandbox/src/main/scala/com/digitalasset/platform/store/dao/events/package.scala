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
  private[events] type Value = lfval.VersionedValue[ContractId]

  import com.daml.lf.{transaction => lftx}
  private[events] type NodeId = lftx.Transaction.NodeId
  private[events] type Transaction = lftx.GenTransaction.WithTxValue[NodeId, ContractId]
  private[events] type Node = lftx.Node.GenNode.WithTxValue[NodeId, ContractId]
  private[events] type Create = lftx.Node.NodeCreate.WithTxValue[ContractId]
  private[events] type Exercise = lftx.Node.NodeExercises.WithTxValue[NodeId, ContractId]

  import com.daml.lf.{data => lfdata}
  private[events] type Party = lfdata.Ref.Party
  private[events] type DisclosureRelation = lfdata.Relation.Relation[NodeId, Party]
  private[events] val DisclosureRelation = lfdata.Relation.Relation

  private[events] type FilterRelation = lfdata.Relation.Relation[Party, lfdata.Ref.Identifier]
  private[events] val FilterRelation = lfdata.Relation.Relation

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

}
