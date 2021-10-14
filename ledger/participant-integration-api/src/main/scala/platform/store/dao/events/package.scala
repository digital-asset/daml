// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import akka.stream.scaladsl.Source
import anorm.{BatchSql, NamedParameter}
import com.daml.lf.transaction.Node.KeyWithMaintainers

/** Type aliases used throughout the package
  */
package object events {

  type SqlSequence[A] = SqlSequence.T[A]

  import com.daml.lf.value.{Value => lfval}
  private[events] type ContractId = lfval.ContractId
  private[events] val ContractId = com.daml.lf.value.Value.ContractId
  private[events] type Value = lfval.VersionedValue
  private[events] type Contract = lfval.VersionedContractInstance
  private[events] val Contract = lfval.VersionedContractInstance

  import com.daml.lf.{transaction => lftx}
  private[events] type NodeId = lftx.NodeId
  private[events] type Node = lftx.Node.GenNode
  private[events] type Create = lftx.Node.NodeCreate
  private[events] type Exercise = lftx.Node.NodeExercises
  private[events] type Fetch = lftx.Node.NodeFetch
  private[events] type LookupByKey = lftx.Node.NodeLookupByKey
  private[events] type Key = lftx.GlobalKey
  private[events] val Key = lftx.GlobalKey

  import com.daml.lf.{data => lfdata}
  private[events] type Party = lfdata.Ref.Party
  private[events] val Party = lfdata.Ref.Party
  private[events] type Identifier = lfdata.Ref.Identifier
  private[events] val Identifier = lfdata.Ref.Identifier
  private[events] type QualifiedName = lfdata.Ref.QualifiedName
  private[events] val QualifiedName = lfdata.Ref.QualifiedName
  private[events] type DottedName = lfdata.Ref.DottedName
  private[events] val DottedName = lfdata.Ref.DottedName
  private[events] type ModuleName = lfdata.Ref.ModuleName
  private[events] val ModuleName = lfdata.Ref.ModuleName
  private[events] type LedgerString = lfdata.Ref.LedgerString
  private[events] val LedgerString = lfdata.Ref.LedgerString
  private[events] type TransactionId = lfdata.Ref.LedgerString
  private[events] val TransactionId = lfdata.Ref.LedgerString
  private[events] type WorkflowId = lfdata.Ref.LedgerString
  private[events] val WorkflowId = lfdata.Ref.LedgerString
  private[events] type ChoiceName = lfdata.Ref.ChoiceName
  private[events] val ChoiceName = lfdata.Ref.ChoiceName
  private[events] type PackageId = lfdata.Ref.PackageId
  private[events] val PackageId = lfdata.Ref.PackageId
  private[events] type WitnessRelation[A] = lfdata.Relation.Relation[A, Party]
  private[events] type DisclosureRelation = WitnessRelation[NodeId]
  private[events] type DivulgenceRelation = WitnessRelation[ContractId]
  private[dao] type FilterRelation = lfdata.Relation.Relation[Party, lfdata.Ref.Identifier]
  private[events] val Relation = lfdata.Relation.Relation

  import com.daml.lf.crypto
  private[events] type Hash = crypto.Hash

  /** Groups together items of type [[A]] that share an attribute [[K]] over a
    * contiguous stretch of the input [[Source]]. Well suited to perform group-by
    * operations of streams where [[K]] attributes are either sorted or at least
    * show up in blocks.
    *
    * Implementation detail: this method _must_ use concatSubstreams instead of
    * mergeSubstreams to prevent the substreams to be processed in parallel,
    * potentially causing the outputs to be delivered in a different order.
    *
    * Docs: https://doc.akka.io/docs/akka/2.6.10/stream/stream-substream.html#groupby
    */
  private[events] def groupContiguous[A, K, Mat](
      source: Source[A, Mat]
  )(by: A => K): Source[Vector[A], Mat] =
    source
      .statefulMapConcat(() => {
        var previousSegmentKey: K = null.asInstanceOf[K]
        entry => {
          val keyForEntry = by(entry)
          val entryWithSplit = entry -> (keyForEntry != previousSegmentKey)
          previousSegmentKey = keyForEntry
          List(entryWithSplit)
        }
      })
      .splitWhen(_._2)
      .map(_._1)
      .fold(Vector.empty[A])(_ :+ _)
      .concatSubstreams

  // Dispatches the call to either function based on the cardinality of the input
  // This is mostly designed to route requests to queries specialized for single/multi-party subs
  // Callers should ensure that the set is not empty, which in the usage this
  // is designed for should be provided by the Ledger API validation layer
  private[events] def route[A, B](
      set: Set[A]
  )(single: A => B, multi: Set[A] => B): B = {
    assume(set.nonEmpty, "Empty set, unable to dispatch to single/multi implementation")
    set.size match {
      case 1 => single(set.iterator.next())
      case n if n > 1 => multi(set)
    }
  }

  private[events] def convert(template: Identifier, key: lftx.Node.KeyWithMaintainers[Value]): Key =
    Key.assertBuild(template, key.key.value)

  private[events] def convertLfValueKey(
      template: Identifier,
      key: KeyWithMaintainers[lfval],
  ) =
    Key.assertBuild(template, key.key)

  private[events] def batch(query: String, parameters: Seq[Seq[NamedParameter]]): Option[BatchSql] =
    if (parameters.isEmpty) None else Some(BatchSql(query, parameters.head, parameters.tail: _*))

}
