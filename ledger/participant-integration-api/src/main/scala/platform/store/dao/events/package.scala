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
  type ContractId = lfval.ContractId
  val ContractId = com.daml.lf.value.Value.ContractId
  type Value = lfval.VersionedValue[ContractId]
  type Contract = lfval.ContractInst[Value]
  val Contract = lfval.ContractInst

  import com.daml.lf.{transaction => lftx}
  type NodeId = lftx.NodeId
  type Node = lftx.Node.GenNode[NodeId, ContractId]
  type Create = lftx.Node.NodeCreate[ContractId]
  type Exercise = lftx.Node.NodeExercises[NodeId, ContractId]
  type Fetch = lftx.Node.NodeFetch[ContractId]
  type LookupByKey = lftx.Node.NodeLookupByKey[ContractId]
  type Key = lftx.GlobalKey
  val Key = lftx.GlobalKey

  import com.daml.lf.{data => lfdata}
  type Party = lfdata.Ref.Party
  val Party = lfdata.Ref.Party
  type Identifier = lfdata.Ref.Identifier
  val Identifier = lfdata.Ref.Identifier
  type QualifiedName = lfdata.Ref.QualifiedName
  val QualifiedName = lfdata.Ref.QualifiedName
  type DottedName = lfdata.Ref.DottedName
  val DottedName = lfdata.Ref.DottedName
  type ModuleName = lfdata.Ref.ModuleName
  val ModuleName = lfdata.Ref.ModuleName
  type LedgerString = lfdata.Ref.LedgerString
  val LedgerString = lfdata.Ref.LedgerString
  type ChoiceName = lfdata.Ref.ChoiceName
  val ChoiceName = lfdata.Ref.ChoiceName
  type PackageId = lfdata.Ref.PackageId
  val PackageId = lfdata.Ref.PackageId
  type WitnessRelation[A] = lfdata.Relation.Relation[A, Party]
  type DisclosureRelation = WitnessRelation[NodeId]
  type DivulgenceRelation = WitnessRelation[ContractId]
  private[dao] type FilterRelation = lfdata.Relation.Relation[Party, lfdata.Ref.Identifier]
  val Relation = lfdata.Relation.Relation

  import com.daml.lf.crypto
  type Hash = crypto.Hash

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
  def groupContiguous[A, K, Mat](
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
  def route[A, B](
      set: Set[A]
  )(single: A => B, multi: Set[A] => B): B = {
    assume(set.nonEmpty, "Empty set, unable to dispatch to single/multi implementation")
    set.size match {
      case 1 => single(set.iterator.next())
      case n if n > 1 => multi(set)
    }
  }

  def convert(template: Identifier, key: lftx.Node.KeyWithMaintainers[Value]): Key =
    Key.assertBuild(template, key.key.value)

  def convertLfValueKey(
      template: Identifier,
      key: KeyWithMaintainers[lfval[ContractId]],
  ) =
    Key.assertBuild(template, key.key)

  def batch(query: String, parameters: Seq[Seq[NamedParameter]]): Option[BatchSql] =
    if (parameters.isEmpty) None else Some(BatchSql(query, parameters.head, parameters.tail: _*))

}
