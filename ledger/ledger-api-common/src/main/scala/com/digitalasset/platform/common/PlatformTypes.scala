// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.common

import com.daml.lf.transaction.{Node => N}
import com.daml.lf.value.{ValueVersions, Value => V}
import com.daml.lf.{transaction => T}
import com.daml.lf.{engine => E}
import com.daml.lf.data.Ref
import scala.collection.breakOut

object PlatformTypes {

  type GenTransaction[Nid, Cid] = T.GenTransaction.WithTxValue[Nid, Cid]
  val GenTransaction: T.GenTransaction.type = T.GenTransaction

  type GenNode[Nid, Cid] = N.GenNode.WithTxValue[Nid, Cid]

  type NodeCreate[Cid] = N.NodeCreate.WithTxValue[Cid]
  val NodeCreate: N.NodeCreate.type = N.NodeCreate

  type NodeLookupByKey[Cid] = N.NodeLookupByKey.WithTxValue[Cid]
  val NodeLookupByKey: N.NodeLookupByKey.type = N.NodeLookupByKey

  type NodeFetch[Cid] = N.NodeFetch.WithTxValue[Cid]
  val NodeFetch: N.NodeFetch.type = N.NodeFetch

  type NodeExercises[Nid, Cid] = N.NodeExercises.WithTxValue[Nid, Cid]
  val NodeExercises: N.NodeExercises.type = N.NodeExercises

  type Event[Nid, Cid] = E.Event[Nid, Cid, T.Transaction.Value[Cid]]

  type Events[Nid, Cid] = E.Event.Events[Nid, Cid, T.Transaction.Value[Cid]]
  val Events: E.Event.Events.type = E.Event.Events

  type CreateEvent[Cid] = E.CreateEvent[Cid, T.Transaction.Value[Cid]]
  val CreateEvent: E.CreateEvent.type = E.CreateEvent

  type ExerciseEvent[Nid, Cid] = E.ExerciseEvent[Nid, Cid, T.Transaction.Value[Cid]]
  val ExerciseEvent: E.ExerciseEvent.type = E.ExerciseEvent

  @deprecated("use resolveRelCid/ensureNoCid/ensureNoRelCid", since = "0.13.52")
  def mapContractIdAndValue[Nid, Cid, Cid2](tx: GenTransaction[Nid, Cid])(
      f: Cid => Cid2): GenTransaction[Nid, Cid2] =
    tx.mapContractIdAndValue(f, _.mapContractId(f))

  def asVersionedValue[Cid <: V.AbsoluteContractId](
      v: V[Cid]): scala.Either[String, V.VersionedValue[Cid]] =
    ValueVersions.asVersionedValue(v)

  def asVersionedValueOrThrow[Cid <: V.AbsoluteContractId](v: V[Cid]): V.VersionedValue[Cid] = {
    asVersionedValue(v).fold(
      s => throw new IllegalArgumentException(s"Can't convert to versioned value: $s"),
      identity)
  }

  def packageId(str: String): Ref.PackageId = Ref.PackageId.assertFromString(str)

  def dn(str: String): Ref.DottedName = Ref.DottedName.assertFromString(str)

  def mn(str: String): Ref.ModuleName = Ref.ModuleName.assertFromString(str)

  def qn(str: String): Ref.QualifiedName = Ref.QualifiedName.assertFromString(str)

  def party(str: String): Ref.Party = Ref.Party.assertFromString(str)

  def parties(as: Iterable[String]): Set[Ref.Party] = as.map(a => party(a))(breakOut)

  def ss(str: String): Ref.PackageId = Ref.PackageId.assertFromString(str)

  def identifier(aPackageId: String, name: String): Ref.Identifier =
    Ref.Identifier(packageId(aPackageId), qn(name))

}
