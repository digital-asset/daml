// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.common

import com.daml.lf.transaction.{Node => N}
import com.daml.lf.{engine => E}
import com.daml.lf.data.Ref
import scala.collection.breakOut

object PlatformTypes {

  type GenNode[Nid, Cid] = N.GenNode[Nid, Cid]

  type NodeCreate[Cid] = N.NodeCreate[Cid]
  val NodeCreate: N.NodeCreate.type = N.NodeCreate

  type NodeLookupByKey[Cid] = N.NodeLookupByKey[Cid]
  val NodeLookupByKey: N.NodeLookupByKey.type = N.NodeLookupByKey

  type NodeFetch[Cid] = N.NodeFetch[Cid]
  val NodeFetch: N.NodeFetch.type = N.NodeFetch

  type NodeExercises[Nid, Cid] = N.NodeExercises[Nid, Cid]
  val NodeExercises: N.NodeExercises.type = N.NodeExercises

  type Event[Nid, Cid] = E.Event[Nid, Cid]

  type Events[Nid, Cid] = E.Event.Events[Nid, Cid]
  val Events: E.Event.Events.type = E.Event.Events

  type CreateEvent[Cid] = E.CreateEvent[Cid]
  val CreateEvent: E.CreateEvent.type = E.CreateEvent

  type ExerciseEvent[Nid, Cid] = E.ExerciseEvent[Nid, Cid]
  val ExerciseEvent: E.ExerciseEvent.type = E.ExerciseEvent

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
