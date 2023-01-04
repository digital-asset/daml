// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.common

import com.daml.lf.transaction.Node
import com.daml.lf.{engine => E}
import com.daml.lf.data.Ref

object PlatformTypes {

  type GenNode = Node

  type NodeCreate = Node.Create
  val NodeCreate: Node.Create.type = Node.Create

  type NodeLookupByKey = Node.LookupByKey
  val NodeLookupByKey: Node.LookupByKey.type = Node.LookupByKey

  type NodeFetch = Node.Fetch
  val NodeFetch: Node.Fetch.type = Node.Fetch

  type NodeExercises = Node.Exercise
  val NodeExercises: Node.Exercise.type = Node.Exercise

  type Event = E.Event

  type Events = E.Event.Events
  val Events: E.Event.Events.type = E.Event.Events

  type CreateEvent = E.CreateEvent
  val CreateEvent: E.CreateEvent.type = E.CreateEvent

  type ExerciseEvent = E.ExerciseEvent
  val ExerciseEvent: E.ExerciseEvent.type = E.ExerciseEvent

  def packageId(str: String): Ref.PackageId = Ref.PackageId.assertFromString(str)

  def dn(str: String): Ref.DottedName = Ref.DottedName.assertFromString(str)

  def mn(str: String): Ref.ModuleName = Ref.ModuleName.assertFromString(str)

  def qn(str: String): Ref.QualifiedName = Ref.QualifiedName.assertFromString(str)

  def party(str: String): Ref.Party = Ref.Party.assertFromString(str)

  def parties(as: Iterable[String]): Set[Ref.Party] = as.view.map(a => party(a)).toSet

  def ss(str: String): Ref.PackageId = Ref.PackageId.assertFromString(str)

  def identifier(aPackageId: String, name: String): Ref.Identifier =
    Ref.Identifier(packageId(aPackageId), qn(name))

}
