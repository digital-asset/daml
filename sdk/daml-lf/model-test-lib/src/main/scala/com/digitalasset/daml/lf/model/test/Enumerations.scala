// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import cats.Applicative
import com.daml.lf.Spaces.Space
import com.daml.lf.Spaces.{Space => S}
import com.daml.lf.Spaces.Space.Instances._
import cats.syntax.all._
import com.daml.lf.model.test.Ledgers._

object Enumerations {

//  Commands := Commands Parties [TopLevelAction]
//
//  TopLevelAction := Create | Exercise Action* | CreateAndExercise Action*
//  Action := Create | Exercise Action* | Fetch | Rollback ActionWithoutRollback+
//  ActionWithoutRollback := Create | Exercise Action* | Fetch

  val AS: Applicative[Space] = implicitly

  def listsOf[A](s: Space[A]): Space[List[A]] = {
    lazy val res: Space[List[A]] = S.pay(S.singleton(List.empty[A]) + AS.map2(s, res)(_ :: _))
    res
  }

  def nonEmptyListOf[A](s: Space[A]): Space[List[A]] = {
    S.pay(AS.map2(s, listsOf(s))(_ :: _))
  }

  lazy val exerciceKinds: Space[ExerciseKind] =
    S.singleton[ExerciseKind](Consuming) + S.singleton[ExerciseKind](NonConsuming)

  lazy val creates: Space[Action] =
    AS.map3(S.singleton(0), S.singleton(Set.empty[PartyId]), S.singleton(Set.empty[PartyId]))(
      Create
    )

  lazy val exercises: Space[Action] =
    AS.map5(
      exerciceKinds,
      S.singleton(0),
      S.singleton(Set.empty[PartyId]),
      S.singleton(Set.empty[PartyId]),
      listsOf(actions),
    )(Exercise)

  lazy val fetches: Space[Action] =
    AS.map(S.singleton(0))(Fetch)

  lazy val rollbacks: Space[Action] =
    AS.map(nonEmptyListOf(actionsWithoutRollback))(Rollback)

  lazy val actions: Space[Action] =
    S.pay(creates + exercises + fetches + rollbacks)

  lazy val actionsWithoutRollback: Space[Action] =
    S.pay(creates + exercises + fetches)

  lazy val topLevelActions: Space[Action] =
    S.pay(creates + exercises)

  lazy val commands: Space[Commands] =
    AS.map2(S.singleton(Set.empty[PartyId]), nonEmptyListOf(topLevelActions))(Commands)

  lazy val ledgers: Space[Ledger] =
    listsOf(commands)

  def ledgersOfSize(n: Int): Space[Ledger] =
    List.fill(n)(commands).sequence
}
