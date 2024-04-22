// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import cats.Applicative
import com.daml.lf.Spaces.Space
import com.daml.lf.Spaces.{Space => S}
import com.daml.lf.Spaces.Space.Instances._
import cats.syntax.all._
import com.daml.lf.language.LanguageVersion
import com.daml.lf.language.LanguageVersion.Features
import com.daml.lf.model.test.Skeletons._

import Ordering.Implicits.infixOrderingOps

class Enumerations(languageVersion: LanguageVersion) {

//  Commands := Commands Parties [TopLevelAction]
//
//  TopLevelAction := Create | Exercise Action* | CreateAndExercise Action*
//  Action := Create | Exercise Action* | Fetch | Rollback ActionWithoutRollback+
//  ActionWithoutRollback := Create | Exercise Action* | Fetch

  val AS: Applicative[Space] = implicitly

  def withFeature[A](feature: LanguageVersion)(s: Space[A]): Space[A] =
    if (languageVersion >= feature) s
    else Space.empty[A]

  def listsOf[A](s: Space[A]): Space[List[A]] = {
    lazy val res: Space[List[A]] = S.pay(S.singleton(List.empty[A]) + AS.map2(s, res)(_ :: _))
    res
  }

  def nonEmptyListOf[A](s: Space[A]): Space[List[A]] = {
    S.pay(AS.map2(s, listsOf(s))(_ :: _))
  }

  lazy val exerciceKinds: Space[ExerciseKind] =
    S.singleton[ExerciseKind](Consuming) + S.singleton[ExerciseKind](NonConsuming)

  lazy val creates: Space[Action] = S.singleton(Create())

  lazy val createsWithKey: Space[Action] = withFeature(Features.contractKeys) {
    S.singleton(CreateWithKey())
  }

  lazy val exercises: Space[Action] =
    AS.map2(
      exerciceKinds,
      listsOf(actions),
    )(Exercise)

  lazy val exercisesByKey: Space[Action] = withFeature(Features.contractKeys) {
    AS.map2(
      exerciceKinds,
      listsOf(actions),
    )(ExerciseByKey)
  }

  lazy val fetches: Space[Action] =
    S.singleton(Fetch())

  lazy val rollbacks: Space[Action] =
    AS.map(nonEmptyListOf(actionsWithoutRollback))(Rollback)

  lazy val actions: Space[Action] =
    S.pay(creates + createsWithKey + exercises + exercisesByKey + fetches + rollbacks)

  lazy val actionsWithoutRollback: Space[Action] =
    S.pay(creates + createsWithKey + exercises + exercisesByKey + fetches)

  lazy val topLevelActions: Space[Action] =
    S.pay(creates + createsWithKey + exercises + exercisesByKey)

  lazy val commands: Space[Commands] =
    AS.map(nonEmptyListOf(topLevelActions))(Commands)

  lazy val ledgers: Space[Ledger] =
    listsOf(commands)

  def scenarios(numParticipants: Int): Space[Scenario] = {
    val topology = Seq.fill(numParticipants)(Participant())
    ledgers.map(Scenario(topology, _))
  }

  def scenarios(numParticipants: Int, numCommands: Int): Space[Scenario] = {
    val topology = Seq.fill(numParticipants)(Participant())
    List.fill(numCommands)(commands).sequence.map(Scenario(topology, _))
  }
}
