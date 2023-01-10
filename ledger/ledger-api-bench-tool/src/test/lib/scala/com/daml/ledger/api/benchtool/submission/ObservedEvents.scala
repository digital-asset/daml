// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

case class ObservedEvents(
    expectedTemplateNames: Set[String],
    createEvents: Seq[ObservedCreateEvent],
    exerciseEvents: Seq[ObservedExerciseEvent],
) {
  private val _actualTemplateNames =
    (createEvents.map(_.templateName) ++ exerciseEvents.map(_.templateName)).toSet
  require(
    _actualTemplateNames.subsetOf(expectedTemplateNames),
    s"${_actualTemplateNames} must be a subset of $expectedTemplateNames",
  )

  val consumingExercises: Seq[ObservedExerciseEvent] = exerciseEvents.filter(_.consuming)
  val nonConsumingExercises: Seq[ObservedExerciseEvent] = exerciseEvents.filterNot(_.consuming)

  val avgSizeOfConsumingExercise: Int = {
    if (consumingExercises.isEmpty) 0
    else consumingExercises.map(_.choiceArgumentsSerializedSize).sum / consumingExercises.size
  }

  val avgSizeOfNonconsumingExercise: Int = {
    if (nonConsumingExercises.isEmpty) 0
    else
      nonConsumingExercises.map(_.choiceArgumentsSerializedSize).sum / nonConsumingExercises.size
  }

  val numberOfCreatesPerTemplateName: Map[String, Int] = {
    val groups = createEvents.groupBy(_.templateName)
    expectedTemplateNames.map(name => name -> groups.get(name).fold(0)(_.size)).toMap
  }

  val numberOfConsumingExercisesPerTemplateName: Map[String, Int] = {
    val groups = consumingExercises.groupBy(_.templateName)
    expectedTemplateNames.map(name => name -> groups.get(name).fold(0)(_.size)).toMap
  }

  val numberOfNonConsumingExercisesPerTemplateName: Map[String, Int] = {
    val groups = nonConsumingExercises.groupBy(_.templateName)
    expectedTemplateNames.map(name => name -> groups.get(name).fold(0)(_.size)).toMap
  }

  val avgSizeOfCreateEventPerTemplateName: Map[String, Int] = {
    val groups = createEvents.groupBy(_.templateName)
    expectedTemplateNames.map { name =>
      val avgSize = groups
        .get(name)
        .fold(0)(events =>
          if (events.isEmpty) 0 else events.map(_.createArgumentsSerializedSize).sum / events.size
        )
      name -> avgSize
    }.toMap
  }

}
