// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

case class ObservedExerciseEvent(
    templateName: String,
    choiceName: String,
    choiceArgumentsSerializedSize: Int,
    consuming: Boolean,
)
object ObservedExerciseEvent {
  def apply(exercised: com.daml.ledger.api.v1.event.ExercisedEvent): ObservedExerciseEvent = {
    val argsSize = exercised.choiceArgument.fold(0)(_.serializedSize)
    val templateName = exercised.templateId
      .getOrElse(sys.error(s"Expected templateId in $exercised"))
      .entityName
    val choiceName = exercised.choice
    ObservedExerciseEvent(
      templateName = templateName,
      choiceName = choiceName,
      choiceArgumentsSerializedSize = argsSize,
      consuming = exercised.consuming,
    )
  }
}
