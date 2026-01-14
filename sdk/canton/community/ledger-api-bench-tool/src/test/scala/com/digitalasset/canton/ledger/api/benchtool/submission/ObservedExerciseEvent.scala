// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

final case class ObservedExerciseEvent(
    templateName: String,
    choiceName: String,
    choiceArgumentsSerializedSize: Int,
    consuming: Boolean,
    offset: Long,
    contractId: String,
)
object ObservedExerciseEvent {
  def apply(
      exercised: com.daml.ledger.api.v2.event.ExercisedEvent,
      offset: Long,
  ): ObservedExerciseEvent = {
    val argsSize = exercised.choiceArgument.fold(0)(_.serializedSize)
    val templateName = exercised.templateId
      .getOrElse(sys.error(s"Expected templateId in $exercised"))
      .entityName
    val contractId = exercised.contractId
    val choiceName = exercised.choice
    ObservedExerciseEvent(
      templateName = templateName,
      choiceName = choiceName,
      choiceArgumentsSerializedSize = argsSize,
      consuming = exercised.consuming,
      offset = offset,
      contractId = contractId,
    )
  }
}
