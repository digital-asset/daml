// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

case class ObservedCreateEvent(
    templateName: String,
    createArgumentsSerializedSize: Int,
    interfaceViews: Seq[ObservedInterfaceView],
)

object ObservedCreateEvent {
  def apply(created: com.daml.ledger.api.v1.event.CreatedEvent): ObservedCreateEvent = {
    val argsSize = created.createArguments.fold(0)(_.serializedSize)
    val templateName =
      created.templateId.getOrElse(sys.error(s"Expected templateId in $created")).entityName

    ObservedCreateEvent(
      templateName = templateName,
      createArgumentsSerializedSize = argsSize,
      interfaceViews = created.interfaceViews.map(ObservedInterfaceView.apply),
    )
  }
}
