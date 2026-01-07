// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

final case class ObservedCreateEvent(
    templateName: String,
    createArgumentsSerializedSize: Int,
    interfaceViews: Seq[ObservedInterfaceView],
    offset: Long,
    contractId: String,
)

object ObservedCreateEvent {
  def apply(
      created: com.daml.ledger.api.v2.event.CreatedEvent
  ): ObservedCreateEvent = {
    val argsSize = created.createArguments.fold(0)(_.serializedSize)
    val templateName =
      created.templateId.getOrElse(sys.error(s"Expected templateId in $created")).entityName
    ObservedCreateEvent(
      templateName = templateName,
      createArgumentsSerializedSize = argsSize,
      interfaceViews = created.interfaceViews.map(ObservedInterfaceView.apply),
      offset = created.offset,
      contractId = created.contractId,
    )
  }
}
