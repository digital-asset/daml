// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

final case class ObservedInterfaceView(interfaceName: String, serializedSize: Int)
object ObservedInterfaceView {
  def apply(interfaceView: com.daml.ledger.api.v2.event.InterfaceView): ObservedInterfaceView = {
    val interfaceName =
      interfaceView.interfaceId
        .getOrElse(sys.error(s"Expected interfaceId in $interfaceView"))
        .entityName
    val serializedSize = interfaceView.serializedSize
    ObservedInterfaceView(interfaceName, serializedSize)
  }
}
