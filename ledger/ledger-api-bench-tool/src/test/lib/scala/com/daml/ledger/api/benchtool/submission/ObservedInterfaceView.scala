// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

case class ObservedInterfaceView(interfaceName: String, serializedSize: Int)
object ObservedInterfaceView {
  def apply(interfaceView: com.daml.ledger.api.v1.event.InterfaceView): ObservedInterfaceView = {
    val interfaceName =
      interfaceView.interfaceId
        .getOrElse(sys.error(s"Expected interfaceId in $interfaceView"))
        .entityName
    val serializedSize = interfaceView.serializedSize
    ObservedInterfaceView(interfaceName, serializedSize)
  }
}
