// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto.rlp

import java.util.ArrayList

/**
 * @author Roman Mandeleil
 * @since 21.04.14
 */
object RLPList {
  def recursivePrint(element: Nothing): Unit = {
    if (element == null) throw new RuntimeException("RLPElement object can't be null")
    element match {
      case rlpList: RLPList =>
        print("[")
        for (singleElement <- rlpList) {
          recursivePrint(singleElement)
        }
        print("]")
      case _ =>
        val hex = ByteUtil.toHexString(element.getRLPData)
        print(hex + ", ")
    }
  }
}

class RLPList extends ArrayList[RLPElement] with RLPElement {
  var rlpData: Array[Byte] = null

  def setRLPData(rlpData: Array[Byte]): Unit = {
    this.rlpData = rlpData
  }

  def getRLPData: Array[Byte] = rlpData
}
