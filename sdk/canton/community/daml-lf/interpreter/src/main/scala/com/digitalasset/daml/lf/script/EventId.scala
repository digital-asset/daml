// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package script

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.transaction.NodeId

// transactionId should be small so the concatenation in toLedgerString does not exceed 255 chars
case class EventId(
    transactionOffset: Long,
    nodeId: NodeId,
) {
  lazy val toLedgerString: LedgerString = {
    val builder = (new StringBuilder()
      += '#'
      ++= transactionOffset.toString
      += ':'
      ++= nodeId.index.toString)
    LedgerString.assertFromString(builder.result())
  }
}
