// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.protocol.LfSerializationVersion

import scala.collection.immutable.SortedMap
import scala.math.Ordered.orderingToOrdered

object LfSerializationVersionToProtocolVersions {

  /** This Map links the Daml Lf-version to the minimum protocol version that supports it. */
  val lfSerializationVersionToMinimumProtocolVersions
      : SortedMap[LfSerializationVersion, ProtocolVersion] =
    SortedMap(
      LfSerializationVersion.V1 -> ProtocolVersion.v34,
      LfSerializationVersion.VDev -> ProtocolVersion.dev,
    )

  def getMinimumSupportedProtocolVersion(
      serializationVersion: LfSerializationVersion
  ): ProtocolVersion = {
    assert(
      serializationVersion >= LfSerializationVersion.V1,
      s"Canton only supports LF serialization versions more recent or equal to ${LfSerializationVersion.V1}",
    )
    lfSerializationVersionToMinimumProtocolVersions(serializationVersion)
  }

}
