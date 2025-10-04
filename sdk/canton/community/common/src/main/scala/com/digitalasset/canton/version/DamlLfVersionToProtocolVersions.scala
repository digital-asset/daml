// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.protocol.LfLanguageVersion

import scala.collection.immutable.SortedMap
import scala.math.Ordered.orderingToOrdered

object DamlLfVersionToProtocolVersions {

  /** This Map links the Daml Lf-version to the minimum protocol version that supports it. */
  val damlLfVersionToMinimumProtocolVersions: SortedMap[LfLanguageVersion, ProtocolVersion] =
    SortedMap(
      LfLanguageVersion.v2_1 -> ProtocolVersion.v34,
      LfLanguageVersion.v2_dev -> ProtocolVersion.dev,
    )

  def getMinimumSupportedProtocolVersion(
      transactionVersion: LfLanguageVersion
  ): ProtocolVersion = {
    assert(
      transactionVersion >= LfLanguageVersion.v2_1,
      s"Canton only supports transaction versions more recent or equal to ${LfLanguageVersion.v2_1}",
    )
    damlLfVersionToMinimumProtocolVersions(transactionVersion)
  }

}
