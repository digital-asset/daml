// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.protocol.LfTransactionVersion
import com.digitalasset.daml.lf.language.LanguageVersion

import scala.collection.immutable.SortedMap
import scala.math.Ordered.orderingToOrdered

object DamlLfVersionToProtocolVersions {

  /** This Map links the Daml Lf-version to the minimum protocol version that supports it. */
  val damlLfVersionToMinimumProtocolVersions: SortedMap[LfTransactionVersion, ProtocolVersion] =
    SortedMap(
      LanguageVersion.v2_1 -> ProtocolVersion.v32,
      LanguageVersion.v2_dev -> ProtocolVersion.dev,
    )

  def getMinimumSupportedProtocolVersion(
      transactionVersion: LfTransactionVersion
  ): ProtocolVersion = {
    assert(
      transactionVersion >= LanguageVersion.v2_1,
    s"Canton only supports transaction versions more recent or equal to ${LanguageVersion.v2_1 }",
    )
    damlLfVersionToMinimumProtocolVersions(transactionVersion)
  }

}
