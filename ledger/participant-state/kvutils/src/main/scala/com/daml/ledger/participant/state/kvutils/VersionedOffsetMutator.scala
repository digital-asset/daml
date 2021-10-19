// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.offset.Offset

/** Modifies specific offset fields while preserving all other fields.
  */
object VersionedOffsetMutator {
  def zeroLowest(offset: Offset): Offset =
    setLowest(offset, 0)

  def setLowest(offset: Offset, newLowest: Int): Offset = {
    val versionedOffset = VersionedOffset(offset)
    new VersionedOffsetBuilder(versionedOffset.version)
      .of(versionedOffset.highest, versionedOffset.middle, newLowest)
  }
}
