// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import java.util.UUID

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.google.protobuf.ByteString

trait LogEntryIdAllocator {
  def allocate(): DamlLogEntryId
}

object LogEntryIdAllocator {
  val random: LogEntryIdAllocator = () =>
    DamlLogEntryId.newBuilder
      .setEntryId(ByteString.copyFromUtf8(UUID.randomUUID().toString))
      .build()
}
