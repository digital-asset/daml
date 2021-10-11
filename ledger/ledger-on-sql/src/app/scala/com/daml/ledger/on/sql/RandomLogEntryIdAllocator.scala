// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.util.UUID

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.daml.ledger.validator.LogEntryIdAllocator
import com.google.protobuf.ByteString

// This is intended to be used only in testing, and is therefore a trivial implementation.
object RandomLogEntryIdAllocator extends LogEntryIdAllocator {
  override def allocate(): DamlLogEntryId =
    DamlLogEntryId.newBuilder
      .setEntryId(ByteString.copyFromUtf8(UUID.randomUUID().toString))
      .build()
}
