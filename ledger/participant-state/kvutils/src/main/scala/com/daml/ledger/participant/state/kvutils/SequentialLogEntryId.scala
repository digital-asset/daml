// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.util.concurrent.atomic.AtomicLong

import com.daml.ledger.participant.state.kvutils.store.DamlLogEntryId
import com.google.protobuf.ByteString

class SequentialLogEntryId(prefix: String) {
  private val currentEntryId = new AtomicLong()
  private val prefixBytes = ByteString.copyFromUtf8(prefix)

  def next(): DamlLogEntryId = {
    val entryId = currentEntryId.getAndIncrement().toHexString
    DamlLogEntryId.newBuilder
      .setEntryId(prefixBytes.concat(ByteString.copyFromUtf8(entryId)))
      .build
  }
}
