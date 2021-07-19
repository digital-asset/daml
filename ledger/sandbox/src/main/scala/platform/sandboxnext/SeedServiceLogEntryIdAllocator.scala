// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandboxnext

import java.util.UUID

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.daml.ledger.validator.LogEntryIdAllocator
import com.daml.platform.apiserver.SeedService
import com.google.protobuf.ByteString

final class SeedServiceLogEntryIdAllocator(seedService: SeedService) extends LogEntryIdAllocator {
  override def allocate(): DamlLogEntryId = {
    val seed = seedService.nextSeed().bytes.toByteArray
    DamlLogEntryId.newBuilder
      .setEntryId(ByteString.copyFromUtf8(UUID.nameUUIDFromBytes(seed).toString))
      .build()
  }
}
