// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.store.DamlLogEntryId

trait LogEntryIdAllocator {
  def allocate(): DamlLogEntryId
}
