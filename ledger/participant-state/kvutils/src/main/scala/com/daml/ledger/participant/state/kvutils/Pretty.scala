// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.google.common.io.BaseEncoding

object Pretty {

  /** Pretty-printing of the entry identifier. Uses the same hexadecimal encoding as is used
    * for absolute contract identifiers.
    */
  def prettyEntryId(entryId: DamlLogEntryId): String =
    BaseEncoding.base16.encode(entryId.getEntryId.toByteArray)

}
