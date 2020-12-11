// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.validator.reading.{DamlLedgerStateReader, LedgerStateReader}

object DamlLedgerStateReader {
  def from(
      ledgerStateReader: LedgerStateReader,
      keySerializationStrategy: StateKeySerializationStrategy,
  ): DamlLedgerStateReader =
    new RawToDamlLedgerStateReaderAdapter(ledgerStateReader, keySerializationStrategy)
}
