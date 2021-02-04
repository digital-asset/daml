// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.Envelope
import com.daml.ledger.validator.reading.{DamlLedgerStateReader, LedgerStateReader}

object SerializingStateReader {

  def apply(keySerializationStrategy: StateKeySerializationStrategy)(
      delegate: LedgerStateReader
  ): DamlLedgerStateReader =
    delegate
      .contramapKeys(keySerializationStrategy.serializeStateKey)
      .mapValues(value =>
        value.map(
          Envelope
            .openStateValue(_)
            .getOrElse(sys.error("Opening enveloped DamlStateValue failed"))
        )
      )
}
