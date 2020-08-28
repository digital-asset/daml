// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.DataInputStream

/**
  * Enables importing ledger data from an input stream.
  * This class is thread-safe.
  */
final class SerializationBasedLedgerDataImporter(input: DataInputStream)
    extends LedgerDataImporter {

  override def read(): Stream[(SubmissionInfo, WriteSet)] =
    if (input.available() == 0)
      Stream.empty
    else
      Deserialization.deserializeEntry(input) #:: read()

}
