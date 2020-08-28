// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream}

final class SerializationBasedLedgerDataExportSpec
    extends LedgerDataExportSpecBase("serialization-based export") {

  override protected def newExporter(outputStream: OutputStream): LedgerDataExporter =
    new SerializationBasedLedgerDataExporter(new DataOutputStream(outputStream))

  override protected def newImporter(inputStream: InputStream): LedgerDataImporter =
    new SerializationBasedLedgerDataImporter(new DataInputStream(inputStream))
}
