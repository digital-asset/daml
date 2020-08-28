// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.{InputStream, OutputStream}

final class ProtobufBasedLedgerDataExportSpec
    extends LedgerDataExportSpecBase("protocol buffers-based export") {

  override protected def newExporter(outputStream: OutputStream): LedgerDataExporter =
    new ProtobufBasedLedgerDataExporter(outputStream)

  override protected def newImporter(inputStream: InputStream): LedgerDataImporter =
    new ProtobufBasedLedgerDataImporter(inputStream)
}
