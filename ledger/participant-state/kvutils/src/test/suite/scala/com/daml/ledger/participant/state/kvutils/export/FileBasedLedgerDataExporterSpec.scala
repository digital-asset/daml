// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.{DataOutputStream, OutputStream}

final class FileBasedLedgerDataExporterSpec
    extends LedgerDataExporterSpecBase[FileBasedLedgerDataExporter] {

  override protected def implementation(outputStream: OutputStream): LedgerDataExporter =
    new FileBasedLedgerDataExporter(new DataOutputStream(outputStream))

}
