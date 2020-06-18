// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools

import java.io.{DataInputStream, FileInputStream}

object IntegrityCheckV2 {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("usage: integrity-check <ledger dump file>")
      println(
        "You can produce a ledger dump on a kvutils ledger by setting KVUTILS_LEDGER_DUMP=/path/to/file")
      sys.exit(1)
    }

    val filename = args(0)
    println(s"Verifying integrity of $filename...")

    val ledgerDumpStream: DataInputStream =
      new DataInputStream(new FileInputStream(filename))
    new IntegrityChecker().run(ledgerDumpStream)
    sys.exit(0)
  }
}
