// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.typesig.reader

import java.io.BufferedInputStream
import java.nio.file.Files

import com.daml.lf2.archive.daml_lf_dev.DamlLf

object SignatureReaderMain extends App {

  val lfFile = new java.io.File(args.apply(0))

  val is = Files.newInputStream(lfFile.toPath)
  try {
    val bis = new BufferedInputStream(is)
    val archive = DamlLf.Archive.parser().parseFrom(bis)
    val out = SignatureReader.readPackageSignature(archive)
    println(s"out: $out")
  } finally {
    is.close()
  }
}
