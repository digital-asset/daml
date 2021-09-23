// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.iface.reader

import java.io.BufferedInputStream
import java.nio.file.Files

import com.daml.daml_lf.ArchiveOuterClass.Archive

object InterfaceReaderMain extends App {

  val lfFile = new java.io.File(args.apply(0))

  val is = Files.newInputStream(lfFile.toPath)
  try {
    val bis = new BufferedInputStream(is)
    val archive = Archive.parser().parseFrom(bis)
    val out = InterfaceReader.readInterface(archive)
    println(s"out: $out")
  } finally {
    is.close()
  }
}
