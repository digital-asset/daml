// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.iface.reader

import java.io.BufferedInputStream
import java.nio.file.Files

import com.digitalasset.daml_lf.DamlLf

object InterfaceReaderMain extends App {

  val lfFile = new java.io.File(args.apply(0))

  val is = Files.newInputStream(lfFile.toPath)
  try {
    val bis = new BufferedInputStream(is)
    val archive = DamlLf.Archive.parser().parseFrom(bis)
    val out = InterfaceReader.readInterface(archive)
    println(s"out: $out")
  } finally {
    is.close()
  }
}
