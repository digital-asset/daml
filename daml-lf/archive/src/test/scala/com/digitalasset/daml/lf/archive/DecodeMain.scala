// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import java.io.File
import java.util.concurrent.TimeUnit

import com.daml.lf.data.Ref
import com.daml.lf.language.Ast

/** Test application for decoding DARs. Useful for testing decoder performance and memory usage.
  */
object DecodeMain extends App {
  if (args.length != 1) {
    println("usage: decode <dar file>")
    System.exit(1)
  }

  def toMillis(a: Long, b: Long) = TimeUnit.NANOSECONDS.toMillis(b - a)

  (1 to 3).foreach { _ =>
    val t0 = System.nanoTime()

    val archives = DarReader.assertReadArchiveFromFile(new File(args(0)))
    val t1 = System.nanoTime()

    val _: (Ref.PackageId, Ast.Package) = Decode.assertDecodeArchivePayload(archives.main)
    val t2 = System.nanoTime()

    println(s"parseFrom in ${toMillis(t0, t1)}ms, decoded in ${toMillis(t1, t2)}ms.")

    // Wait a while to allow for running e.g. jmap -heap etc.
    // val pid = Integer.parseInt(new File("/proc/self").getCanonicalFile.getName)
    // println(s"sleeping 5s, pid is $pid.")
    // Thread.sleep(5000)

    System.gc()
  }
}
