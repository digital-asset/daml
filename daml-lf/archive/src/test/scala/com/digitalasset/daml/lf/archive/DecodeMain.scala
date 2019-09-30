// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import java.io.File
import java.util.concurrent.TimeUnit

import com.digitalasset.daml_lf.DamlLf.Archive

import scala.util.Try

/**
  * Test application for decoding DARs. Useful for testing decoder performance and memory usage.
  */
object DecodeMain extends App {
  /*if (args.length != 1) {
    println("usage: decode <dar file>")
    System.exit(1)
  }*/

  println("waiting 10s")
  Thread.sleep(10000)

  val file = new File("/tmp/spider-dev.dar")

  //(1 to 3).foreach { _ =>
  val t0 = System.nanoTime()

  val archives =
    DarReader { case (_, x) => Try(Archive.parseFrom(x)) }
      .readArchiveFromFile(file) // new File(args(0)))
      .get
  val t1 = System.nanoTime()

  val _ = Decode.decodeArchive(archives.main)
  val t2 = System.nanoTime()

  def toMillis(a: Long, b: Long) = TimeUnit.NANOSECONDS.toMillis(b - a)

  println(s"parseFrom in ${toMillis(t0, t1)}ms, decoded in ${toMillis(t1, t2)}ms.")

  //  System.gc()
  //}

  val pid = Integer.parseInt(new File("/proc/self").getCanonicalFile.getName)
  println(s"waiting 5s to exit. pid is $pid.")

  Thread.sleep(5000)
}
