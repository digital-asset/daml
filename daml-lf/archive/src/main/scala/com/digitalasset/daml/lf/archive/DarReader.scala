// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive
import java.io.InputStream
import java.util
import java.util.zip.{ZipEntry, ZipFile}

import scala.collection.JavaConverters._
import scala.util.Try

class DarReader[A](parse: InputStream => A) {
  def readArchive(darFile: ZipFile): Try[List[A]] = {
    import com.digitalasset.daml.lf.archive.TryOps.sequence
    val collector = util.stream.Collectors.toList[Try[A]]
    val list: util.List[Try[A]] = darFile.stream
      .filter(isDalfEntry)
      .map[Try[A]](e => parseEntry(darFile, e))
      .collect(collector)
    sequence(list.asScala.toList)
  }

  private def isDalfEntry(e: ZipEntry): Boolean = e.getName.endsWith(".dalf")

  private def parseEntry(f: ZipFile, e: ZipEntry): Try[A] = Try {
    val is = f.getInputStream(e)
    try {
      parse(is)
    } finally {
      is.close()
    }
  }
}

object DarReader extends DarReader(Reader.decodeArchiveFromInputStream)

object DarReaderWithVersion extends DarReader(Reader.readArchiveAndVersion)
