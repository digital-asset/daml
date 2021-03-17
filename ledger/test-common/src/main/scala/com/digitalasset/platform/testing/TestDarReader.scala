package com.daml.platform.testing

import java.util.zip.ZipInputStream

import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.archive.{Dar, DarReader}

import scala.util.Try

object TestDarReader {

  def read(testDarName: String): Try[Dar[DamlLf.Archive]] = {
    val fileName = com.daml.ledger.test.TestDars.fileNames(testDarName)
    val reader = DarReader { (_, stream) => Try(DamlLf.Archive.parseFrom(stream)) }
    reader
      .readArchive(
        fileName,
        new ZipInputStream(this.getClass.getClassLoader.getResourceAsStream(fileName)),
      )
  }
}
