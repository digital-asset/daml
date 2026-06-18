// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.proto

import com.digitalasset.canton.http.json.v2.ExtractedProtoComments
import io.protostuff.compiler.parser.{
  ClasspathFileReader,
  FileDescriptorLoaderImpl,
  FileReader,
  ImporterImpl,
  LocalFileReader,
  ParseErrorLogger,
}

import java.net.JarURLConnection
import java.nio.file.{Files, Path, Paths}
import java.util.jar.JarFile
import scala.jdk.CollectionConverters.*
import scala.util.Using

object ProtoParser {

  private val ledgerApiProtoLocation = "com/daml/ledger/api/v2"

  // we need to load any proto file in order to get jarResource (to scan for others)
  private val startingProtoFile = s"$ledgerApiProtoLocation/transaction.proto"

  def readProto(): ExtractedProtoComments = {

    val classLoader = Thread.currentThread.getContextClassLoader
    val url = classLoader.getResource(startingProtoFile)
    val resourceConnection = url.openConnection
    val (protoFiles, fileReader): (Seq[String], FileReader) = resourceConnection match {
      case jarResource: JarURLConnection =>
        Using(jarResource.getJarFile()) { jarFile =>
          val entries =
            findProtoFilesInJar(jarFile)
          (entries.map(entry => entry.getRealName()), new ClasspathFileReader())
        }.fold(cause => throw new IllegalStateException(cause), identity)
      case fileResource =>
        val protoMainPath = Paths.get(fileResource.getURL().getPath()).getParent()
        (findProtoFilesInFileSystem(protoMainPath), new LocalFileReader(protoMainPath))
    }

    val errorListener = new ParseErrorLogger()
    val fdLoader = new FileDescriptorLoaderImpl(errorListener, Set.empty.asJava)
    val importer = new ImporterImpl(fdLoader)

    val protos = protoFiles.map { pf =>
      val protoCtx = importer.importFile(fileReader, pf)
      protoCtx.getProto()
    }
    ProtoDescriptionExtractor.extract(protos)
  }

  private def findProtoFilesInJar(jarFile: JarFile) =
    jarFile
      .entries()
      .asScala
      .filter(entry =>
        entry
          .getRealName()
          .startsWith(ledgerApiProtoLocation) && entry.getRealName().endsWith(".proto")
      )
      .toSeq

  private def findProtoFilesInFileSystem(protoFolder: Path, prefix: String = ""): Seq[String] =
    Files
      .list(protoFolder)
      .toList
      .asScala
      .toSeq
      .flatMap { f =>
        if (Files.isRegularFile(f) && f.getFileName().toString().endsWith(".proto")) {
          Seq(f.getFileName().toString()).map(f => s"$prefix$f")
        } else if (Files.isDirectory(f)) {
          findProtoFilesInFileSystem(f, s"$prefix${f.getFileName()}/")
        } else Seq.empty
      }
}
