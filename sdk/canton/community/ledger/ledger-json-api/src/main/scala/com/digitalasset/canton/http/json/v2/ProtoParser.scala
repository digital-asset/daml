// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.digitalasset.canton.http.json.v2.ProtoParser.{camelToSnake, normalizeName}
import io.protostuff.compiler.model.{FieldContainer, Proto}
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

/** Parses protofiles in order to extract comments (and put into openapi).
  *
  * The algorithms used are inefficient, but since the code is used only to generate documentation
  * it should not cause any problems.
  */
class ProtoParser {

  private val ledgerApiProtoLocation = "com/daml/ledger/api/v2"

  // we need to load any proto file in order to get jarResource (to scan for others)
  private val startingProtoFile = s"$ledgerApiProtoLocation/transaction.proto"

  def readProto(): ProtoInfo = {

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
    new ProtoInfo(protos)
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

class ProtoInfo(protos: Seq[Proto]) {

  private val (messages, oneOfMessages) = extractMessages()

  def findMessageInfo(msgName: String): Option[MessageInfo] = messages
    .get(msgName)
    .orElse(messages.get(normalizeName(msgName)))
    .orElse(oneOfMessages.get(camelToSnake(normalizeName(msgName))))

  private def extractMessages(): (Map[String, MessageInfo], Map[String, MessageInfo]) = {
    val messages = protos.flatMap(_.getMessages().asScala)
    val componentsMessages = messages.map(msg => (msg.getName(), new MessageInfo(msg))).toMap
    val oneOfMessages = messages
      .flatMap(_.getOneofs().asScala)
      .map(msg => (msg.getName(), new MessageInfo(msg)))
      .map { case (name, msgInfo) =>
        (camelToSnake(normalizeName(name)), msgInfo)
      }
      .toMap
    (componentsMessages, oneOfMessages)
  }
}

class MessageInfo(message: FieldContainer) {
  def getComments(): Option[String] = Option(message.getComments).filter(_.nonEmpty)

  def getFieldComment(name: String): Option[String] =
    Option(message.getField(name))
      .orElse(Option(message.getField(camelToSnake(name))))
      .map(_.getComments)
      .filter(!_.isEmpty)

}

object ProtoParser {

  def camelToSnake(name: String): String =
    name
      .replaceAll("([a-z0-9])([A-Z])", "$1_$2")
      .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
      .toLowerCase

  /** We drop initial `Js` prefix and single digits suffixes.
    */
  def normalizeName(s: String): String =
    if (s.nonEmpty && s.last.isDigit) s.dropRight(1) else if (s.startsWith("Js")) s.drop(2) else s

}
