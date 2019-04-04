// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.damllf

import java.io.InputStream
import java.util.zip.{ZipEntry, ZipInputStream}

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.lfpackage.{Ast, Decode}
import com.digitalasset.daml_lf.DamlLf.Archive
import com.typesafe.scalalogging.Logger

import scala.annotation.tailrec
import scala.util.Try
import scala.util.control.NonFatal

object PackageParser {

  private val logger = Logger(this.getClass)

  def parseDarOrDalf(archive: () => InputStream): Either[Throwable, (PackageId, Ast.Package)] = { //TODO DEL-7200 clean this up
    val darDecodingResult =
      handleDar(archive())(damlStream =>
        Try(Decode.decodeArchiveFromInputStream(damlStream)).toEither)
    val dalfDecodingResult = darDecodingResult.toTry.recoverWith {
      case t =>
        logger.warn("Could not parse input as dar file. Falling back to dalf parsing.")
        Try(Decode.decodeArchiveFromInputStream(archive()))
    }.toEither
    dalfDecodingResult.fold({ t =>
      logger.warn("Dalf parsing failed with error", t)
      darDecodingResult.left.map { tt =>
        tt.addSuppressed(t)
        tt
      }
    }, Right(_))
    if (dalfDecodingResult.isLeft) darDecodingResult else dalfDecodingResult
  }

  def getPackageIdFromDalf(dalf: InputStream): Either[Throwable, PackageId] =
    Try(PackageId.assertFromString(Archive.parseFrom(dalf).getHash)).toEither

  def getPackageIdFromDar(dar: InputStream): Either[Throwable, String] =
    handleDar(dar)(damlStream => Try(Archive.parseFrom(damlStream).getHash).toEither)

  private def handleDar[A](dar: InputStream)(
      fun: InputStream => Either[Throwable, A]): Either[Throwable, A] = {
    for {
      zipfile <- try {
        Right(new ZipInputStream(dar))
      } catch {
        case NonFatal(t) => Left(new RuntimeException(s"Error opening input as zip file", t))
      }
      processed <- getDalfs(zipfile, fun, Right(Nil))
      firstProcessed <- processed.headOption
        .fold[Either[Throwable, A]](
          Left(new IllegalArgumentException("No dalf entry found in input")))(Right(_))
    } yield firstProcessed
  }

  private def getDalfs[A](
      zipInputStream: ZipInputStream,
      fun: InputStream => Either[Throwable, A],
      acc: Either[Throwable, List[A]]): Either[Throwable, List[A]] = {
    val entry = zipInputStream.getNextEntry
    if (entry == null || acc.isLeft) acc
    else {
      if (entry.getName.endsWith(".dalf")) {
        if (entry.getSize != -1) {
          getDalfs(zipInputStream, fun, fun(zipInputStream).flatMap(res => acc.map(res :: _)))
        } else {
          Left(
            new IllegalArgumentException(s"Unknown size for '${entry.getName}' dalf entry in dar"))
        }
      } else getDalfs(zipInputStream, fun, acc)
    }
  }

  @tailrec
  private def findInZip(
      entries: java.util.Enumeration[_ <: ZipEntry],
      fileName: String): Either[Throwable, ZipEntry] =
    if (entries.hasMoreElements) {
      val entry = entries.nextElement()
      if (entry.getName == fileName)
        Right(entry)
      else findInZip(entries, fileName)
    } else Left(new RuntimeException(s"Cannot find $fileName in dar"))
}
