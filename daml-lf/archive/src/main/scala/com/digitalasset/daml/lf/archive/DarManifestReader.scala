// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import com.daml.lf.data.Bytes

import java.io.InputStream
import java.util.jar.{Attributes, Manifest}

object DarManifestReader {

  private val supportedFormat = "daml-lf"

  def dalfNames(bytes: Bytes): Result[Dar[String]] =
    dalfNames(bytes.toInputStream)

  def dalfNames(is: InputStream): Result[Dar[String]] = {
    val manifest = new Manifest(is)
    val attributes = value(manifest.getMainAttributes) _
    for {
      mainDalf <- attributes("Main-Dalf")
      allDalfs <- attributes("Dalfs")
      format <- attributes("Format")
      _ <- checkFormat(format)
    } yield Dar(mainDalf, dependencies(allDalfs, mainDalf))
  }

  private def dependencies(other: String, main: String): List[String] = {
    val deps = other.split(',').view.map(_.trim)
    deps.filter(x => x != main).toList
  }

  private def value(attributes: Attributes)(key: String): Result[String] =
    Option(attributes.getValue(key)) match {
      case Some(x) => Right(x.trim)
      case None => failure(s"Cannot find attribute: $key")
    }

  private def checkFormat(format: String): Result[Unit] =
    if (format == supportedFormat) Right(())
    else failure(s"Unsupported format: $format")

  private def failure(msg: String) = Left(Error.DarManifestReaderException(msg))

}
