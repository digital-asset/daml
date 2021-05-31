// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import java.io.InputStream
import java.util.jar.{Attributes, Manifest}

import scala.util.{Failure, Success, Try}

object DarManifestReader {

  private val supportedFormat = "daml-lf"

  def dalfNames(is: InputStream): Try[Dar[String]] = {
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

  private def value(attributes: Attributes)(key: String): Try[String] =
    Option(attributes.getValue(key)) match {
      case None => failure(s"Cannot find attribute: $key")
      case Some(x) => Success(x.trim)
    }

  private def checkFormat(format: String): Try[Unit] =
    if (format == supportedFormat) Success(())
    else failure(s"Unsupported format: $format")

  private def failure(msg: String) = Failure(DarManifestReaderException(msg))

  case class DarManifestReaderException(msg: String) extends IllegalStateException(msg)
}
