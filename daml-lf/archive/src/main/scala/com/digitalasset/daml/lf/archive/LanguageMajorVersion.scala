// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package archive

import java.io.InputStream
import java.security.MessageDigest

import scala.util.Try
import scalaz.syntax.std.option._

// an ADT version of the DAML-LF version
sealed abstract class LanguageMajorVersion(
    val maxSupportedMinorVersion: LanguageMinorVersion,
    previousMinorVersions: List[LanguageMinorVersion])
    extends LfVersions(maxSupportedMinorVersion, previousMinorVersions)(identity)
    with Product
    with Serializable {

  // do *not* use implicitly unless type `LanguageMinorVersion` becomes
  // indexed by the enclosing major version's singleton type --SC
  def minorVersionOrdering: Ordering[LanguageMinorVersion] =
    Ordering.by(acceptedVersions.zipWithIndex.toMap)

  val supportedMinorVersions: List[LanguageMinorVersion] =
    acceptedVersions

  final def supportsMinorVersion(fromLFFile: LanguageMinorVersion): Boolean =
    isAcceptedVersion(fromLFFile).isDefined
}

object LanguageMajorVersion {

  lazy val All: List[LanguageMajorVersion] = List(V0, V1, VDev)

  // Note that DAML-LF v0 never had and never will have minor versions.
  case object V0
      extends LanguageMajorVersion(maxSupportedMinorVersion = "", previousMinorVersions = List())

  case object V1
      extends LanguageMajorVersion(
        maxSupportedMinorVersion = "3",
        previousMinorVersions = List("0", "1", "2"))

  // delay computing the SHA until we really need it
  lazy val VDev: LanguageMajorVersion = {
    import java.io.{BufferedInputStream, FileInputStream}
    val hash =
      Option(getClass getResourceAsStream "/daml-lf/archive/da/daml_lf_dev.proto")
        .orElse {
          // used only when running/testing with sbt
          import java.io.File.{separator => sep}
          Try(new FileInputStream(s"archive${sep}da${sep}daml_lf_dev.proto")).toOption
        }
        .cata(
          devProto =>
            try sha256(new BufferedInputStream(devProto))
            finally devProto.close(),
          sys.error("daml_lf_dev.proto missing from classpath"))
    case object VDev
        extends LanguageMajorVersion(
          maxSupportedMinorVersion = hash,
          previousMinorVersions = List())
    VDev
  }

  val supported: List[LanguageMajorVersion] =
    List(V0, V1, VDev)

  val ordering: Ordering[LanguageMajorVersion] =
    Ordering.by(supported.zipWithIndex.toMap)

  private[this] def sha256(is: InputStream): String = {
    val buflen = 1024
    val buf = Array.fill(buflen)(0: Byte)
    val digest = MessageDigest getInstance "SHA-256"
    @annotation.tailrec
    def lp(): Unit = {
      val read = is.read(buf, 0, buflen)
      if (read < 0) ()
      else {
        digest.update(buf, 0, read)
        lp()
      }
    }
    lp()
    digest.digest().map("%02x" format _).mkString
  }

}
