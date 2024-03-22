// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.Charset

import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DarManifestReaderTest extends AnyWordSpec with Matchers with Inside {

  private val unicode = Charset.forName("UTF-8")

  "should read dalf names from manifest, real scenario with Dalfs line split" in {
    val manifest = """Manifest-Version: 1.0
      |Created-By: Digital Asset packager (Daml-GHC)
      |Main-Dalf: com.daml.lf.archive:DarReaderTest:0.1.dalf
      |Dalfs: com.daml.lf.archive:DarReaderTest:0.1.dalf, daml-pri
      | m.dalf
      |Format: daml-lf
      |Encryption: non-encrypted""".stripMargin

    val inputStream: InputStream = new ByteArrayInputStream(manifest.getBytes(unicode))
    val actual = DarManifestReader.dalfNames(inputStream)

    actual shouldBe Right(
      Dar("com.daml.lf.archive:DarReaderTest:0.1.dalf", List("daml-prim.dalf"))
    )

    inputStream.close()
  }

  "should read dalf names from manifest, Main-Dalf returned in the head" in {
    val manifest = """Main-Dalf: A.dalf
                     |Dalfs: B.dalf, C.dalf, A.dalf, E.dalf
                     |Format: daml-lf
                     |Encryption: non-encrypted""".stripMargin

    val inputStream: InputStream = new ByteArrayInputStream(manifest.getBytes(unicode))
    val actual = DarManifestReader.dalfNames(inputStream)

    actual shouldBe Right(Dar("A.dalf", List("B.dalf", "C.dalf", "E.dalf")))

    inputStream.close()
  }

  "should read dalf names from manifest, can handle one Dalf per manifest" in {
    val manifest = """Main-Dalf: A.dalf
                     |Dalfs: A.dalf
                     |Format: daml-lf
                     |Encryption: non-encrypted""".stripMargin

    val inputStream: InputStream = new ByteArrayInputStream(manifest.getBytes(unicode))
    val actual = DarManifestReader.dalfNames(inputStream)

    actual shouldBe Right(Dar("A.dalf", List.empty))

    inputStream.close()
  }

  "should return failure if Format is not daml-lf" in {
    val manifest = """Main-Dalf: A.dalf
                     |Dalfs: B.dalf, C.dalf, A.dalf, E.dalf
                     |Format: anything-different-from-daml-lf
                     |Encryption: non-encrypted""".stripMargin

    val inputStream: InputStream = new ByteArrayInputStream(manifest.getBytes(unicode))
    val actual = DarManifestReader.dalfNames(inputStream)

    inside(actual) { case Left(Error.DarManifestReaderException(msg)) =>
      msg shouldBe "Unsupported format: anything-different-from-daml-lf"
    }

    inputStream.close()
  }
}
