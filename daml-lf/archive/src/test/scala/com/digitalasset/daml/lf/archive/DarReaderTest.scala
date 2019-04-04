// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import java.io.File
import java.util.zip.ZipFile

import com.digitalasset.daml_lf.DamlLf
import com.digitalasset.daml_lf.DamlLf1
import org.scalatest.{Inside, Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.util.{Success, Try}
import com.digitalasset.daml.lf.data.Ref.SimpleString

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class DarReaderTest extends WordSpec with Matchers with Inside {

  private val darFile = resource("daml-lf/archive/DarReaderTest.dar")

  private def resource(path: String): File = {
    val f = new File(path).getAbsoluteFile
    require(f.exists, s"File does not exist: $f")
    f
  }

  s"should read dar file: $darFile" in {
    val archives: Try[List[((SimpleString, DamlLf.ArchivePayload), LanguageMajorVersion)]] =
      DarReaderWithVersion.readArchive(new ZipFile(darFile))

    inside(archives) {
      case Success(
          ((packageId1, archive1), LanguageMajorVersion.V1) ::
            ((packageId2, archive2), LanguageMajorVersion.V1) :: Nil) =>
        packageId1.underlyingString shouldNot be('empty)
        packageId2.underlyingString shouldNot be('empty)
        archive1.getDamlLf1.getModulesCount should be > 0
        archive2.getDamlLf1.getModulesCount should be > 0
        val archive1Modules = archive1.getDamlLf1.getModulesList.asScala
        val archive2Modules = archive2.getDamlLf1.getModulesList.asScala
        inside(
          archive1Modules
            .find(m => name(m.getName) == "DarReaderTest")
            .orElse(archive2Modules.find(m => name(m.getName) == "DarReaderTest"))) {
          case Some(module) =>
            val actualTypes: Set[String] =
              module.getDataTypesList.asScala.toSet
                .map((t: DamlLf1.DefDataType) => name(t.getName))
            actualTypes shouldBe Set("Transfer", "Call2", "CallablePayout", "PayOut")
        }
    }
  }

  private def name(n: DamlLf1.DottedName): String =
    n.getSegmentsList.iterator.asScala.mkString(".")
}
