// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen

import com.daml.lf.data.Ref.{QualifiedName, PackageId}

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import com.daml.lf.{iface => I}

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class UtilTest extends UtilTestHelpers with GeneratorDrivenPropertyChecks {

  val packageInterface =
    I.Interface(packageId = PackageId.assertFromString("abcdef"), typeDecls = Map.empty)
  val scalaPackageParts = Array("com", "digitalasset")
  val scalaPackage: String = scalaPackageParts.mkString(".")
  val util =
    lf.LFUtil(
      scalaPackage,
      I.EnvironmentInterface fromReaderInterfaces packageInterface,
      outputDir.toFile)

  def damlScalaName(damlNameSpace: Array[String], name: String): util.DamlScalaName =
    util.DamlScalaName(damlNameSpace, name)

  behavior of "Util"

  it should "mkDamlScalaName for a Contract named Test" in {
    val result = util.mkDamlScalaNameFromDirsAndName(Array(), "Test")
    result shouldEqual damlScalaName(Array.empty, "Test")
    result.packageName shouldEqual scalaPackage
    result.qualifiedName shouldEqual (scalaPackage + ".Test")
  }

  it should "mkDamlScalaName for a Template names foo.bar.Test" in {
    val result = util.mkDamlScalaName(Util.Template, QualifiedName assertFromString "foo.bar:Test")
    result shouldEqual damlScalaName(Array("foo", "bar"), "Test")
    result.packageName shouldEqual (scalaPackage + ".foo.bar")
    result.qualifiedName shouldEqual (scalaPackage + ".foo.bar.Test")
  }

  "partitionEithers" should "equal scalaz separate in simple cases" in forAll {
    iis: List[Either[Int, Int]] =>
      import scalaz.syntax.monadPlus._, scalaz.std.list._, scalaz.std.either._
      Util.partitionEithers(iis) shouldBe iis.separate
  }

}

abstract class UtilTestHelpers extends FlatSpec with Matchers with BeforeAndAfterAll {

  val outputDir = Files.createTempDirectory("codegenUtilTest")

  override protected def afterAll(): Unit = {
    super.afterAll()
    deleteRecursively(outputDir)
  }

  def deleteRecursively(dir: Path): Unit = {
    Files.walkFileTree(
      dir,
      new SimpleFileVisitor[Path] {
        override def postVisitDirectory(dir: Path, exc: IOException) = {
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }

        override def visitFile(file: Path, attrs: BasicFileAttributes) = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }
      }
    )
    ()
  }
}
