// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen

import java.io.File
import java.nio.file.Files

import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.archive.DarReader
import com.daml.lf.codegen.backend.java.JavaBackend
import com.daml.lf.codegen.conf.Conf
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class CodeGenRunnerTests extends AnyFlatSpec with Matchers with BazelRunfiles {

  behavior of "collectDamlLfInterfaces"

  def path(p: String) = new File(p).getAbsoluteFile.toPath

  val testDar = path(rlocation("language-support/java/codegen/test-daml.dar"))
  val dar = DarReader().readArchiveFromFile(testDar.toFile).get

  val dummyOutputDir = Files.createTempDirectory("codegen")

  it should "always use JavaBackend, which is currently hardcoded" in {
    CodeGenRunner.backend should be theSameInstanceAs JavaBackend
  }

  it should "read interfaces from a single DAR file without a prefix" in {

    val conf = Conf(
      Map(testDar -> None),
      dummyOutputDir,
    )

    val (interfaces, pkgPrefixes) = CodeGenRunner.collectDamlLfInterfaces(conf)

    interfaces.flatMap(
      _.typeDecls.keys.map(_.qualifiedName)
    ) should contain theSameElementsAs List(
      "DA.Date.Types:DayOfWeek",
      "DA.Date.Types:Month",
      "DA.Generics:Associativity",
      "DA.Generics:DecidedStrictness",
      "DA.Generics:Fixity",
      "DA.Generics:Infix0",
      "DA.Generics:K1",
      "DA.Generics:Par1",
      "DA.Generics:SourceStrictness",
      "DA.Generics:SourceUnpackedness",
      "DA.Generics:U1",
      "DA.Internal.Down:Down",
      "DA.Internal.Template:Archive",
      "DA.Logic.Types:Formula",
      "DA.Monoid.Types:All",
      "DA.Monoid.Types:Any",
      "DA.Monoid.Types:Product",
      "DA.Monoid.Types:Sum",
      "DA.Next.Map:Map",
      "DA.Next.Set:Set",
      "DA.NonEmpty.Types:NonEmpty",
      "DA.Random:Minstd",
      "DA.Semigroup.Types:Max",
      "DA.Semigroup.Types:Min",
      "DA.Stack:SrcLoc",
      "DA.Time.Types:RelTime",
      "DA.Types:Either",
      "DA.Types:Tuple10",
      "DA.Types:Tuple11",
      "DA.Types:Tuple12",
      "DA.Types:Tuple13",
      "DA.Types:Tuple14",
      "DA.Types:Tuple15",
      "DA.Types:Tuple16",
      "DA.Types:Tuple17",
      "DA.Types:Tuple18",
      "DA.Types:Tuple19",
      "DA.Types:Tuple2",
      "DA.Types:Tuple20",
      "DA.Types:Tuple3",
      "DA.Types:Tuple4",
      "DA.Types:Tuple5",
      "DA.Types:Tuple6",
      "DA.Types:Tuple7",
      "DA.Types:Tuple8",
      "DA.Types:Tuple9",
      "DA.Validation.Types:Validation",
      "Foo:Foo",
      "GHC.Stack.Types:CallStack",
      "GHC.Stack.Types:SrcLoc",
      "GHC.Tuple:Unit",
      "GHC.Types:Ordering",
    )

    assert(pkgPrefixes == Map.empty)
  }

  it should "read interfaces from a single DAR file with a prefix" in {

    val conf = Conf(
      Map(testDar -> Some("PREFIX")),
      dummyOutputDir,
    )

    val (interfaces, pkgPrefixes) = CodeGenRunner.collectDamlLfInterfaces(conf)

    assert(interfaces.map(_.packageId).length == dar.all.length)
    assert(pkgPrefixes.size == dar.all.length)
    assert(pkgPrefixes.values.forall(_ == "PREFIX"))
  }
}
