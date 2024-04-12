// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package lf
package language
package util

import com.daml.lf.data.Ref.{PackageId, TypeConName}
import com.daml.lf.testing.parser.ParserParameters
import data.{Ref, Relation}
import testing.parser
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import scala.language.implicitConversions

class PackageInfoSpecV2 extends PackageInfoSpec(LanguageMajorVersion.V2)

class PackageInfoSpec(majorLanguageVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with Matchers {

  import parser.Implicits.SyntaxHelper

  lazy val pkg0 = {
    implicit val parseParameters: parser.ParserParameters[this.type] =
      ParserParameters.defaultFor(majorLanguageVersion).copy(defaultPackageId = "-pkg0-")

    p"""metadata ( 'pkg' : '1.0.0' )
        module Mod0 {
          record @serializable MyUnit = {};
          record @serializable T0 = {};
          template (this : T0) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
            };
       }
    """
  }

  lazy val pkg1 = {
    implicit val parseParameters: parser.ParserParameters[this.type] =
      ParserParameters.defaultFor(majorLanguageVersion).copy(defaultPackageId = "-pkg1-")

    p"""metadata ( 'pkg' : '1.0.0' )
        module Mod11 {
          record @serializable T11 = {};
          template (this : T11) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              implements 'pkgA':ModA:IA { view = Mod0:MyUnit {}; };
              implements 'pkgB':ModB:IB { view = Mod0:MyUnit {}; };
            };
        }

        module Mod12 {
          record @serializable T12 = {};
          template (this : T12) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              implements 'pkgA':ModA:IA { view = Mod0:MyUnit {}; };
              implements 'pkgC':ModC:IC { view = Mod0:MyUnit {}; };
            };
        }
        """
  }

  lazy val pkg2 = {
    implicit val parseParameters: parser.ParserParameters[this.type] =
      ParserParameters.defaultFor(majorLanguageVersion).copy(defaultPackageId = "-pkg2-")

    p""" metadata ( 'pkg' : '1.0.0' )

         module Mod {
           record @serializable MyUnit = {};
         }

         module Mod21 {
           interface (this: I21) = {
             viewtype Mod:MyUnit;
           };
         }

         module Mod22 {
           interface (this: I22) = {
             viewtype Mod0:MyUnit;
           };
         }
     """
  }

  lazy val pkg3 = {
    implicit val parseParameters: parser.ParserParameters[this.type] =
      ParserParameters.defaultFor(majorLanguageVersion).copy(defaultPackageId = "-pkg3-")

    p"""metadata ( 'pkg' : '1.0.0' )

        module Mod31 {
          record @serializable T31 = {};
          template (this : T31) =  {
            precondition True;
            signatories Nil @Party;
            observers Nil @Party;
            implements '-pkg1-':Mod11:I11 {
              view = Mod0:MyUnit {};
            };
            implements 'pkgB':ModB:IB {
              view = Mod0:MyUnit {};
            };
          };
          interface (this: I31) = {
            viewtype Mod0:MyUnit;
          };
        }

        module Mod32 {
          record @serializable T32 = {};
          template (this : T32) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              implements '-pkg3-':Mod32:I32 { view = Mod0:MyUnit {}; };
              implements 'pkgA':ModA:IA { view = Mod0:MyUnit {}; };
            };
            interface (this: I32) = {
             viewtype Mod0:MyUnit;
           };
        }
        """
  }

  lazy val pkgs: Map[Ref.PackageId, Ast.Package] = Map(
    ("-pkg0-": Ref.PackageId) -> pkg0,
    ("-pkg1-": Ref.PackageId) -> pkg1,
    ("-pkg2-": Ref.PackageId) -> pkg2,
    ("-pkg3-": Ref.PackageId) -> pkg3,
  )

  "definedTemplates" should {
    "return the identifiers of the templates defined in the given packages" in {

      val testCases = List(
        ("-pkg0-": Ref.PackageId) -> Set[Ref.TypeConName]("-pkg0-:Mod0:T0"),
        ("-pkg1-": Ref.PackageId) -> Set[Ref.TypeConName]("-pkg1-:Mod11:T11", "-pkg1-:Mod12:T12"),
        ("-pkg2-": Ref.PackageId) -> Set.empty[Ref.TypeConName],
        ("-pkg3-": Ref.PackageId) -> Set[Ref.TypeConName]("-pkg3-:Mod31:T31", "-pkg3-:Mod32:T32"),
      )

      for (n <- 0 to testCases.size)
        testCases.combinations(n).filter(_.nonEmpty).foreach { cases =>
          val (pkgIds, ids) = cases.unzip
          val testPkgs = pkgIds.view.map(pkgId => pkgId -> pkgs(pkgId)).toMap
          val pkgInfo = new PackageInfo(pkgIds.head, pkg0.metadata, testPkgs)
          pkgInfo.definedTemplates shouldBe ids.fold(Set.empty)(_ | _)
        }
    }
  }

  "definedInterfaces" should {
    "return the identifiers of the interfaces defined in the given packages" in {

      val testCases = List(
        ("-pkg0-": Ref.PackageId) -> Set.empty[Ref.TypeConName],
        ("-pkg1-": Ref.PackageId) -> Set.empty[Ref.TypeConName],
        ("-pkg2-": Ref.PackageId) -> Set[Ref.TypeConName]("-pkg2-:Mod21:I21", "-pkg2-:Mod22:I22"),
        ("-pkg3-": Ref.PackageId) -> Set[Ref.TypeConName]("-pkg3-:Mod31:I31", "-pkg3-:Mod32:I32"),
      )

      for (n <- 0 to testCases.size)
        testCases.combinations(n).filter(_.nonEmpty).foreach { cases =>
          val (pkgIds, ids) = cases.unzip
          println(pkgIds)
          val testPkgs = pkgIds.view.map(pkgId => pkgId -> pkgs(pkgId)).toMap
          val pkgInfo = new PackageInfo(pkgIds.head, pkg0.metadata, testPkgs)
          pkgInfo.definedInterfaces shouldBe ids.fold(Set.empty)(_ | _)
        }
    }
  }

  "interfaceDirectInstances" should {
    "return the relation between interface and their direct instances" in {

      val testCases: List[(PackageId, Relation[TypeConName, TypeConName])] = List(
        ("-pkg0-": Ref.PackageId) ->
          Relation.empty[Ref.TypeConName, Ref.TypeConName],
        ("-pkg1-": Ref.PackageId) -> Map(
          ("pkgA:ModA:IA": Ref.TypeConName) ->
            Set[Ref.TypeConName]("-pkg1-:Mod11:T11", "-pkg1-:Mod12:T12"),
          ("pkgB:ModB:IB": Ref.TypeConName) ->
            Set[Ref.TypeConName]("-pkg1-:Mod11:T11"),
          ("pkgC:ModC:IC": Ref.TypeConName) ->
            Set[Ref.TypeConName]("-pkg1-:Mod12:T12"),
        ),
        ("-pkg2-": Ref.PackageId) ->
          Relation.empty[Ref.TypeConName, Ref.TypeConName],
        ("-pkg3-": Ref.PackageId) -> Map(
          ("-pkg1-:Mod11:I11": Ref.TypeConName) ->
            Set[Ref.TypeConName]("-pkg3-:Mod31:T31"),
          ("-pkg3-:Mod32:I32": Ref.TypeConName) ->
            Set[Ref.TypeConName]("-pkg3-:Mod32:T32"),
          ("pkgA:ModA:IA": Ref.TypeConName) ->
            Set[Ref.TypeConName]("-pkg3-:Mod32:T32"),
          ("pkgB:ModB:IB": Ref.TypeConName) ->
            Set[Ref.TypeConName]("-pkg3-:Mod31:T31"),
        ),
      )

      for (n <- 0 to testCases.size)
        testCases
          .combinations(n)
          .filter(_.nonEmpty)
          .foreach { cases =>
            val (pkgIds, rels) = cases.unzip
            val testPkgs = pkgIds.view.map(pkgId => pkgId -> pkgs(pkgId)).toMap
            val expectedResult = rels.fold(Relation.empty)(Relation.union)
            val pkgInfo = new PackageInfo(pkgIds.head, pkg0.metadata, testPkgs)
            pkgInfo.interfaceInstances shouldBe expectedResult
          }
    }
  }

  implicit def toPackageId(s: String): Ref.PackageId =
    Ref.PackageId.assertFromString(s)

  implicit def toIdentifier(s: String): Ref.Identifier =
    Ref.Identifier.assertFromString(s)

}
