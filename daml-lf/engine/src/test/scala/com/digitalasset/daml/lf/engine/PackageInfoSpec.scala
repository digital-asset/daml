// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package lf
package language
package util

import com.daml.lf.data.Ref.{PackageId, TypeConName}
import data.{Ref, Relation}
import testing.parser
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import scala.language.implicitConversions

class PackageInfoSpec extends AnyWordSpec with Matchers {

  import parser.Implicits.{defaultParserParameters => _, _}

  lazy val pkg0 = {
    implicit val parseParameters: parser.ParserParameters[this.type] =
      parser.ParserParameters("-pkg0-", parser.defaultLanguageVersion)

    p"""
        module Mod0 {
          record @serializable T0 = {};
          template (this : T0) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              agreement "Agreement";
            };
       }
    """
  }

  lazy val pkg1 = {
    implicit val parseParameters: parser.ParserParameters[this.type] =
      parser.ParserParameters("-pkg1-", parser.defaultLanguageVersion)

    p"""
        module Mod11 {
          record @serializable T11 = {};
          template (this : T11) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              agreement "Agreement";
              implements 'pkgA':ModA:IA {};
              implements 'pkgB':ModB:IB {};
            };
        }

        module Mod12 {
          record @serializable T12 = {};
          template (this : T12) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              agreement "Agreement";
              implements 'pkgA':ModA:IA {};
              implements 'pkgC':ModC:IC {};
            };
        }
        """
  }

  lazy val pkg2 = {
    implicit val parseParameters: parser.ParserParameters[this.type] =
      parser.ParserParameters("-pkg2-", parser.defaultLanguageVersion)

    p"""
         module Mod21 {
           interface (this: I21) = {
             coimplements '-pkg1-':Mod11:T11 {};
             coimplements 'pkgA':ModA:TA {};
           };
         }

         module Mod22 {
           interface (this: I22) = {
             coimplements '-pkg1-':Mod11:T11 {};
             coimplements 'pkgB':ModB:TB {};
           };
         }
     """
  }

  lazy val pkg3 = {
    implicit val parseParameters: parser.ParserParameters[this.type] =
      parser.ParserParameters("-pkg3-", parser.defaultLanguageVersion)

    p"""
        module Mod31 {
          record @serializable T31 = {};
          template (this : T31) =  {
            precondition True;
            signatories Nil @Party;
            observers Nil @Party;
            agreement "Agreement";
            implements '-pkg1-':Mod11:I11 {};
            implements 'pkgB':ModB:IB {};
          };
          interface (this: I31) = {};
        }

        module Mod32 {
          record @serializable T32 = {};
          template (this : T32) =  {
              precondition True;
              signatories Nil @Party;
              observers Nil @Party;
              agreement "Agreement";
              implements '-pkg3-':Mod32:I32 {};
              implements 'pkgA':ModA:IA {};
            };
            interface (this: I32) = {
             coimplements '-pkg1-':Mod11:T11 {};
             coimplements 'pkgB':ModB:TB {};
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
        testCases.combinations(n).foreach { cases =>
          val (pkgIds, ids) = cases.unzip
          println(pkgIds)
          val testPkgs = pkgIds.view.map(pkgId => pkgId -> pkgs(pkgId)).toMap
          new PackageInfo(testPkgs).definedTemplates shouldBe ids.fold(Set.empty)(_ | _)
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
        testCases.combinations(n).foreach { cases =>
          val (pkgIds, ids) = cases.unzip
          println(pkgIds)
          val testPkgs = pkgIds.view.map(pkgId => pkgId -> pkgs(pkgId)).toMap
          new PackageInfo(testPkgs).definedInterfaces shouldBe ids.fold(Set.empty)(_ | _)
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
        testCases.combinations(n).foreach { cases =>
          val (pkgIds, rels) = cases.unzip
          val testPkgs = pkgIds.view.map(pkgId => pkgId -> pkgs(pkgId)).toMap
          val expectedResult = rels.fold(Relation.empty)(Relation.union)
          new PackageInfo(testPkgs).interfacesDirectImplementations shouldBe expectedResult
        }
    }
  }

  "interfaceRetroactiveInstances" should {
    "return the relation between interface and their retroaction instances" in {

      val testCases: List[(PackageId, Relation[TypeConName, TypeConName])] = List(
        ("-pkg0-": Ref.PackageId) ->
          Relation.empty[Ref.TypeConName, Ref.TypeConName],
        ("-pkg1-": Ref.PackageId) ->
          Relation.empty[Ref.TypeConName, Ref.TypeConName],
        ("-pkg2-": Ref.PackageId) -> Map(
          ("-pkg2-:Mod21:I21": Ref.TypeConName) ->
            Set[Ref.TypeConName]("-pkg1-:Mod11:T11", "pkgA:ModA:TA"),
          ("-pkg2-:Mod22:I22": Ref.TypeConName) ->
            Set[Ref.TypeConName]("-pkg1-:Mod11:T11", "pkgB:ModB:TB"),
        ),
        ("-pkg3-": Ref.PackageId) -> Map(
          ("-pkg3-:Mod32:I32": Ref.TypeConName) ->
            Set[Ref.TypeConName]("-pkg1-:Mod11:T11", "pkgB:ModB:TB")
        ),
      )

      for (n <- 0 to testCases.size)
        testCases.combinations(n).foreach { cases =>
          val (pkgIds, rels) = cases.unzip
          val testPkgs = pkgIds.view.map(pkgId => pkgId -> pkgs(pkgId)).toMap
          val expectedResult = rels.fold(Relation.empty)(Relation.union)
          new PackageInfo(testPkgs).interfacesRetroactiveInstances shouldBe expectedResult
        }
    }
  }

  implicit def toPackageId(s: String): Ref.PackageId =
    Ref.PackageId.assertFromString(s)

  implicit def toIdentifier(s: String): Ref.Identifier =
    Ref.Identifier.assertFromString(s)

}
