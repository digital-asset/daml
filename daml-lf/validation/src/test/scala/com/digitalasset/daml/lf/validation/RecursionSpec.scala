// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.testing.parser.defaultPackageId
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RecursionSpec extends AnyWordSpec with TableDrivenPropertyChecks with Matchers {

  "Recursion validation should not detect cycles between a module and itself" in {

    val p =
      p"""
         module Math {
           val fact : (Int64 -> Int64) = \ (x: Int64) ->
             case (EQUAL_INT64 x 0) of
                 True -> 1
               | _    -> MULT_INT64 x (fact (SUB_INT64 x 1));
         }
       """

    Recursion.checkPackage(defaultPackageId, p)

  }

  "Recursion validation should detect cycles between modules" in {

    def module(modName: String, modRefs: String*) =
      s"""
        |module $modName {
        |           record R = {};
        |           ${modRefs.map(b => s"val v$b : $b:R = $b:R {}; \n").mkString("\n")}
        |         }
      """.stripMargin

    val negativeCase =
      // package without cyclic module dependencies
      p"""
         ${module("A", "B", "E")}
         ${module("B", "B", "E")}
         ${module("E", "E")}
       """

    val positiveCase1 =
      // package with cyclic module dependency ("A" -> "B" -> "A")
      p"""
         ${module("A", "B")}
         ${module("B", "A")}
       """

    val positiveCase2 =
      // package with cyclic module dependency ("B" -> "C" -> "D" -> "B")
      p"""
        ${module("A", "B", "C", "E")}
        ${module("B", "C", "E")}
        ${module("C", "D")}
        ${module("D", "B", "E")}
        ${module("E", "E")}
       """

    Recursion.checkPackage(defaultPackageId, negativeCase)
    an[EImportCycle] should be thrownBy Recursion.checkPackage(defaultPackageId, positiveCase1)
    an[EImportCycle] should be thrownBy Recursion.checkPackage(defaultPackageId, positiveCase2)

  }

  "Recursion validation should detect type synonym cycles" in {

    val negativeCase =
      // module without a type-syn cycle
      p"""
         module Mod {
           synonym SynInt = Int64 ;
           synonym SynSynInt = |Mod:SynInt| ;
         }
       """

    val positiveCase1 =
      // module with a direct type-syn cycle
      p"""
         module Mod {
           synonym SynCycle = |Mod:SynCycle| ;
         }
       """

    val positiveCase2 =
      // module with a mutual type-syn cycle
      p"""
         module Mod {
           synonym SynInt = Int64 ;
           synonym SynBad1 = |Mod:SynBad2| ;
           synonym SynBad2 = List |Mod:SynBad1| ;
         }
       """

    Recursion.checkPackage(defaultPackageId, negativeCase)
    an[ETypeSynCycle] should be thrownBy Recursion.checkPackage(defaultPackageId, positiveCase1)
    an[ETypeSynCycle] should be thrownBy Recursion.checkPackage(defaultPackageId, positiveCase2)

  }

}
