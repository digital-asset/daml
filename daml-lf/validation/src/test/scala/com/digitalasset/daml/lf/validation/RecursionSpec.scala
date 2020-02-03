// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import com.digitalasset.daml.lf.testing.parser.Implicits._
import com.digitalasset.daml.lf.testing.parser.defaultPackageId
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

class RecursionSpec extends WordSpec with TableDrivenPropertyChecks with Matchers {

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

    Recursion.checkPackage(defaultPackageId, p.modules)

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

    Recursion.checkPackage(defaultPackageId, negativeCase.modules)
    an[EImportCycle] should be thrownBy
      Recursion.checkPackage(defaultPackageId, positiveCase1.modules)
    an[EImportCycle] should be thrownBy
      Recursion.checkPackage(defaultPackageId, positiveCase2.modules)

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

    Recursion.checkPackage(defaultPackageId, negativeCase.modules)
    an[ETypeSynCycle] should be thrownBy
      Recursion.checkPackage(defaultPackageId, positiveCase1.modules)
    an[ETypeSynCycle] should be thrownBy
      Recursion.checkPackage(defaultPackageId, positiveCase2.modules)

  }

}
