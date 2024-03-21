// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.data.{FrontStack, ImmArray, Ref}
import com.daml.lf.language.Ast
import com.daml.lf.value.Value.{ValueInt64, ValueList, ValueParty, ValueRecord}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PreprocessorSpec extends AnyWordSpec with Inside with Matchers {

  import com.daml.lf.testing.parser.Implicits._
  import com.daml.lf.transaction.test.TransactionBuilder.Implicits.{defaultPackageId => _, _}

  private[this] implicit val defaultPackageId: Ref.PackageId =
    defaultParserParameters.defaultPackageId

  private[this] lazy val pkg =
    p"""
        module Mod {

          record @serializable Record = { owners: List Party, data : Int64 };

          template (this : Record) = {
            precondition True;
            signatories Mod:Record {owners} this;
            observers Mod:Record {owners} this;
            agreement "Agreement";
            key @(List Party) (Mod:Record {owners} this) (\ (parties: List Party) -> parties);
          };

        }
    """

  private[this] val pkgs = Map(defaultPackageId -> pkg)
  private[this] val parties = ValueList(FrontStack(ValueParty("Alice")))

  "preprocessor" should {
    "returns correct result when resuming" in {
      val preporcessor = new preprocessing.Preprocessor(ConcurrentCompiledPackages())
      val intermediaryResult = preporcessor
        .translateValue(
          Ast.TTyCon("Mod:Record"),
          ValueRecord("", ImmArray("owners" -> parties, "data" -> ValueInt64(42))),
        )
      intermediaryResult shouldBe a[ResultNeedPackage[_]]
      val finalResult = intermediaryResult.consume(_ => None, pkgs.get, _ => None)
      finalResult shouldBe a[Right[_, _]]
    }

    "returns correct error when resuming" in {
      val preporcessor = new preprocessing.Preprocessor(ConcurrentCompiledPackages())
      val intermediaryResult = preporcessor
        .translateValue(
          Ast.TTyCon("Mod:Record"),
          ValueRecord(
            "",
            ImmArray("owners" -> parties, "wrong_field" -> ValueInt64(42)),
          ),
        )
      intermediaryResult shouldBe a[ResultNeedPackage[_]]
      val finalResult = intermediaryResult.consume(_ => None, pkgs.get, _ => None)
      inside(finalResult) { case Left(Error.Preprocessing(error)) =>
        error shouldBe a[Error.Preprocessing.TypeMismatch]
      }
    }
  }

}
