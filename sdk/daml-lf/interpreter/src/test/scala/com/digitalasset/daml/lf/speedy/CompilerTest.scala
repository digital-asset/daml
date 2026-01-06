// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.SError.SError
import com.digitalasset.daml.lf.speedy.SExpr.SExpr
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.FatContractInstance
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml.lf.value.Value.ContractId.V1.`V1 Order`
import com.digitalasset.daml.lf.value.Value.ContractId.`Cid Order`
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.ArraySeq

class CompilerTest extends AnyWordSpec with TableDrivenPropertyChecks with Matchers with Inside {

  val helpers = new CompilerTestHelpers
  import helpers._

  "unsafeCompile" should {
    "handle 10k commands" in {
      val cmds = ImmArray.ImmArraySeq
        .fill(10 * 1000)(Command.Create(recordCon, contract()))
        .toImmArray

      compiledPackages.compiler.unsafeCompile(cmds) shouldBe a[SExpr]
    }

    "compile deeply nested lets" in {
      val expr = List
        .range[Long](1, 3000)
        .foldRight[Expr](EBuiltinLit(BLInt64(5000)))((i, acc) =>
          ELet(
            Binding(
              Some(Ref.Name.assertFromString(s"v$i")),
              TBuiltin(BTInt64),
              EBuiltinLit(BLInt64(i)),
            ),
            acc,
          )
        )

      compiledPackages.compiler.unsafeCompile(expr) shouldBe a[SExpr]
    }

    "handle propose ETyAbs, ETyApp, and ELoc" in {
      import language.Util.{EEmptyString, EFalse, TUnit}
      val List(a, x, precond, label) =
        List("a", "x", "precond", "label").map(Ref.Name.assertFromString)
      val l = Ref.Location(
        packageId = pkgId,
        module = Ref.ModuleName.assertFromString("Module"),
        definition = "test",
        start = (1, 1),
        end = (1, 10),
      )
      val tyCon = Ref.Identifier.assertFromString("-pkgId-:Module:Record")
      val tyConApp = TypeConApp(tyCon, ImmArray.empty)

      val context = List[Expr => Expr](
        ELocation(l, _),
        ETyAbs(a -> KStar, _),
        ETyApp(_, typ = TUnit),
      )

      val tests =
        Table(
          "inputs" -> "output",
          context.map(f => EAbs(x -> TUnit, f(EAbs(x -> TUnit, EVar(x))))) ->
            SExpr.SEMakeClo(ArraySeq.empty, 2, SExpr.SELocA(1)),
          context.map(f =>
            EAbs(
              x -> TTyCon(tyCon),
              ERecUpd(
                tyConApp,
                precond,
                f(
                  ERecUpd(
                    tyConApp,
                    label,
                    EEmptyString,
                    EVar(x),
                  )
                ),
                EFalse,
              ),
            )
          ) -> SExpr.SEMakeClo(
            ArraySeq.empty,
            1,
            SExpr.SEAppAtomicSaturatedBuiltin(
              SBuiltinFun.SBRecUpdMulti(tyCon, List(1, 0)),
              ArraySeq(
                SExpr.SEValue(SValue.SText("")),
                SExpr.SELocA(0),
                SExpr.SEValue(SValue.SBool(false)),
              ),
            ),
          ),
        )

      forEvery(tests) { case (inputs, output) =>
        inputs.foreach(input => compiledPackages.compiler.unsafeCompile(input) shouldBe output)
      }
    }
  }
}

final class CompilerTestHelpers {

  import SpeedyTestLib.loggingContext

  implicit val parserParameters: ParserParameters[this.type] = ParserParameters.default
  val pkgId = parserParameters.defaultPackageId

  implicit val contractIdOrder: Ordering[ContractId] = `Cid Order`.toScalaOrdering
  implicit val contractIdV1Order: Ordering[ContractId.V1] = `V1 Order`.toScalaOrdering

  val recordCon: Ref.Identifier =
    Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Module:Record"))
  val pkg =
    p"""  metadata ( '-compiler-test-package-' : '1.0.0' )
        module Module {

          record @serializable Record = { precond: Bool, label: Text, party: Party };
          template (this : Record) =  {
            precondition Module:Record {precond} this;
            signatories Cons @Party [Module:Record {party} this] (Nil @Party);
            observers Nil @Party;
          };

          record @serializable RecordKey = { precond: Bool, label: Text, party: Party };
          template (this : RecordKey) =  {
            precondition Module:RecordKey {precond} this;
            signatories Cons @Party [Module:RecordKey {party} this] (Nil @Party);
            observers Nil @Party;
            key @Module:RecordKey
              this
              (\(key: Module:RecordKey) -> (Cons @Party [Module:RecordKey {party} key] (Nil @Party)));
          };
        }
    """
  val compiledPackages: PureCompiledPackages =
    PureCompiledPackages.assertBuild(
      Map(pkgId -> pkg),
      Compiler.Config.Default,
    )
  val alice: Party = Ref.Party.assertFromString("Alice")

  def contract(label: String = "", precondition: Boolean = true): SValue.SRecord = SValue.SRecord(
    recordCon,
    ImmArray(
      Ref.Name.assertFromString("precond"),
      Ref.Name.assertFromString("label"),
      Ref.Name.assertFromString("party"),
    ),
    ArraySeq(SValue.SBool(precondition), SValue.SText(label), SValue.SParty(alice)),
  )

  def tokenApp(sexpr: SExpr): SExpr =
    SExpr.SEApp(sexpr, ArraySeq(SValue.SToken))

  def evalSExpr(
      sexpr: SExpr,
      getContract: PartialFunction[Value.ContractId, FatContractInstance] = PartialFunction.empty,
      committers: Set[Party] = Set.empty,
  ): Either[
    SError,
    (SValue, Map[ContractId, (Ref.Identifier, SValue)]),
  ] = {
    val machine =
      Speedy.UpdateMachine(
        compiledPackages = compiledPackages,
        preparationTime = Time.Timestamp.MinValue,
        initialSeeding = InitialSeeding.TransactionSeed(crypto.Hash.hashPrivateKey("CompilerTest")),
        expr = sexpr,
        committers = committers,
        readAs = Set.empty,
      )

    SpeedyTestLib
      .run(machine, getContract = getContract)
      .map((_, machine.localContractStore))
  }
}
