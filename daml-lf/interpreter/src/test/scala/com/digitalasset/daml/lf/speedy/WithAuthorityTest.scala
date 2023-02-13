// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.FrontStack
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.Party
import com.daml.lf.interpretation.Error.FailedAuthorization
import com.daml.lf.ledger.FailedAuthorization.CreateMissingAuthorization
import com.daml.lf.speedy.SError.SError
import com.daml.lf.speedy.SExpr.SEApp
import com.daml.lf.speedy.SValue.{SList, SParty}
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.transaction.Node
import com.daml.lf.transaction.NodeId
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.lf.value.Value.{ValueRecord, ValueParty}

import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers._

class WithAuthorityTest extends AnyFreeSpec with Inside {

  val a = Party.assertFromString("Alice")
  val b = Party.assertFromString("Bob")
  val c = Party.assertFromString("Charlie")

  import WithAuthorityTest._

  "Single" - {

    "single (auth changed): A->{B}->A [FAIL]" in {
      inside(makeSingle(committers = Set(a), required = Set(b), signed = a)) { case Left(err) =>
        inside(err) { case SError.SErrorDamlException(FailedAuthorization(_, why)) =>
          inside(why) { case cma: CreateMissingAuthorization =>
            cma.authorizingParties shouldBe Set(b)
            cma.requiredParties shouldBe Set(a)
          }
        }
      }
    }
    "single (auth changed): A->{B}->B [OK]" in {
      inside(makeSingle(committers = Set(a), required = Set(b), signed = b)) { case Right(tx) =>
        val shape = shapeOfTransaction(tx)
        val expected = List(Authority(Set(b), List(Create(b))))
        shape shouldBe expected
      }
    }
    "single (auth restricted/extended) {A,B}->{B,C}->A [FAIL]" in {
      inside(makeSingle(committers = Set(a, b), required = Set(b, c), signed = a)) {
        case Left(err) =>
          inside(err) { case SError.SErrorDamlException(FailedAuthorization(_, why)) =>
            inside(why) { case cma: CreateMissingAuthorization =>
              cma.authorizingParties shouldBe Set(b, c)
              cma.requiredParties shouldBe Set(a)
            }
          }
      }
    }
    "single (auth restricted/extended) {A,B}->{B,C}->B [OK]" in {
      inside(makeSingle(committers = Set(a, b), required = Set(b, c), signed = b)) {
        case Right(tx) =>
          val shape = shapeOfTransaction(tx)
          val expected = List(Authority(Set(b, c), List(Create(b))))
          shape shouldBe expected
      }
    }
    "single (auth restricted/extended) {A,B}->{B,C}->C [OK]" in {
      inside(makeSingle(committers = Set(a, b), required = Set(b, c), signed = c)) {
        case Right(tx) =>
          val shape = shapeOfTransaction(tx)
          val expected = List(Authority(Set(b, c), List(Create(c))))
          shape shouldBe expected
      }
    }
    "single (auth unchanged) A->{A}->A [OK; no auth-node]" in {
      inside(makeSingle(committers = Set(a), required = Set(a), signed = a)) { case Right(tx) =>
        val shape = shapeOfTransaction(tx)
        val expected = List(Create(a))
        shape shouldBe expected
      }
    }
    "single (auth restricted) {A,B}->{B}->B [OK; no auth-node]" in {
      inside(makeSingle(committers = Set(a, b), required = Set(b), signed = b)) { case Right(tx) =>
        val shape = shapeOfTransaction(tx)
        val expected = List(Create(b))
        shape shouldBe expected
      }
    }
    "single (auth restricted) {A,B}->{B}->A [FAIL]" in {
      inside(makeSingle(committers = Set(a, b), required = Set(b), signed = a)) { case Left(err) =>
        inside(err) { case SError.SErrorDamlException(FailedAuthorization(_, why)) =>
          inside(why) { case cma: CreateMissingAuthorization =>
            cma.authorizingParties shouldBe Set(b)
            cma.requiredParties shouldBe Set(a)
          }
        }
      }
    }
  }

  "Sequence1" - {

    "sequence1: A-> ( {B}->B ; ->A ) [OK]" in {
      inside(
        makeSequence1(
          committers = Set(a),
          required = Set(b),
          signed1 = b,
          signed2 = a,
        )
      ) { case Right(tx) =>
        val shape = shapeOfTransaction(tx)
        val expected = List[Shape](Authority(Set(b), List(Create(b))), Create(a))
        shape shouldBe expected
      }
    }
    "sequence1: A-> ( {B}->B ; ->B ) [FAIL]" in {
      inside(
        makeSequence1(
          committers = Set(a),
          required = Set(b),
          signed1 = b,
          signed2 = b,
        )
      ) { case Left(err) =>
        inside(err) { case SError.SErrorDamlException(FailedAuthorization(_, why)) =>
          inside(why) { case cma: CreateMissingAuthorization =>
            cma.authorizingParties shouldBe Set(a)
            cma.requiredParties shouldBe Set(b)
          }
        }
      }
    }
    "sequence1: A-> ( {A}->A ; ->A ) [OK; no auth-node]" in {
      inside(
        makeSequence1(
          committers = Set(a),
          required = Set(a),
          signed1 = a,
          signed2 = a,
        )
      ) { case Right(tx) =>
        val shape = shapeOfTransaction(tx)
        val expected = List[Shape](Create(a), Create(a))
        shape shouldBe expected
      }
    }
    "sequence1: {A,B}-> ( {B}->B ; ->A ) [OK; no auth-node]" in {
      inside(
        makeSequence1(
          committers = Set(a, b),
          required = Set(b),
          signed1 = b,
          signed2 = a,
        )
      ) { case Right(tx) =>
        val shape = shapeOfTransaction(tx)
        val expected = List[Shape](Create(b), Create(a))
        shape shouldBe expected
      }
    }
    "sequence1: {A,B}-> ( {B}->B ; ->B ) [OK; no auth-node]" in {
      inside(
        makeSequence1(
          committers = Set(a, b),
          required = Set(b),
          signed1 = b,
          signed2 = b,
        )
      ) { case Right(tx) =>
        val shape = shapeOfTransaction(tx)
        val expected = List[Shape](Create(b), Create(b))
        shape shouldBe expected
      }
    }

  }

  "Sequence2" - {

    "sequence2: A-> ( {B}->B ; {C}->C ) [OK; 2 auth nodes]" in {
      inside(
        makeSequence2(
          committers = Set(a),
          required1 = Set(b),
          signed1 = b,
          required2 = Set(c),
          signed2 = c,
        )
      ) { case Right(tx) =>
        val shape = shapeOfTransaction(tx)
        val expected = List(Authority(Set(b), List(Create(b))), Authority(Set(c), List(Create(c))))
        shape shouldBe expected
      }
    }
    "sequence2: A-> ( {B}->B ; {B}->B ) [OK; 2 auth nodes]" in {
      inside(
        makeSequence2(
          committers = Set(a),
          required1 = Set(b),
          signed1 = b,
          required2 = Set(b),
          signed2 = b,
        )
      ) { case Right(tx) =>
        val shape = shapeOfTransaction(tx)
        val expected = List(Authority(Set(b), List(Create(b))), Authority(Set(b), List(Create(b))))
        shape shouldBe expected
      }
    }
    "sequence2: A-> ( {B}->B ; {A}->A ) [OK; only 1 auth node]" in {
      inside(
        makeSequence2(
          committers = Set(a),
          required1 = Set(b),
          signed1 = b,
          required2 = Set(a),
          signed2 = a,
        )
      ) { case Right(tx) =>
        val shape = shapeOfTransaction(tx)
        val expected = List[Shape](Authority(Set(b), List(Create(b))), Create(a))
        shape shouldBe expected
      }
    }
    "sequence2: A-> ( {B}->B ; {C}->A ) [FAIL]" in {
      inside(
        makeSequence2(
          committers = Set(a),
          required1 = Set(b),
          signed1 = b,
          required2 = Set(c),
          signed2 = a,
        )
      ) { case Left(err) =>
        inside(err) { case SError.SErrorDamlException(FailedAuthorization(_, why)) =>
          inside(why) { case cma: CreateMissingAuthorization =>
            cma.authorizingParties shouldBe Set(c)
            cma.requiredParties shouldBe Set(a)
          }
        }
      }
    }
    "sequence2: A-> ( {B}->B ; {C}->B ) [FAIL]" in {
      inside(
        makeSequence2(
          committers = Set(a),
          required1 = Set(b),
          signed1 = b,
          required2 = Set(c),
          signed2 = b,
        )
      ) { case Left(err) =>
        inside(err) { case SError.SErrorDamlException(FailedAuthorization(_, why)) =>
          inside(why) { case cma: CreateMissingAuthorization =>
            cma.authorizingParties shouldBe Set(c)
            cma.requiredParties shouldBe Set(b)
          }
        }
      }
    }
  }

  "Nested" - {

    "nested: A->{B}->{C}->C [OK]" in {
      inside(makeNested(committers = Set(a), outer = Set(b), inner = Set(c), signed = c)) {
        case Right(tx) =>
          val shape = shapeOfTransaction(tx)
          val expected = List(Authority(Set(b), List(Authority(Set(c), List(Create(c))))))
          shape shouldBe expected
      }
    }
    "nested: A->{B}->{C}->A [FAIL]" in {
      inside(makeNested(committers = Set(a), outer = Set(b), inner = Set(c), signed = a)) {
        case Left(err) =>
          inside(err) { case SError.SErrorDamlException(FailedAuthorization(_, why)) =>
            inside(why) { case cma: CreateMissingAuthorization =>
              cma.authorizingParties shouldBe Set(c)
              cma.requiredParties shouldBe Set(a)
            }
          }
      }
    }
    "nested: A->{B}->{C}->B [FAIL]" in {
      inside(makeNested(committers = Set(a), outer = Set(b), inner = Set(c), signed = b)) {
        case Left(err) =>
          inside(err) { case SError.SErrorDamlException(FailedAuthorization(_, why)) =>
            inside(why) { case cma: CreateMissingAuthorization =>
              cma.authorizingParties shouldBe Set(c)
              cma.requiredParties shouldBe Set(b)
            }
          }
      }
    }
    "nested: A->{A}->{C}->C [OK; 1 auth-node]" in {
      inside(makeNested(committers = Set(a), outer = Set(a), inner = Set(c), signed = c)) {
        case Right(tx) =>
          val shape = shapeOfTransaction(tx)
          val expected = List(Authority(Set(c), List(Create(c))))
          shape shouldBe expected
      }
    }
    "nested: A->{A}->{C}->A [FAIL]" in {
      inside(makeNested(committers = Set(a), outer = Set(a), inner = Set(c), signed = a)) {
        case Left(err) =>
          inside(err) { case SError.SErrorDamlException(FailedAuthorization(_, why)) =>
            inside(why) { case cma: CreateMissingAuthorization =>
              cma.authorizingParties shouldBe Set(c)
              cma.requiredParties shouldBe Set(a)
            }
          }
      }
    }
    "nested: A->{C}->{C}->C [OK; 1 auth-node]" in {
      inside(makeNested(committers = Set(a), outer = Set(c), inner = Set(c), signed = c)) {
        case Right(tx) =>
          val shape = shapeOfTransaction(tx)
          val expected = List(Authority(Set(c), List(Create(c))))
          shape shouldBe expected
      }
    }
    "nested: A->{C}->{C}->A [FAIL]" in {
      inside(makeNested(committers = Set(a), outer = Set(c), inner = Set(c), signed = a)) {
        case Left(err) =>
          inside(err) { case SError.SErrorDamlException(FailedAuthorization(_, why)) =>
            inside(why) { case cma: CreateMissingAuthorization =>
              cma.authorizingParties shouldBe Set(c)
              cma.requiredParties shouldBe Set(a)
            }
          }
      }
    }
    "nested: A->{A,B}->{B,C}->C [OK]" in {
      inside(makeNested(committers = Set(a), outer = Set(a, b), inner = Set(b, c), signed = c)) {
        case Right(tx) =>
          val shape = shapeOfTransaction(tx)
          val expected = List(Authority(Set(a, b), List(Authority(Set(b, c), List(Create(c))))))
          shape shouldBe expected
      }
    }
    "nested: A->{A,B}->{B,C}->A [FAIL]" in {
      inside(makeNested(committers = Set(a), outer = Set(a, b), inner = Set(b, c), signed = a)) {
        case Left(err) =>
          inside(err) { case SError.SErrorDamlException(FailedAuthorization(_, why)) =>
            inside(why) { case cma: CreateMissingAuthorization =>
              cma.authorizingParties shouldBe Set(b, c)
              cma.requiredParties shouldBe Set(a)
            }
          }
      }
    }
  }

  // TODO #15882 -- test interaction between Authority and Exercise/Rollback nodes

}

object WithAuthorityTest {

  import SpeedyTestLib.loggingContext

  val transactionSeed = crypto.Hash.hashPrivateKey("WithAuthorityTest.scala")

  val pkgs: PureCompiledPackages = SpeedyTestLib.typeAndCompile(p"""
      module M {

        record @serializable T1 = { signed: Party, info: Int64 } ;
        template (record : T1) = {
          precondition True;
          signatories Cons @Party [M:T1 {signed} record] (Nil @Party);
          observers Nil @Party;
          agreement "Agreement";
        };

        val single : List Party -> Party -> Update Unit =
          \(requested: List Party) -> \(signed: Party) ->
           WITH_AUTHORITY @Unit requested
           (ubind x1: ContractId M:T1 <- create @M:T1 M:T1 { signed = signed, info = 100 }
            in upure @Unit ());

        val nested : List Party -> List Party -> Party -> Update Unit =
          \(outer: List Party) -> \(inner: List Party) -> \(signed: Party) ->
           WITH_AUTHORITY @Unit outer
           (WITH_AUTHORITY @Unit inner
            (ubind x1: ContractId M:T1 <- create @M:T1 M:T1 { signed = signed, info = 100 }
             in upure @Unit ()));

        val sequence1 : List Party -> Party -> Party -> Update Unit =
          \(required1: List Party) -> \(signed1: Party) -> \(signed2: Party) ->
            ubind
              u: Unit <-
                WITH_AUTHORITY @Unit required1
                 (ubind x1: ContractId M:T1 <- create @M:T1 M:T1 { signed = signed1, info = 100 }
                   in upure @Unit ());
              x2: ContractId M:T1 <- create @M:T1 M:T1 { signed = signed2, info = 100 }
            in upure @Unit ();

        val sequence2 : List Party -> Party -> List Party -> Party -> Update Unit =
          \(required1: List Party) -> \(signed1: Party) -> \(required2: List Party) -> \(signed2: Party) ->
            ubind
              u: Unit <-
                WITH_AUTHORITY @Unit required1
                 (ubind x1: ContractId M:T1 <- create @M:T1 M:T1 { signed = signed1, info = 100 }
                   in upure @Unit ());
              u: Unit <-
                WITH_AUTHORITY @Unit required2
                 (ubind x2: ContractId M:T1 <- create @M:T1 M:T1 { signed = signed2, info = 100 }
                   in upure @Unit ())
            in upure @Unit ();
       }
      """)

  def makeSetPartyValue(set: Set[Party]): SValue = {
    SList(FrontStack(set.toList.map(SParty(_)): _*))
  }

  def makeSingle(
      committers: Set[Party],
      required: Set[Party],
      signed: Party,
  ): Either[SError, SubmittedTransaction] = {
    val requiredV = makeSetPartyValue(required)
    val signedV = SParty(signed)
    val example = SEApp(pkgs.compiler.unsafeCompile(e"M:single"), Array(requiredV, signedV))
    val machine = Speedy.Machine.fromUpdateSExpr(pkgs, transactionSeed, example, committers)
    SpeedyTestLib.buildTransaction(machine)
  }

  def makeSequence1(
      committers: Set[Party],
      required: Set[Party],
      signed1: Party,
      signed2: Party,
  ): Either[SError, SubmittedTransaction] = {
    val requiredV = makeSetPartyValue(required)
    val signed1V = SParty(signed1)
    val signed2V = SParty(signed2)
    val example = SEApp(
      pkgs.compiler.unsafeCompile(e"M:sequence1"),
      Array(requiredV, signed1V, signed2V),
    )
    val machine = Speedy.Machine.fromUpdateSExpr(pkgs, transactionSeed, example, committers)
    SpeedyTestLib.buildTransaction(machine)
  }

  def makeSequence2(
      committers: Set[Party],
      required1: Set[Party],
      signed1: Party,
      required2: Set[Party],
      signed2: Party,
  ): Either[SError, SubmittedTransaction] = {
    val required1V = makeSetPartyValue(required1)
    val signed1V = SParty(signed1)
    val required2V = makeSetPartyValue(required2)
    val signed2V = SParty(signed2)
    val example = SEApp(
      pkgs.compiler.unsafeCompile(e"M:sequence2"),
      Array(required1V, signed1V, required2V, signed2V),
    )
    val machine = Speedy.Machine.fromUpdateSExpr(pkgs, transactionSeed, example, committers)
    SpeedyTestLib.buildTransaction(machine)
  }

  def makeNested(
      committers: Set[Party],
      outer: Set[Party],
      inner: Set[Party],
      signed: Party,
  ): Either[SError, SubmittedTransaction] = {
    val outerV = makeSetPartyValue(outer)
    val innerV = makeSetPartyValue(inner)
    val signedV = SParty(signed)
    val example = SEApp(pkgs.compiler.unsafeCompile(e"M:nested"), Array(outerV, innerV, signedV))
    val machine = Speedy.Machine.fromUpdateSExpr(pkgs, transactionSeed, example, committers)
    SpeedyTestLib.buildTransaction(machine)
  }

  sealed trait Shape // minimal transaction tree, for purposes of writing test expectation
  final case class Create(signed: Party) extends Shape
  final case class Exercise(x: List[Shape]) extends Shape
  final case class Rollback(x: List[Shape]) extends Shape
  final case class Authority(obtained: Set[Party], x: List[Shape]) extends Shape

  private def shapeOfTransaction(tx: SubmittedTransaction): List[Shape] = {
    def trees(nid: NodeId): List[Shape] = {
      tx.nodes(nid) match {
        case create: Node.Create =>
          create.arg match {
            case ValueRecord(_, ImmArray((None, ValueParty(signed)), _)) =>
              List(Create(signed))
            case _ =>
              sys.error(s"unexpected create.arg: ${create.arg}")
          }
        case _: Node.LeafOnlyAction =>
          Nil
        case node: Node.Exercise =>
          List(Exercise(node.children.toList.flatMap(nid => trees(nid))))
        case node: Node.Rollback =>
          List(Rollback(node.children.toList.flatMap(nid => trees(nid))))
        case node: Node.Authority =>
          List(Authority(node.obtained, node.children.toList.flatMap(nid => trees(nid))))
      }
    }
    tx.roots.toList.flatMap(nid => trees(nid))
  }
}
