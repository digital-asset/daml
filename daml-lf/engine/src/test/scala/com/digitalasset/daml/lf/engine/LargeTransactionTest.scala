// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import java.io.File

import com.digitalasset.daml.bazeltools.BazelRunfiles
import com.digitalasset.daml.lf.archive.{Decode, UniversalArchiveReader}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.{FrontStack, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.transaction.Transaction.Transaction
import com.digitalasset.daml.lf.transaction.{Node => N, Transaction => Tx}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value._
import com.digitalasset.daml.lf.command._
import org.scalameter
import org.scalameter.Quantity
import org.scalatest.{Assertion, Matchers, WordSpec}

import scala.language.implicitConversions

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class LargeTransactionTest extends WordSpec with Matchers with BazelRunfiles {

  private def hash(s: String, i: Int) =
    Some(crypto.Hash.hashPrivateKey(s + ":" + i.toString))

  private val participant = Ref.ParticipantId.assertFromString("participant")

  private def loadPackage(
      resource: String): (PackageId, Ast.Package, Map[PackageId, Ast.Package]) = {
    val packages =
      UniversalArchiveReader().readFile(new File(rlocation(resource))).get
    val packagesMap = Map(packages.all.map {
      case (pkgId, pkgArchive) => Decode.readArchivePayloadAndVersion(pkgId, pkgArchive)._1
    }: _*)
    val (mainPkgId, mainPkgArchive) = packages.main
    val mainPkg = Decode.readArchivePayloadAndVersion(mainPkgId, mainPkgArchive)._1._2
    (mainPkgId, mainPkg, packagesMap)
  }

  private[this] val (largeTxId, largeTxPkg, allPackages) = loadPackage(
    "daml-lf/tests/LargeTransaction.dar")
  private[this] val largeTx = (largeTxId, largeTxPkg)

  private[this] val party = Party.assertFromString("party")

  private def lookupPackage(pkgId: PackageId): Option[Ast.Package] = allPackages.get(pkgId)

  private def report(name: String, quantity: Quantity[Double]): Unit =
    println(s"$name: $quantity")

  List(5000, 50000, 500000)
    .foreach { txSize =>
      val testName = s"create large transaction with one contract containing $txSize Ints"
      testName in {
        report(
          testName,
          testLargeTransactionOneContract(InMemoryPrivateLedgerData(), Engine())(txSize))
      }
    }

  List(5000, 50000, 500000)
    .foreach { txSize =>
      val testName = s"create large transaction with $txSize small contracts"
      testName in {
        report(
          testName,
          testLargeTransactionManySmallContracts(InMemoryPrivateLedgerData(), Engine())(txSize))
      }
    }

  List(5000, 50000, 500000)
    .foreach { txSize =>
      val testName = s"execute choice with a List of $txSize Ints"
      testName in {
        report(testName, testLargeChoiceArgument(InMemoryPrivateLedgerData(), Engine())(txSize))
      }
    }

  private def testLargeTransactionOneContract(pcs: PrivateLedgerData, engine: Engine)(
      txSize: Int): Quantity[Double] = {
    val rangeOfIntsTemplateId = Identifier(largeTx._1, qn("LargeTransaction:RangeOfInts"))
    val createCmd = rangeOfIntsCreateCmd(rangeOfIntsTemplateId, 0, 1, txSize)
    val createCmdTx: Transaction =
      submitCommand(pcs, engine)(
        submitter = party,
        cmd = createCmd,
        cmdReference = "create RangeOfInts",
        seed = hash("testLargeTransactionOneContract:create", txSize))
    val contractId: AbsoluteContractId = firstRootNode(createCmdTx) match {
      case N.NodeCreate(_, x: RelativeContractId, _, _, _, _, _) =>
        AbsoluteContractId(pcs.toContractIdString(pcs.transactionCounter - 1)(x))
      case N.NodeCreate(_, x: AbsoluteContractId, _, _, _, _, _) => x
      case n @ _ => fail(s"Expected NodeCreate, but got: $n")
    }
    val exerciseCmd = toListContainerExerciseCmd(rangeOfIntsTemplateId, contractId)
    val (exerciseCmdTx, quanity) = measureWithResult(
      submitCommand(pcs, engine)(
        submitter = party,
        cmd = exerciseCmd,
        cmdReference = "exercise RangeOfInts.ToListContainer",
        seed = hash("testLargeTransactionOneContract:exercise", txSize),
      ))

    assertOneContractWithManyInts(exerciseCmdTx, List.range(0L, txSize.toLong))
    quanity
  }

  private def testLargeTransactionManySmallContracts(pcs: PrivateLedgerData, engine: Engine)(
      num: Int): Quantity[Double] = {
    val rangeOfIntsTemplateId = Identifier(largeTx._1, qn("LargeTransaction:RangeOfInts"))
    val createCmd = rangeOfIntsCreateCmd(rangeOfIntsTemplateId, 0, 1, num)
    val createCmdTx: Transaction =
      submitCommand(pcs, engine)(
        submitter = party,
        cmd = createCmd,
        cmdReference = "create RangeOfInts",
        seed = hash("testLargeTransactionManySmallContracts:create", num))
    val contractId: AbsoluteContractId = firstRootNode(createCmdTx) match {
      case N.NodeCreate(_, x: RelativeContractId, _, _, _, _, _) =>
        AbsoluteContractId(pcs.toContractIdString(pcs.transactionCounter - 1)(x))
      case N.NodeCreate(_, x: AbsoluteContractId, _, _, _, _, _) => x
      case n @ _ => fail(s"Expected NodeCreate, but got: $n")
    }
    val exerciseCmd = toListOfIntContainers(rangeOfIntsTemplateId, contractId)
    val (exerciseCmdTx, quanity) = measureWithResult(
      submitCommand(pcs, engine)(
        submitter = party,
        cmd = exerciseCmd,
        cmdReference = "exercise RangeOfInts.ToListContainer",
        seed = hash("testLargeTransactionManySmallContracts:exercise", num)
      ))

    assertManyContractsOneIntPerContract(exerciseCmdTx, num)
    quanity
  }

  private def testLargeChoiceArgument(pcs: PrivateLedgerData, engine: Engine)(
      size: Int): Quantity[Double] = {
    val listUtilTemplateId = Identifier(largeTx._1, qn("LargeTransaction:ListUtil"))
    val createCmd = listUtilCreateCmd(listUtilTemplateId)
    val createCmdTx: Transaction =
      submitCommand(pcs, engine)(
        submitter = party,
        cmd = createCmd,
        cmdReference = "create ListUtil",
        seed = hash("testLargeChoiceArgument:create", size))
    val contractId: AbsoluteContractId = firstRootNode(createCmdTx) match {
      case N.NodeCreate(_, x: RelativeContractId, _, _, _, _, _) =>
        AbsoluteContractId(pcs.toContractIdString(pcs.transactionCounter - 1)(x))
      case N.NodeCreate(_, x: AbsoluteContractId, _, _, _, _, _) => x
      case n @ _ => fail(s"Expected NodeCreate, but got: $n")
    }
    val exerciseCmd = sizeExerciseCmd(listUtilTemplateId, contractId)(size)
    val (exerciseCmdTx, quantity) = measureWithResult(
      submitCommand(pcs, engine)(
        submitter = party,
        cmd = exerciseCmd,
        cmdReference = "exercise ListUtil.Size",
        seed = hash("testLargeTransactionManySmallContracts:exercise", size)))

    assertSizeExerciseTransaction(exerciseCmdTx, size.toLong)
    quantity
  }

  private def qn(str: String): QualifiedName = QualifiedName.assertFromString(str)

  private def assertOneContractWithManyInts(
      exerciseCmdTx: Transaction,
      expected: List[Long]): Assertion = {

    val listValue = extractResultFieldFromExerciseTransaction(exerciseCmdTx, "list")

    val list: FrontStack[Value[Tx.TContractId]] = listValue match {
      case ValueList(x) => x
      case f @ _ => fail(s"Unexpected match: $f")
    }

    val actual: List[Long] = list.iterator.collect { case ValueInt64(x) => x }.toList
    actual shouldBe expected
  }

  private def assertManyContractsOneIntPerContract(
      exerciseCmdTx: Transaction,
      expectedNumberOfContracts: Int): Assertion = {

    val newContracts: List[N.GenNode.WithTxValue[Tx.NodeId, Tx.TContractId]] =
      firstRootNode(exerciseCmdTx) match {
        case ne: N.NodeExercises[_, _, _] => ne.children.toList.map(nid => exerciseCmdTx.nodes(nid))
        case n @ _ => fail(s"Unexpected match: $n")
      }

    newContracts.count {
      case N.NodeCreate(_, _, _, _, _, _, _) => true
      case n @ _ => fail(s"Unexpected match: $n")
    } shouldBe expectedNumberOfContracts
  }

  private def submitCommand(pcs: PrivateLedgerData, engine: Engine)(
      submitter: Party,
      cmd: Command,
      cmdReference: String,
      seed: Option[crypto.Hash]
  ): Tx.Transaction = {
    engine
      .submit(
        Commands(submitter, ImmArray(cmd), Time.Timestamp.now(), cmdReference),
        participant,
        seed,
      )
      .consume(pcs.get, lookupPackage, { _ =>
        sys.error("TODO keys for LargeTransactionTest")
      }) match {
      case Left(err) =>
        fail(s"Unexpected error: $err")
      case Right(tx) =>
        pcs.update(tx)
        tx
    }
  }

  private def rangeOfIntsCreateCmd(
      templateId: Identifier,
      start: Int,
      step: Int,
      number: Int): CreateCommand = {
    val fields = ImmArray(
      (Some[Name]("party"), ValueParty(party)),
      (Some[Name]("start"), ValueInt64(start.toLong)),
      (Some[Name]("step"), ValueInt64(step.toLong)),
      (Some[Name]("size"), ValueInt64(number.toLong))
    )
    CreateCommand(templateId, ValueRecord(Some(templateId), fields))
  }

  private def toListContainerExerciseCmd(
      templateId: Identifier,
      contractId: AbsoluteContractId): ExerciseCommand = {
    val choice = "ToListContainer"
    val emptyArgs = ValueRecord(None, ImmArray(Seq()))
    ExerciseCommand(templateId, contractId.coid, choice, (emptyArgs))
  }

  private def toListOfIntContainers(
      templateId: Identifier,
      contractId: AbsoluteContractId): ExerciseCommand = {
    val choice = "ToListOfIntContainers"
    val emptyArgs = ValueRecord(None, ImmArray(Seq()))
    ExerciseCommand(templateId, contractId.coid, choice, (emptyArgs))
  }

  private def listUtilCreateCmd(templateId: Identifier): CreateCommand = {
    val fields = ImmArray((Some[Name]("party"), ValueParty(party)))
    CreateCommand(templateId, ValueRecord(Some(templateId), fields))
  }

  private def sizeExerciseCmd(templateId: Identifier, contractId: AbsoluteContractId)(
      size: Int): ExerciseCommand = {
    val choice = "Size"
    val choiceDefRef = Identifier(templateId.packageId, qn(s"LargeTransaction:$choice"))
    val damlList = ValueList(FrontStack(elements = List.range(0L, size.toLong).map(ValueInt64)))
    val choiceArgs = ValueRecord(Some(choiceDefRef), ImmArray((None, damlList)))
    ExerciseCommand(templateId, contractId.coid, choice, (choiceArgs))
  }

  private def assertSizeExerciseTransaction(
      exerciseCmdTx: Transaction,
      expected: Long): Assertion = {

    val value: Value[Tx.TContractId] =
      extractResultFieldFromExerciseTransaction(exerciseCmdTx, "value")

    val actual: Long = value match {
      case ValueInt64(x) => x
      case f @ _ => fail(s"Unexpected match: $f")
    }

    actual shouldBe expected
  }

  private def extractResultFieldFromExerciseTransaction(
      exerciseCmdTx: Transaction,
      fieldName: String): Value[Tx.TContractId] = {

    val contractInst: ContractInst[Tx.Value[Tx.TContractId]] = extractResultFromExerciseTransaction(
      exerciseCmdTx)

    val fields: ImmArray[(Option[String], Value[Tx.TContractId])] =
      contractInst.arg.value match {
        case ValueRecord(_, x: ImmArray[_]) => x
        case v @ _ => fail(s"Unexpected match: $v")
      }

    val valueField: Option[(Option[String], Value[Tx.TContractId])] = fields.find {
      case (n, _) => n.contains(fieldName)
    }

    valueField match {
      case Some((Some(`fieldName`), x)) => x
      case f @ _ => fail(s"Unexpected match: $f")
    }
  }

  private def extractResultFromExerciseTransaction(
      exerciseCmdTx: Transaction): ContractInst[Tx.Value[Tx.TContractId]] = {

    exerciseCmdTx.roots.length shouldBe 1
    exerciseCmdTx.nodes.size shouldBe 2

    val createNode
      : N.GenNode.WithTxValue[Tx.NodeId, Tx.TContractId] = firstRootNode(exerciseCmdTx) match {
      case ne: N.NodeExercises[_, _, _] => exerciseCmdTx.nodes(ne.children.head)
      case n @ _ => fail(s"Unexpected match: $n")
    }

    createNode match {
      case N.NodeCreate(_, _, x: ContractInst[_], _, _, _, _) => x
      case n @ _ => fail(s"Unexpected match: $n")
    }
  }

  private def firstRootNode(tx: Tx.Transaction): Tx.Node = tx.nodes(tx.roots.head)

  private def measureWithResult[R](body: => R): (R, Quantity[Double]) = {
    lazy val result: R = body
    val quantity: Quantity[Double] = scalameter.measure(result)
    (result, quantity)
  }

  private implicit def toChoiceName(s: String): Ref.Name = Name.assertFromString(s)

}
