// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.transaction.Node.{
  KeyWithMaintainers,
  NodeCreate,
  NodeExercises,
  NodeFetch
}
import com.digitalasset.daml.lf.transaction.VersionTimeline.Implicits._
import com.digitalasset.daml.lf.transaction.{Transaction => Tx}
import com.digitalasset.daml.lf.transaction._
import com.digitalasset.daml.lf.value.Value._
import org.scalacheck.{Arbitrary, Gen}
import Arbitrary.arbitrary

import scala.collection.immutable.HashMap
import scalaz.syntax.apply._
import scalaz.scalacheck.ScalaCheckBinding._

object ValueGenerators {

  import Ref.LedgerString.{assertFromString => toContractId}

  //generate decimal values
  def numGen(scale: Numeric.Scale): Gen[Numeric] = {
    val num = for {
      integerPart <- Gen.listOfN(Numeric.maxPrecision - scale, Gen.choose(1, 9)).map(_.mkString)
      decimalPart <- Gen.listOfN(scale, Gen.choose(1, 9)).map(_.mkString)
    } yield Numeric.assertFromString(s"$integerPart.$decimalPart")

    Gen
      .frequency(
        (1, Gen.const(Numeric.assertFromBigDecimal(scale, 0))),
        (1, Gen.const(Numeric.maxValue(scale))),
        (1, Gen.const(Numeric.minValue(scale))),
        (5, num)
      )
  }

  def unscaledNumGen: Gen[Numeric] =
    Gen.oneOf(Numeric.Scale.values).flatMap(numGen)

  val moduleSegmentGen: Gen[String] = for {
    n <- Gen.choose(1, 100)
    name <- Gen.listOfN(n, Gen.alphaChar)
  } yield name.mkString.capitalize
  val moduleGen: Gen[ModuleName] = for {
    n <- Gen.choose(1, 10)
    segments <- Gen.listOfN(n, moduleSegmentGen)
  } yield ModuleName.assertFromSegments(segments)

  val dottedNameSegmentGen: Gen[String] = for {
    ch <- Gen.alphaLowerChar
    n <- Gen.choose(0, 99)
    name <- Gen.listOfN(n, Gen.alphaChar)
  } yield (ch :: name).mkString
  val dottedNameGen: Gen[DottedName] = for {
    n <- Gen.choose(1, 10)
    segments <- Gen.listOfN(n, dottedNameSegmentGen)
  } yield DottedName.assertFromSegments(segments)

  // generate a junk identifier
  val idGen: Gen[Identifier] = for {
    n <- Gen.choose(1, 200)
    packageId <- Gen
      .listOfN(n, Gen.alphaNumChar)
      .map(s => PackageId.assertFromString(s.mkString))
    module <- moduleGen
    name <- dottedNameGen
  } yield Identifier(packageId, QualifiedName(module, name))

  val nameGen: Gen[Name] = {
    val firstChars =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ$_".toVector
    val mainChars =
      firstChars ++ "1234567890"
    for {
      h <- Gen.oneOf(firstChars)
      t <- Gen.listOf(Gen.oneOf(mainChars))
    } yield Name.assertFromString((h :: t).mkString)
  }

  // generate a more or less acceptable date value
  private val minDate = Time.Date.assertFromString("1900-01-01")
  private val maxDate = Time.Date.assertFromString("2100-12-31")
  val dateGen: Gen[Time.Date] = for {
    i <- Gen.chooseNum(minDate.days, maxDate.days)
  } yield Time.Date.assertFromDaysSinceEpoch(i)

  val timestampGen: Gen[Time.Timestamp] =
    Gen
      .chooseNum(Time.Timestamp.MinValue.micros, Time.Timestamp.MaxValue.micros)
      .map(Time.Timestamp.assertFromLong)

  // generate a variant with arbitrary value
  private def variantGen(nesting: Int): Gen[ValueVariant[ContractId]] =
    for {
      id <- idGen
      variantName <- nameGen
      toOption <- Gen
        .oneOf(true, false)
        .map(
          withoutLabels =>
            if (withoutLabels) (_: Identifier) => None
            else (variantId: Identifier) => Some(variantId))
      value <- Gen.lzy(valueGen(nesting))
    } yield ValueVariant(toOption(id), variantName, value)
  def variantGen: Gen[ValueVariant[ContractId]] = variantGen(0)

  private def recordGen(nesting: Int): Gen[ValueRecord[ContractId]] =
    for {
      id <- idGen
      toOption <- Gen
        .oneOf(true, false)
        .map(
          a =>
            if (a) (_: Identifier) => None
            else (x: Identifier) => Some(x))
      labelledValues <- Gen.listOf(nameGen.flatMap(label =>
        Gen.lzy(valueGen(nesting)).map(x => if (label.isEmpty) (None, x) else (Some(label), x))))
    } yield ValueRecord[ContractId](toOption(id), ImmArray(labelledValues))
  def recordGen: Gen[ValueRecord[ContractId]] = recordGen(0)

  private def valueOptionalGen(nesting: Int): Gen[ValueOptional[ContractId]] =
    Gen.option(valueGen(nesting)).map(v => ValueOptional(v))
  def valueOptionalGen: Gen[ValueOptional[ContractId]] = valueOptionalGen(0)

  private def valueListGen(nesting: Int): Gen[ValueList[ContractId]] =
    for {
      values <- Gen.listOf(Gen.lzy(valueGen(nesting)))
    } yield ValueList[ContractId](FrontStack(values))
  def valueListGen: Gen[ValueList[ContractId]] = valueListGen(0)

  private def valueMapGen(nesting: Int) =
    for {
      list <- Gen.listOf(for {
        k <- Gen.asciiPrintableStr; v <- Gen.lzy(valueGen(nesting))
      } yield k -> v)
    } yield ValueTextMap[ContractId](SortedLookupList(Map(list: _*)))
  def valueMapGen: Gen[ValueTextMap[ContractId]] = valueMapGen(0)

  private def valueGenMapGen(nesting: Int) =
    Gen
      .listOf(Gen.zip(Gen.lzy(valueGen(nesting)), Gen.lzy(valueGen(nesting))))
      .map(list => ValueGenMap[ContractId](ImmArray(list)))

  def valueGenMapGen: Gen[ValueGenMap[ContractId]] = valueGenMapGen(0)

  private val genRel: Gen[ContractId] =
    Arbitrary.arbInt.arbitrary.map(i => RelativeContractId(Tx.NodeId(i)))
  private val genAbsV0: Gen[ContractId] =
    Gen.zip(Gen.alphaChar, Gen.alphaStr) map {
      case (h, t) => AbsoluteContractId(toContractId(h +: t))
    }

  def coidGen: Gen[ContractId] =
    Gen.frequency((1, genRel), (3, genAbsV0))

  def coidValueGen: Gen[ValueContractId[ContractId]] =
    coidGen.map(ValueContractId(_))

  private def valueGen(nesting: Int): Gen[Value[ContractId]] = {
    Gen.sized(sz => {
      val newNesting = nesting + 1
      val nested = List(
        (sz / 2 + 1, Gen.resize(sz / 5, valueListGen(newNesting))),
        (sz / 2 + 1, Gen.resize(sz / 5, variantGen(newNesting))),
        (sz / 2 + 1, Gen.resize(sz / 5, recordGen(newNesting))),
        (sz / 2 + 1, Gen.resize(sz / 5, valueOptionalGen(newNesting))),
        (sz / 2 + 1, Gen.resize(sz / 5, valueMapGen(newNesting))),
      )
      val flat = List(
        (sz + 1, dateGen.map(ValueDate)),
        (sz + 1, Gen.alphaStr.map(ValueText)),
        (sz + 1, unscaledNumGen.map(ValueNumeric)),
        (sz + 1, numGen(Decimal.scale).map(ValueNumeric)),
        (sz + 1, Arbitrary.arbLong.arbitrary.map(ValueInt64)),
        (sz + 1, Gen.alphaStr.map(ValueText)),
        (sz + 1, timestampGen.map(ValueTimestamp)),
        (sz + 1, coidValueGen),
        (sz + 1, party.map(ValueParty)),
        (sz + 1, Gen.oneOf(ValueTrue, ValueFalse)),
      )
      val all =
        if (nesting >= MAXIMUM_NESTING) { List() } else { nested } ++
          flat
      Gen.frequency(all: _*)
    })
  }

  private val simpleChars =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_ ".toVector

  def simpleStr: Gen[PackageId] = {
    Gen
      .nonEmptyListOf(Gen.oneOf(simpleChars))
      .map(s => PackageId.assertFromString(s.mkString))
  }

  def party: Gen[Party] = {
    Gen
      .nonEmptyListOf(Gen.oneOf(simpleChars))
      .map(s => Party.assertFromString(s.mkString))
  }

  def valueGen: Gen[Value[ContractId]] = valueGen(0)

  def versionedValueGen: Gen[VersionedValue[ContractId]] =
    for {
      value <- valueGen
      minVersion = ValueVersions.assertAssignVersion(value)
      version <- valueVersionGen(minVersion)
    } yield VersionedValue(version, value)

  private[lf] val genMaybeEmptyParties: Gen[Set[Party]] = Gen.listOf(party).map(_.toSet)

  val genNonEmptyParties: Gen[Set[Party]] = ^(party, genMaybeEmptyParties)((hd, tl) => tl + hd)

  @deprecated("use genNonEmptyParties instead", since = "100.11.17")
  private[lf] def genParties = genNonEmptyParties

  val contractInstanceGen: Gen[ContractInst[Tx.Value[Tx.TContractId]]] = {
    for {
      template <- idGen
      arg <- versionedValueGen
      agreement <- Arbitrary.arbitrary[String]
    } yield ContractInst(template, arg, agreement)
  }

  val keyWithMaintainersGen: Gen[KeyWithMaintainers[Tx.Value[Tx.TContractId]]] = {
    for {
      key <- versionedValueGen
      maintainers <- genNonEmptyParties
    } yield KeyWithMaintainers(key, maintainers)
  }

  /** Makes create nodes that violate the rules:
    *
    * 1. stakeholders may not be a superset of signatories
    * 2. key's maintainers may not be a subset of signatories
    */
  val malformedCreateNodeGen: Gen[NodeCreate.WithTxValue[Tx.TContractId]] = {
    for {
      coid <- coidGen
      coinst <- contractInstanceGen
      signatories <- genNonEmptyParties
      stakeholders <- genNonEmptyParties
      key <- Gen.option(keyWithMaintainersGen)
    } yield NodeCreate(None, coid, coinst, None, signatories, stakeholders, key)
  }

  val fetchNodeGen: Gen[NodeFetch[ContractId]] = {
    for {
      coid <- coidGen
      templateId <- idGen
      actingParties <- genNonEmptyParties
      signatories <- genNonEmptyParties
      stakeholders <- genNonEmptyParties
    } yield NodeFetch(coid, templateId, None, Some(actingParties), signatories, stakeholders)
  }

  /** Makes exercise nodes with some random child IDs. */
  val danglingRefExerciseNodeGen
    : Gen[NodeExercises[Tx.NodeId, Tx.TContractId, Tx.Value[Tx.TContractId]]] = {
    for {
      targetCoid <- coidGen
      templateId <- idGen
      choiceId <- nameGen
      consume <- Gen.oneOf(true, false)
      actingParties <- genNonEmptyParties
      chosenValue <- versionedValueGen
      stakeholders <- genNonEmptyParties
      signatories <- genNonEmptyParties
      children <- Gen
        .listOf(Arbitrary.arbInt.arbitrary)
        .map(_.map(Transaction.NodeId(_)))
        .map(ImmArray(_))
      exerciseResultValue <- versionedValueGen
      key <- versionedValueGen
      maintainers <- genNonEmptyParties
    } yield
      NodeExercises(
        None,
        targetCoid,
        templateId,
        choiceId,
        None,
        consume,
        actingParties,
        chosenValue,
        stakeholders,
        signatories,
        actingParties,
        children,
        Some(exerciseResultValue),
        Some(KeyWithMaintainers(key, maintainers))
      )
  }

  @deprecated("use danglingRefExerciseNodeGen instead", since = "100.11.17")
  private[lf] def exerciseNodeGen = danglingRefExerciseNodeGen

  /** Makes nodes with the problems listed under `malformedCreateNodeGen`, and
    * `malformedGenTransaction` should they be incorporated into a transaction.
    */
  val danglingRefGenNode: Gen[(NodeId, Tx.Node)] = {
    for {
      id <- Arbitrary.arbInt.arbitrary.map(NodeId(_))
      node <- Gen.oneOf(malformedCreateNodeGen, danglingRefExerciseNodeGen, fetchNodeGen)
    } yield (id, node)
  }

  @deprecated("use danglingRefGenNode instead", since = "100.11.17")
  private[lf] def genNode = danglingRefGenNode

  /** Aside from the invariants failed as listed under `malformedCreateNodeGen`,
    * resulting transactions may be malformed in several other ways:
    *
    * 1. The "exactly once" invariant of `node_id` is almost certain to fail.
    *    roots won't match up with nodes' actual IDs, exercise nodes' children
    *    will refer to nonexistent or duplicate nodes, or even the exercise node
    *    itself.  Therefore most transaction folds will not terminate.
    * 2. For fetch and exercise nodes, if the contract_id is relative, the
    *    associated invariants will probably fail; a create node with that ID
    *    may not exist, and even if it does, the stakeholders and signatories
    *    may not match up.
    *
    * This list is complete as of transaction version 5. -SC
    */
  val malformedGenTransaction: Gen[Tx.Transaction] = {
    for {
      nodes <- Gen.listOf(danglingRefGenNode)
      roots <- Gen.listOf(Arbitrary.arbInt.arbitrary.map(Tx.NodeId(_)))
    } yield GenTransaction(HashMap(nodes: _*), ImmArray(roots), None)
  }

  /*
   * Create a transaction without no dangling nodeId.
   *
   *  Data expect nodeId are still generated completely randomly so for
   *  fetch and exercise nodes, if the contract_id is relative, the
   *  associated invariants will probably fail; a create node with that ID
   *  may not exist, and even if it does, the stakeholders and signatories
   *  may not match up.
   *
   */

  val noDanglingRefGenTransaction: Gen[Tx.Transaction] = {

    def nonDanglingRefNodeGen(
        maxDepth: Int,
        nodeId: NodeId
    ): Gen[(ImmArray[NodeId], HashMap[NodeId, Tx.Node])] = {

      val exerciseFreq = if (maxDepth <= 0) 0 else 1

      def nodeGen(nodeId: NodeId): Gen[(NodeId, HashMap[NodeId, Tx.Node])] =
        for {
          node <- Gen.frequency(
            exerciseFreq -> danglingRefExerciseNodeGen,
            1 -> malformedCreateNodeGen,
            2 -> fetchNodeGen
          )
          nodeWithChildren <- node match {
            case node: NodeExercises.WithTxValue[Tx.NodeId, Tx.TContractId] =>
              for {
                depth <- Gen.choose(0, maxDepth - 1)
                nodeWithChildren <- nonDanglingRefNodeGen(depth, nodeId)
                (children, nodes) = nodeWithChildren
              } yield node.copy(children = children) -> nodes
            case node =>
              Gen.const(node -> HashMap.empty[NodeId, Tx.Node])
          }
          (node, nodes) = nodeWithChildren
        } yield nodeId -> nodes.updated(nodeId, node)

      def nodesGen(
          parentNodeId: NodeId,
          size: Int,
          nodeIds: BackStack[NodeId] = BackStack.empty,
          nodes: HashMap[NodeId, Tx.Node] = HashMap.empty
      ): Gen[(ImmArray[NodeId], HashMap[NodeId, Tx.Node])] =
        if (size <= 0)
          Gen.const(nodeIds.toImmArray -> nodes)
        else
          nodeGen(Tx.NodeId(parentNodeId.index * 10 + size)).flatMap {
            case (nodeId, children) =>
              nodesGen(parentNodeId, size - 1, nodeIds :+ nodeId, nodes ++ children)
          }

      Gen.choose(0, 6).flatMap(nodesGen(nodeId, _))
    }

    nonDanglingRefNodeGen(3, Tx.NodeId(0)).map {
      case (nodeIds, nodes) =>
        GenTransaction(
          nodes,
          nodeIds,
          None
        )
    }
  }

  val genBlindingInfo: Gen[BlindingInfo] = {
    val nodePartiesGen = Gen.mapOf(
      arbitrary[Int]
        .map(NodeId(_))
        .flatMap(n => genMaybeEmptyParties.map(ps => (n, ps))))
    for {
      disclosed1 <- nodePartiesGen
      disclosed2 <- nodePartiesGen
      divulged <- Gen.mapOf(
        Gen
          .nonEmptyListOf(Gen.alphaChar)
          .map(s => AbsoluteContractId(toContractId(s.mkString)))
          .flatMap(c => genMaybeEmptyParties.map(ps => (c, ps))))
    } yield BlindingInfo(disclosed1, disclosed2, divulged)
  }

  def stringVersionGen: Gen[String] = {
    val g: Gen[String] = for {
      major <- Gen.posNum[Int]
      minorO <- Gen.option(Gen.posNum[Int])
      raw = minorO.fold(major.toString)(x => s"$major.$x")
    } yield raw

    Gen.frequency((1, Gen.const("")), (10, g))
  }

  def valueVersionGen(minVersion: ValueVersion = ValueVersions.minVersion): Gen[ValueVersion] =
    Gen.oneOf(ValueVersions.acceptedVersions.filterNot(_ precedes minVersion).toSeq)

  def unsupportedValueVersionGen: Gen[ValueVersion] =
    stringVersionGen.map(ValueVersion).filter(x => !ValueVersions.acceptedVersions.contains(x))

  def transactionVersionGen: Gen[TransactionVersion] =
    Gen.oneOf(TransactionVersions.acceptedVersions.toSeq)

  def unsupportedTransactionVersionGen: Gen[TransactionVersion] =
    stringVersionGen
      .map(TransactionVersion)
      .filter(x => !TransactionVersions.acceptedVersions.contains(x))

  object Implicits {
    implicit val vdateArb: Arbitrary[Time.Date] = Arbitrary(dateGen)
    implicit val vtimestampArb: Arbitrary[Time.Timestamp] = Arbitrary(timestampGen)
    implicit val vpartyArb: Arbitrary[Ref.Party] = Arbitrary(party)
    implicit val scaleArb: Arbitrary[Numeric.Scale] = Arbitrary(Gen.oneOf(Numeric.Scale.values))
  }
}
