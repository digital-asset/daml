// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package value
package test

import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.transaction.Node.{
  GenNode,
  KeyWithMaintainers,
  NodeCreate,
  NodeExercises,
  NodeFetch,
  NodeLookupByKey,
  NodeRollback,
}
import com.daml.lf.transaction.{
  GenTransaction,
  NodeId,
  TransactionVersion,
  VersionedTransaction,
  Transaction => Tx,
}
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value._
import org.scalacheck.{Arbitrary, Gen}
import Arbitrary.arbitrary

import scala.Ordering.Implicits.infixOrderingOps
import scala.collection.immutable.HashMap
import scalaz.syntax.apply._
import scalaz.scalacheck.ScalaCheckBinding._

object ValueGenerators {

  import TransactionVersion.minExceptions

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
        (5, num),
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
    n <- Gen.choose(1, 64)
    packageId <- Gen
      .listOfN(n, Gen.alphaNumChar)
      .map(s => PackageId.assertFromString(s.mkString))
    module <- moduleGen
    name <- dottedNameGen
  } yield Identifier(packageId, QualifiedName(module, name))

  val nameGen: Gen[Name] = {
    val firstChars =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_$".toVector
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
  private def variantGen(nesting: Int): Gen[ValueVariant] =
    for {
      id <- idGen
      variantName <- nameGen
      toOption <- Gen
        .oneOf(true, false)
        .map(withoutLabels =>
          if (withoutLabels) (_: Identifier) => None
          else (variantId: Identifier) => Some(variantId)
        )
      value <- Gen.lzy(valueGen(nesting))
    } yield ValueVariant(toOption(id), variantName, value)

  def variantGen: Gen[ValueVariant] = variantGen(0)

  private def recordGen(nesting: Int): Gen[ValueRecord] =
    for {
      id <- idGen
      toOption <- Gen
        .oneOf(true, false)
        .map(a =>
          if (a) (_: Identifier) => None
          else (x: Identifier) => Some(x)
        )
      labelledValues <- Gen.listOf(
        nameGen.flatMap(label =>
          Gen.lzy(valueGen(nesting)).map(x => if (label.isEmpty) (None, x) else (Some(label), x))
        )
      )
    } yield ValueRecord(toOption(id), labelledValues.to(ImmArray))

  def recordGen: Gen[ValueRecord] = recordGen(0)

  private def valueOptionalGen(nesting: Int): Gen[ValueOptional] =
    Gen.option(valueGen(nesting)).map(v => ValueOptional(v))

  def valueOptionalGen: Gen[ValueOptional] = valueOptionalGen(0)

  private def valueListGen(nesting: Int): Gen[ValueList] =
    for {
      values <- Gen.listOf(Gen.lzy(valueGen(nesting)))
    } yield ValueList(values.to(FrontStack))

  def valueListGen: Gen[ValueList] = valueListGen(0)

  private def valueMapGen(nesting: Int) =
    for {
      list <- Gen.listOf(for {
        k <- Gen.asciiPrintableStr; v <- Gen.lzy(valueGen(nesting))
      } yield k -> v)
    } yield ValueTextMap(SortedLookupList(Map(list: _*)))

  def valueMapGen: Gen[ValueTextMap] = valueMapGen(0)

  private def valueGenMapGen(nesting: Int) =
    Gen
      .listOf(Gen.zip(Gen.lzy(valueGen(nesting)), Gen.lzy(valueGen(nesting))))
      .map(list => ValueGenMap(list.to(ImmArray)))

  def valueGenMapGen: Gen[ValueGenMap] = valueGenMapGen(0)

  private val genHash: Gen[crypto.Hash] =
    Gen
      .containerOfN[Array, Byte](
        crypto.Hash.underlyingHashLength,
        arbitrary[Byte],
      ) map crypto.Hash.assertFromByteArray
  private val genSuffixes: Gen[Bytes] = for {
    sz <- Gen.chooseNum(0, ContractId.V1.MaxSuffixLength)
    ab <- Gen.containerOfN[Array, Byte](sz, arbitrary[Byte])
  } yield Bytes fromByteArray ab

  val cidV0Gen: Gen[ContractId.V0] =
    Gen.alphaStr.map(t => Value.ContractId.V0.assertFromString('#' +: t.take(254)))
  private val cidV1Gen: Gen[ContractId.V1] =
    Gen.zip(genHash, genSuffixes) map { case (h, b) =>
      ContractId.V1.assertBuild(h, b)
    }

  /** Universes of totally-ordered ContractIds. */
  def comparableCoidsGen: Seq[Gen[ContractId]] =
    Seq(
      Gen.oneOf(
        cidV0Gen,
        Gen.zip(cidV1Gen, arbitrary[Byte]) map { case (b1, b) =>
          ContractId.V1
            .assertBuild(
              b1.discriminator,
              if (b1.suffix.nonEmpty) b1.suffix else Bytes fromByteArray Array(b),
            )
        },
      ),
      Gen.oneOf(cidV0Gen, cidV1Gen map (cid => ContractId.V1(cid.discriminator))),
    )

  def coidGen: Gen[ContractId] = Gen.oneOf(cidV0Gen, cidV1Gen)

  def coidValueGen: Gen[ValueContractId] =
    coidGen.map(ValueContractId(_))

  private def valueGen(nesting: Int): Gen[Value] = {
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
        if (nesting >= MAXIMUM_NESTING) { List() }
        else { nested } ++
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
      .map(s => Party.assertFromString(s.take(255).mkString))
  }

  def valueGen: Gen[Value] = valueGen(0)

  def versionedValueGen: Gen[VersionedValue] =
    for {
      value <- valueGen
      minVersion = TransactionBuilder.assertAssignVersion(value)
      version <- transactionVersionGen(minVersion)
    } yield VersionedValue(version, value)

  private[lf] val genMaybeEmptyParties: Gen[Set[Party]] = Gen.listOf(party).map(_.toSet)

  val genNonEmptyParties: Gen[Set[Party]] = ^(party, genMaybeEmptyParties)((hd, tl) => tl + hd)

  val contractInstanceGen: Gen[ContractInst[Value]] = {
    for {
      template <- idGen
      arg <- valueGen
      agreement <- Arbitrary.arbitrary[String]
    } yield ContractInst(template, arg, agreement)
  }

  val versionedContractInstanceGen: Gen[ContractInst[Value.VersionedValue]] =
    for {
      template <- idGen
      arg <- versionedValueGen
      agreement <- Arbitrary.arbitrary[String]
    } yield ContractInst(template, arg, agreement)

  val keyWithMaintainersGen: Gen[KeyWithMaintainers[Value]] = {
    for {
      key <- valueGen
      maintainers <- genNonEmptyParties
    } yield KeyWithMaintainers(key, maintainers)
  }

  /** Makes create nodes that violate the rules:
    *
    * 1. stakeholders may not be a superset of signatories
    * 2. key's maintainers may not be a subset of signatories
    */
  val malformedCreateNodeGen: Gen[NodeCreate] = {
    for {
      version <- transactionVersionGen()
      node <- malformedCreateNodeGenWithVersion(version)
    } yield node
  }

  /** Makes create nodes with the given version that violate the rules:
    *
    * 1. stakeholders may not be a superset of signatories
    * 2. key's maintainers may not be a subset of signatories
    */
  def malformedCreateNodeGenWithVersion(
      version: TransactionVersion
  ): Gen[NodeCreate] = {
    for {
      coid <- coidGen
      templateId <- idGen
      arg <- valueGen
      agreement <- Arbitrary.arbitrary[String]
      signatories <- genNonEmptyParties
      stakeholders <- genNonEmptyParties
      key <- Gen.option(keyWithMaintainersGen)
    } yield NodeCreate(
      coid,
      templateId,
      arg,
      agreement,
      signatories,
      stakeholders,
      key,
      version,
    )
  }

  val fetchNodeGen: Gen[NodeFetch] =
    for {
      version <- transactionVersionGen()
      node <- fetchNodeGenWithVersion(version)
    } yield node

  def fetchNodeGenWithVersion(version: TransactionVersion): Gen[NodeFetch] = {
    for {
      coid <- coidGen
      templateId <- idGen
      actingParties <- genNonEmptyParties
      signatories <- genNonEmptyParties
      stakeholders <- genNonEmptyParties
      key <- Gen.option(keyWithMaintainersGen)
      byKey <- Gen.oneOf(true, false)
    } yield NodeFetch(
      coid,
      templateId,
      actingParties,
      signatories,
      stakeholders,
      key,
      byKey,
      version,
    )
  }

  /** Makes rollback node with some random child IDs. */
  val danglingRefRollbackNodeGen: Gen[NodeRollback] = {
    for {
      children <- Gen
        .listOf(Arbitrary.arbInt.arbitrary)
        .map(_.map(NodeId(_)))
        .map(_.to(ImmArray))
    } yield NodeRollback(children)
  }

  /** Makes exercise nodes with the given version and some random child IDs. */
  val danglingRefExerciseNodeGen: Gen[NodeExercises] =
    for {
      version <- transactionVersionGen()
      node <- danglingRefExerciseNodeGenWithVersion(version)
    } yield node

  /** Makes exercise nodes with the given version and some random child IDs. */
  def danglingRefExerciseNodeGenWithVersion(
      version: TransactionVersion
  ): Gen[NodeExercises] = {
    for {
      targetCoid <- coidGen
      templateId <- idGen
      choiceId <- nameGen
      consume <- Gen.oneOf(true, false)
      actingParties <- genNonEmptyParties
      chosenValue <- valueGen
      stakeholders <- genNonEmptyParties
      signatories <- genNonEmptyParties
      choiceObservers <- genMaybeEmptyParties
      children <- Gen
        .listOf(Arbitrary.arbInt.arbitrary)
        .map(_.map(NodeId(_)))
        .map(_.to(ImmArray))
      exerciseResult <- if (version < minExceptions) valueGen.map(Some(_)) else Gen.option(valueGen)
      key <- Gen.option(keyWithMaintainersGen)
      byKey <- Gen.oneOf(true, false)
    } yield NodeExercises(
      targetCoid,
      templateId,
      choiceId,
      consume,
      actingParties,
      chosenValue,
      stakeholders,
      signatories,
      choiceObservers = choiceObservers,
      children,
      exerciseResult,
      key,
      byKey,
      version,
    )
  }

  val lookupNodeGen: Gen[NodeLookupByKey] =
    for {
      version <- transactionVersionGen()
      targetCoid <- coidGen
      templateId <- idGen
      key <- keyWithMaintainersGen
      result <- Gen.option(targetCoid)
    } yield NodeLookupByKey(
      templateId,
      key,
      result,
      version,
    )

  /** Makes nodes with the problems listed under `malformedCreateNodeGen`, and
    * `malformedGenTransaction` should they be incorporated into a transaction.
    */
  def danglingRefGenActionNode: Gen[(NodeId, Tx.ActionNode)] = {
    for {
      id <- Arbitrary.arbInt.arbitrary.map(NodeId(_))
      version <- transactionVersionGen()
      create = malformedCreateNodeGenWithVersion(version)
      exe = danglingRefExerciseNodeGenWithVersion(version)
      fetch = fetchNodeGenWithVersion(version)
      node <- Gen.oneOf(create, exe, fetch)
    } yield (id, node)
  }

  /** Makes nodes with the problems listed under `malformedCreateNodeGen`, and
    * `malformedGenTransaction` should they be incorporated into a transaction.
    */
  def danglingRefGenNode = for {
    version <- transactionVersionGen()
    node <- danglingRefGenNodeWithVersion(version)
  } yield node

  private[this] def refGenNode(g: Gen[Tx.Node]): Gen[(NodeId, Tx.Node)] =
    for {
      id <- Arbitrary.arbInt.arbitrary.map(NodeId(_))
      node <- g
    } yield (id, node)

  /** Version of danglingRefGenNode that allows to set the version of the node.
    *    Note that this only ensures that the node can be normalized to the given version.
    *    It does not normalize the node itself.
    */
  def danglingRefGenActionNodeWithVersion(version: TransactionVersion): Gen[(NodeId, Tx.Node)] =
    refGenNode(
      Gen.oneOf(
        malformedCreateNodeGenWithVersion(version),
        danglingRefExerciseNodeGenWithVersion(version),
        fetchNodeGenWithVersion(version),
      )
    )

  def danglingRefGenNodeWithVersion(version: TransactionVersion): Gen[(NodeId, Tx.Node)] = {
    if (version < minExceptions)
      danglingRefGenActionNodeWithVersion(version)
    else
      Gen.frequency(
        3 -> danglingRefGenActionNodeWithVersion(version),
        1 -> refGenNode(danglingRefRollbackNodeGen),
      )
  }

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
  val malformedGenTransaction: Gen[GenTransaction] = {
    for {
      nodes <- Gen.listOf(danglingRefGenNode)
      roots <- Gen.listOf(Arbitrary.arbInt.arbitrary.map(NodeId(_)))
    } yield GenTransaction(nodes.toMap, roots.to(ImmArray))
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

  val noDanglingRefGenTransaction: Gen[GenTransaction] = {

    def nonDanglingRefNodeGen(
        maxDepth: Int,
        nodeId: NodeId,
    ): Gen[(ImmArray[NodeId], HashMap[NodeId, Tx.Node])] = {

      val exerciseFreq = if (maxDepth <= 0) 0 else 1
      val rollbackFreq = if (maxDepth <= 0) 0 else 1

      def nodeGen(nodeId: NodeId): Gen[(NodeId, HashMap[NodeId, Tx.Node])] =
        for {
          node <- Gen.frequency(
            exerciseFreq -> danglingRefExerciseNodeGen,
            rollbackFreq -> danglingRefRollbackNodeGen,
            1 -> malformedCreateNodeGen,
            2 -> fetchNodeGen,
          )
          nodeWithChildren <- node match {
            case node: NodeExercises =>
              for {
                depth <- Gen.choose(0, maxDepth - 1)
                nodeWithChildren <- nonDanglingRefNodeGen(depth, nodeId)
                (children, nodes) = nodeWithChildren
              } yield node.copy(children = children) -> nodes
            case node: NodeRollback =>
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
          nodes: HashMap[NodeId, Tx.Node] = HashMap.empty,
      ): Gen[(ImmArray[NodeId], HashMap[NodeId, Tx.Node])] =
        if (size <= 0)
          Gen.const(nodeIds.toImmArray -> nodes)
        else
          nodeGen(NodeId(parentNodeId.index * 10 + size)).flatMap { case (nodeId, children) =>
            nodesGen(parentNodeId, size - 1, nodeIds :+ nodeId, nodes ++ children)
          }

      Gen.choose(0, 6).flatMap(nodesGen(nodeId, _))
    }

    nonDanglingRefNodeGen(3, NodeId(0)).map { case (nodeIds, nodes) =>
      GenTransaction(nodes, nodeIds)
    }
  }

  val noDanglingRefGenVersionedTransaction: Gen[VersionedTransaction] = {
    for {
      tx <- noDanglingRefGenTransaction
      txVer <- transactionVersionGen()
      nodeVersionGen = transactionVersionGen().filterNot(_ < txVer)
      nodes <- tx.fold(Gen.const(HashMap.empty[NodeId, GenNode])) { case (acc, (nodeId, node)) =>
        for {
          hashMap <- acc
        } yield hashMap.updated(nodeId, node)
      }
    } yield VersionedTransaction(txVer, nodes, tx.roots)

  }

  def stringVersionGen: Gen[String] = {
    val g: Gen[String] = for {
      major <- Gen.posNum[Int]
      minorO <- Gen.option(Gen.posNum[Int])
      raw = minorO.fold(major.toString)(x => s"$major.$x")
    } yield raw

    Gen.frequency((1, Gen.const("")), (10, g))
  }

  def transactionVersionGen(
      minVersion: TransactionVersion = TransactionVersion.minVersion, // inclusive
      maxVersion: Option[TransactionVersion] = None, // exclusive if defined
  ): Gen[TransactionVersion] =
    Gen.oneOf(TransactionVersion.All.filter(v => minVersion <= v && maxVersion.forall(v < _)))

  object Implicits {
    implicit val vdateArb: Arbitrary[Time.Date] = Arbitrary(dateGen)
    implicit val vtimestampArb: Arbitrary[Time.Timestamp] = Arbitrary(timestampGen)
    implicit val vpartyArb: Arbitrary[Ref.Party] = Arbitrary(party)
    implicit val scaleArb: Arbitrary[Numeric.Scale] = Arbitrary(Gen.oneOf(Numeric.Scale.values))
  }
}
