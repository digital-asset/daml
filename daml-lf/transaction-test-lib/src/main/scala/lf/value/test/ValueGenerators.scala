// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package value
package test

import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.transaction.{
  GlobalKey,
  GlobalKeyWithMaintainers,
  Node,
  NodeId,
  Transaction,
  TransactionVersion,
  Versioned,
  VersionedTransaction,
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
  import TransactionVersion.minInterfaces

  // generate decimal values
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

  def pkgNameGen(version: TransactionVersion): Gen[Option[PackageName]] =
    if (version < TransactionVersion.minUpgrade)
      None
    else
      for {
        n <- Gen.choose(1, 64)
        pkgName <- Gen
          .listOfN(n, Gen.alphaNumChar)
          .map(s => PackageName.assertFromString(s.mkString))
      } yield Some(pkgName)

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
    val suffixLengthGen = Gen.frequency(
      90 -> Gen.choose(0, 9),
      8 -> Gen.choose(0, 99),
      2 -> Gen.choose(0, 999),
    )
    for {
      h <- Gen.oneOf(firstChars)
      suffixLength <- suffixLengthGen
      t <- Gen.listOfN(suffixLength, Gen.oneOf(mainChars))
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
  def variantGen: Gen[ValueVariant] =
    for {
      id <- idGen
      variantName <- nameGen
      toOption <- Gen
        .oneOf(true, false)
        .map(withoutLabels =>
          if (withoutLabels) (_: Identifier) => None
          else (variantId: Identifier) => Some(variantId)
        )
      value <- valueGen()
    } yield ValueVariant(toOption(id), variantName, value)

  def recordGen: Gen[ValueRecord] =
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
          valueGen().map(x => if (label.isEmpty) (None, x) else (Some(label), x))
        )
      )
    } yield ValueRecord(toOption(id), labelledValues.to(ImmArray))

  def valueOptionalGen: Gen[ValueOptional] =
    Gen.option(valueGen()).map(v => ValueOptional(v))

  def valueListGen: Gen[ValueList] =
    for {
      values <- Gen.listOf(valueGen())
    } yield ValueList(values.to(FrontStack))

  def valueMapGen: Gen[ValueTextMap] =
    for {
      list <- Gen.listOf(for {
        k <- Gen.asciiPrintableStr; v <- valueGen()
      } yield k -> v)
    } yield ValueTextMap(SortedLookupList(Map(list: _*)))

  def valueGenMapGen: Gen[ValueGenMap] =
    Gen
      .listOf(Gen.zip(valueGen(), valueGen()))
      .map(list => ValueGenMap(list.distinctBy(_._1).to(ImmArray)))

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

  private val cidV1Gen: Gen[ContractId.V1] =
    Gen.zip(genHash, genSuffixes) map { case (h, b) =>
      ContractId.V1.assertBuild(h, b)
    }

  /** Universes of totally-ordered ContractIds. */
  def comparableCoidsGen: Seq[Gen[ContractId]] =
    Seq(suffixedV1CidGen, nonSuffixedCidV1Gen)

  def suffixedV1CidGen: Gen[ContractId] = Gen.zip(cidV1Gen, arbitrary[Byte]) map { case (b1, b) =>
    ContractId.V1
      .assertBuild(
        b1.discriminator,
        if (b1.suffix.nonEmpty) b1.suffix else Bytes fromByteArray Array(b),
      )
  }

  def nonSuffixedCidV1Gen: Gen[ContractId] = cidV1Gen map (cid => ContractId.V1(cid.discriminator))

  def coidGen: Gen[ContractId] = cidV1Gen

  def coidValueGen: Gen[ValueContractId] =
    coidGen.map(ValueContractId(_))

  private def nestedGen = Gen.oneOf(
    valueListGen,
    variantGen,
    recordGen,
    valueOptionalGen,
    valueMapGen,
  )

  private def flatGen = Gen.oneOf(
    dateGen.map(ValueDate),
    Gen.alphaStr.map(ValueText),
    unscaledNumGen.map(ValueNumeric),
    numGen(Decimal.scale).map(ValueNumeric),
    arbitrary[Long].map(ValueInt64),
    Gen.alphaStr.map(ValueText),
    timestampGen.map(ValueTimestamp),
    coidValueGen,
    party.map(ValueParty),
    Gen.oneOf(ValueTrue, ValueFalse),
    Gen.const(ValueUnit),
  )

  def valueGen(nested: => Gen[Value] = nestedGen): Gen[Value] = Gen.lzy {
    Gen.sized { size =>
      for {
        s <- Gen.choose(0, size)
        value <-
          if (s < 1) flatGen
          else
            Gen.frequency(
              5 -> flatGen,
              1 -> Gen.resize(size / (s + 1), nested),
            )
      } yield value
    }
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

  def versionedValueGen: Gen[VersionedValue] =
    for {
      value <- valueGen()
      minVersion = TransactionBuilder.assertAssignVersion(value)
      version <- transactionVersionGen(minVersion)
    } yield Versioned(version, value)

  private[lf] val genMaybeEmptyParties: Gen[Set[Party]] = Gen.listOf(party).map(_.toSet)

  val genNonEmptyParties: Gen[Set[Party]] = ^(party, genMaybeEmptyParties)((hd, tl) => tl + hd)

  val versionedContractInstanceGen: Gen[Value.VersionedContractInstance] =
    for {
      template <- idGen
      arg <- versionedValueGen
      pkgName <- pkgNameGen(arg.version)
    } yield arg.map(Value.ContractInstance(pkgName, template, _))

  def keyWithMaintainersGen(templateId: TypeConName): Gen[GlobalKeyWithMaintainers] = {
    for {
      key <- valueGen()
      maintainers <- genNonEmptyParties
      gkey = GlobalKey.build(templateId, key).toOption
      if gkey.isDefined
    } yield GlobalKeyWithMaintainers(gkey.get, maintainers)
  }

  /** Makes create nodes that violate the rules:
    *
    * 1. stakeholders may not be a superset of signatories
    * 2. key's maintainers may not be a subset of signatories
    */
  def malformedCreateNodeGen(
      minVersion: TransactionVersion = TransactionVersion.V14
  ): Gen[Node.Create] = {
    for {
      version <- transactionVersionGen(minVersion)
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
  ): Gen[Node.Create] =
    for {
      coid <- coidGen
      packageName <- pkgNameGen(version)
      templateId <- idGen
      arg <- valueGen()
      signatories <- genNonEmptyParties
      stakeholders <- genNonEmptyParties
      key <- Gen.option(keyWithMaintainersGen(templateId))
    } yield Node.Create(
      coid = coid,
      packageName = packageName,
      templateId = templateId,
      arg = arg,
      agreementText = "", // to be removed
      signatories = signatories,
      stakeholders = stakeholders,
      keyOpt = key,
      version = version,
    )

  val fetchNodeGen: Gen[Node.Fetch] =
    for {
      version <- transactionVersionGen()
      node <- fetchNodeGenWithVersion(version)
    } yield node

  def fetchNodeGenWithVersion(version: TransactionVersion): Gen[Node.Fetch] =
    for {
      coid <- coidGen
      pkgName <- pkgNameGen(version)
      templateId <- idGen
      actingParties <- genNonEmptyParties
      signatories <- genNonEmptyParties
      stakeholders <- genNonEmptyParties
      key <- Gen.option(keyWithMaintainersGen(templateId))
      byKey <- Gen.oneOf(true, false)
    } yield Node.Fetch(
      coid = coid,
      packageName = pkgName,
      templateId = templateId,
      actingParties = actingParties,
      signatories = signatories,
      stakeholders = stakeholders,
      keyOpt = key,
      byKey = byKey,
      version = version,
    )

  /** Makes rollback node with some random child IDs. */
  val danglingRefRollbackNodeGen: Gen[Node.Rollback] = {
    for {
      children <- Gen
        .listOf(Arbitrary.arbInt.arbitrary)
        .map(_.map(NodeId(_)))
        .map(_.to(ImmArray))
    } yield Node.Rollback(children)
  }

  /** Makes exercise nodes with the given version and some random child IDs. */
  val danglingRefExerciseNodeGen: Gen[Node.Exercise] =
    for {
      version <- transactionVersionGen()
      node <- danglingRefExerciseNodeGenWithVersion(version)
    } yield node

  /** Makes exercise nodes with the given version and some random child IDs. */
  def danglingRefExerciseNodeGenWithVersion(
      version: TransactionVersion
  ): Gen[Node.Exercise] =
    for {
      targetCoid <- coidGen
      pkgName <- pkgNameGen(version)
      templateId <- idGen
      interfaceId <- if (version < minInterfaces) Gen.const(None) else Gen.option(idGen)
      choiceId <- nameGen
      consume <- Gen.oneOf(true, false)
      actingParties <- genNonEmptyParties
      chosenValue <- valueGen()
      stakeholders <- genNonEmptyParties
      signatories <- genNonEmptyParties
      choiceObservers <- genMaybeEmptyParties
      choiceAuthorizersList <- genMaybeEmptyParties
      choiceAuthorizers = if (choiceAuthorizersList.isEmpty) None else Some(choiceAuthorizersList)
      children <- Gen
        .listOf(Arbitrary.arbInt.arbitrary)
        .map(_.map(NodeId(_)))
        .map(_.to(ImmArray))
      exerciseResult <-
        if (version < minExceptions) valueGen().map(Some(_)) else Gen.option(valueGen())
      key <- Gen.option(keyWithMaintainersGen(templateId))
      byKey <- Gen.oneOf(true, false)
    } yield Node.Exercise(
      targetCoid = targetCoid,
      packageName = pkgName,
      templateId = templateId,
      interfaceId = interfaceId,
      choiceId = choiceId,
      consuming = consume,
      actingParties = actingParties,
      chosenValue = chosenValue,
      stakeholders = stakeholders,
      signatories = signatories,
      choiceObservers = choiceObservers,
      choiceAuthorizers = choiceAuthorizers,
      children = children,
      exerciseResult = exerciseResult,
      keyOpt = key,
      byKey = byKey,
      version = version,
    )

  val lookupNodeGen: Gen[Node.LookupByKey] =
    for {
      version <- transactionVersionGen()
      targetCoid <- coidGen
      pkgName <- pkgNameGen(version)
      templateId <- idGen
      key <- keyWithMaintainersGen(templateId)
      result <- Gen.option(targetCoid)
    } yield Node.LookupByKey(
      packageName = pkgName,
      templateId,
      key,
      result,
      version,
    )

  /** Makes nodes with the problems listed under `malformedCreateNodeGen`, and
    * `malformedGenTransaction` should they be incorporated into a transaction.
    */
  def danglingRefGenActionNode: Gen[(NodeId, Node.Action)] = {
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

  private[this] def refGenNode(g: Gen[Node]): Gen[(NodeId, Node)] =
    for {
      id <- Arbitrary.arbInt.arbitrary.map(NodeId(_))
      node <- g
    } yield (id, node)

  /** Version of danglingRefGenNode that allows to set the version of the node.
    *    Note that this only ensures that the node can be normalized to the given version.
    *    It does not normalize the node itself.
    */
  def danglingRefGenActionNodeWithVersion(version: TransactionVersion): Gen[(NodeId, Node)] =
    refGenNode(
      Gen.oneOf(
        malformedCreateNodeGenWithVersion(version),
        danglingRefExerciseNodeGenWithVersion(version),
        fetchNodeGenWithVersion(version),
      )
    )

  def danglingRefGenNodeWithVersion(version: TransactionVersion): Gen[(NodeId, Node)] = {
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
  val malformedGenTransaction: Gen[Transaction] = {
    for {
      nodes <- Gen.listOf(danglingRefGenNode)
      roots <- Gen.listOf(Arbitrary.arbInt.arbitrary.map(NodeId(_)))
    } yield Transaction(nodes.toMap, roots.to(ImmArray))
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

  val noDanglingRefGenTransaction: Gen[Transaction] = {

    def nonDanglingRefNodeGen(
        maxDepth: Int,
        nodeId: NodeId,
    ): Gen[(ImmArray[NodeId], HashMap[NodeId, Node])] = {

      val exerciseFreq = if (maxDepth <= 0) 0 else 1
      val rollbackFreq = if (maxDepth <= 0) 0 else 1

      def nodeGen(nodeId: NodeId): Gen[(NodeId, HashMap[NodeId, Node])] =
        for {
          node <- Gen.frequency(
            exerciseFreq -> danglingRefExerciseNodeGen,
            rollbackFreq -> danglingRefRollbackNodeGen,
            1 -> malformedCreateNodeGen(),
            2 -> fetchNodeGen,
          )
          nodeWithChildren <- node match {
            case node: Node.Exercise =>
              for {
                depth <- Gen.choose(0, maxDepth - 1)
                nodeWithChildren <- nonDanglingRefNodeGen(depth, nodeId)
                (children, nodes) = nodeWithChildren
              } yield node.copy(children = children) -> nodes
            case node: Node.Rollback =>
              for {
                depth <- Gen.choose(0, maxDepth - 1)
                nodeWithChildren <- nonDanglingRefNodeGen(depth, nodeId)
                (children, nodes) = nodeWithChildren
              } yield node.copy(children = children) -> nodes
            case node =>
              Gen.const(node -> HashMap.empty[NodeId, Node])
          }
          (node, nodes) = nodeWithChildren
        } yield nodeId -> nodes.updated(nodeId, node)

      def nodesGen(
          parentNodeId: NodeId,
          size: Int,
          nodeIds: BackStack[NodeId] = BackStack.empty,
          nodes: HashMap[NodeId, Node] = HashMap.empty,
      ): Gen[(ImmArray[NodeId], HashMap[NodeId, Node])] =
        if (size <= 0)
          Gen.const(nodeIds.toImmArray -> nodes)
        else
          nodeGen(NodeId(parentNodeId.index * 10 + size)).flatMap { case (nodeId, children) =>
            nodesGen(parentNodeId, size - 1, nodeIds :+ nodeId, nodes ++ children)
          }

      Gen.choose(0, 6).flatMap(nodesGen(nodeId, _))
    }

    nonDanglingRefNodeGen(3, NodeId(0)).map { case (nodeIds, nodes) =>
      Transaction(nodes, nodeIds)
    }
  }

  val noDanglingRefGenVersionedTransaction: Gen[VersionedTransaction] = {
    for {
      tx <- noDanglingRefGenTransaction
      txVer <- transactionVersionGen()
      nodeVersionGen = transactionVersionGen().filterNot(_ < txVer)
      nodes <- tx.fold(Gen.const(HashMap.empty[NodeId, Node])) { case (acc, (nodeId, node)) =>
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
