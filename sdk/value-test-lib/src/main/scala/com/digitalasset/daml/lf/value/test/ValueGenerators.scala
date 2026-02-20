// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value
package test

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.transaction.{
  GlobalKey,
  GlobalKeyWithMaintainers,
  Node,
  NodeId,
  Transaction,
  SerializationVersion,
  Versioned,
  VersionedTransaction,
}
import com.digitalasset.daml.lf.value.Value._
import org.scalacheck.{Arbitrary, Gen}
import Arbitrary.arbitrary
import com.google.protobuf.ByteString

import scala.Ordering.Implicits.infixOrderingOps
import scala.collection.immutable.HashMap
import scalaz.syntax.apply._
import scalaz.scalacheck.ScalaCheckBinding._

object ValueGenerators {

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

  val pkgNameGen: Gen[PackageName] =
    for {
      n <- Gen.choose(1, 64)
      pkgName <- Gen
        .listOfN(n, Gen.alphaNumChar)
        .map(s => PackageName.assertFromString(s.mkString))
    } yield pkgName

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

  def valueTextMapGen: Gen[ValueTextMap] =
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
  private val genSuffixesV1: Gen[Bytes] = for {
    sz <- Gen.chooseNum(0, ContractId.V1.MaxSuffixLength)
    ab <- Gen.containerOfN[Array, Byte](sz, arbitrary[Byte])
  } yield Bytes fromByteArray ab

  private val genSuffixesV2: Gen[Bytes] = for {
    sz <- Gen.chooseNum(0, ContractId.V2.MaxSuffixLength)
    ab <- Gen.containerOfN[Array, Byte](sz, arbitrary[Byte])
  } yield Bytes fromByteArray ab

  private val cidV1Gen: Gen[ContractId.V1] =
    Gen.zip(genHash, genSuffixesV1) map { case (h, b) =>
      ContractId.V1.assertBuild(h, b)
    }

  private val cidV2Gen: Gen[ContractId.V2] = Gen
    .zip(
      Gen
        .containerOfN[Array, Byte](ContractId.V2.localSize, arbitrary[Byte])
        .map(Bytes.fromByteArray),
      genSuffixesV2,
    )
    .map { case (local, suffix) =>
      ContractId.V2.assertBuild(local, suffix)
    }

  /** Universes of totally-ordered ContractIds. */
  def comparableCoidsGen: Seq[Gen[ContractId]] =
    Seq(globalAbsoluteCidGen, unsuffixedCidGen, relativeCidGen)

  private def unsuffixCid(cid: ContractId): ContractId = cid match {
    case cidV1: ContractId.V1 => ContractId.V1(cidV1.discriminator)
    case cidV2: ContractId.V2 => ContractId.V2(cidV2.local, Bytes.Empty)
  }
  private def globalAbsoluteCid(cid: ContractId, suffix: Bytes): ContractId = cid match {
    case cidV1: ContractId.V1 =>
      ContractId.V1.assertBuild(
        cidV1.discriminator,
        if (cidV1.suffix.isEmpty) suffix else cidV1.suffix,
      )
    case cidV2: ContractId.V2 =>
      val newSuffix = (if (cidV2.suffix.isEmpty) suffix else cidV2.suffix).toByteString
      // Ensure that the first bit is set
      val absoluteSuffix = ByteString
        .copyFrom(Array[Byte]((newSuffix.byteAt(0) | 0x80).toByte))
        .concat(newSuffix.substring(1))
      ContractId.V2.assertBuild(cidV2.local, Bytes.fromByteString(absoluteSuffix))
  }
  private def relativeCid(cid: ContractId.V2): ContractId.V2 =
    // Always use the same suffix as each suffix defines its own universe of comparable prefixes.
    ContractId.V2.assertBuild(cid.local, Bytes.assertFromString("00"))

  def unsuffixedCidGen: Gen[ContractId] = coidGen.map(unsuffixCid)
  def globalAbsoluteCidGen: Gen[ContractId] =
    Gen.zip(coidGen, arbitrary[Byte]).map { case (cid, suffixByte) =>
      globalAbsoluteCid(cid, Bytes.fromByteArray(Array(suffixByte)))
    }
  def relativeCidGen: Gen[ContractId] = cidV2Gen.map(relativeCid)

  def coidGen: Gen[ContractId] = Gen.oneOf(cidV1Gen, cidV2Gen)

  def coidValueGen: Gen[ValueContractId] =
    coidGen.map(ValueContractId(_))

  private def nestedGen = Gen.oneOf(
    valueListGen,
    variantGen,
    recordGen,
    valueOptionalGen,
    valueGenMapGen,
  )

  private def flatGen = Gen.oneOf(
    dateGen.map(ValueDate),
    Gen.alphaStr.map(ValueText),
    unscaledNumGen.map(ValueNumeric),
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
      version <- SerializationVersionGen()
    } yield Versioned(version, value)

  private[lf] val genMaybeEmptyParties: Gen[Set[Party]] = Gen.listOf(party).map(_.toSet)

  val genNonEmptyParties: Gen[Set[Party]] = ^(party, genMaybeEmptyParties)((hd, tl) => tl + hd)

  val versionedContractInstanceGen: Gen[Value.VersionedThinContractInstance] =
    for {
      template <- idGen
      arg <- versionedValueGen
      pkgName <- pkgNameGen
    } yield arg.map(Value.ThinContractInstance(pkgName, template, _))

  def keyWithMaintainersGen(
      templateId: TypeConId,
      packageName: PackageName,
  ): Gen[GlobalKeyWithMaintainers] = {
    for {
      key <- valueGen()
      maintainers <- genNonEmptyParties
      gkey = GlobalKey
        .build(templateId, packageName, key, crypto.Hash.hashPrivateKey(key.toString))
        .toOption
      if gkey.isDefined
    } yield GlobalKeyWithMaintainers(gkey.get, maintainers)
  }

  /** Makes create nodes that violate the rules:
    *
    * 1. stakeholders may not be a superset of signatories
    * 2. key's maintainers may not be a subset of signatories
    */
  def malformedCreateNodeGen(
      minVersion: SerializationVersion = SerializationVersion.minVersion
  ): Gen[Node.Create] = {
    for {
      version <- SerializationVersionGen(minVersion)
      node <- malformedCreateNodeGenWithVersion(version)
    } yield node
  }

  /** Makes create nodes with the given version that violate the rules:
    *
    * 1. stakeholders may not be a superset of signatories
    * 2. key's maintainers may not be a subset of signatories
    */
  def malformedCreateNodeGenWithVersion(
      version: SerializationVersion
  ): Gen[Node.Create] =
    for {
      coid <- coidGen
      packageName <- pkgNameGen
      templateId <- idGen
      arg <- valueGen()
      signatories <- genNonEmptyParties
      stakeholders <- genNonEmptyParties
      key <-
        if (version < SerializationVersion.minContractKeys)
          Gen.const(Option.empty[GlobalKeyWithMaintainers])
        else Gen.option(keyWithMaintainersGen(templateId, packageName))
    } yield Node.Create(
      coid = coid,
      packageName = packageName,
      templateId = templateId,
      arg = arg,
      signatories = signatories,
      stakeholders = stakeholders,
      keyOpt = key,
      version = version,
    )

  val fetchNodeGen: Gen[Node.Fetch] =
    for {
      version <- SerializationVersionGen()
      node <- fetchNodeGenWithVersion(version)
    } yield node

  def fetchNodeGenWithVersion(version: SerializationVersion): Gen[Node.Fetch] =
    for {
      coid <- coidGen
      pkgName <- pkgNameGen
      templateId <- idGen
      actingParties <- genNonEmptyParties
      signatories <- genNonEmptyParties
      stakeholders <- genNonEmptyParties
      key <-
        if (version < SerializationVersion.minContractKeys)
          Gen.const(Option.empty[GlobalKeyWithMaintainers])
        else Gen.option(keyWithMaintainersGen(templateId, pkgName))
      byKey <-
        if (version < SerializationVersion.minContractKeys) Gen.const(false)
        else Gen.oneOf(true, false)
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
      interfaceId = None,
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
      version <- SerializationVersionGen()
      node <- danglingRefExerciseNodeGenWithVersion(version)
    } yield node

  /** Makes exercise nodes with the given version and some random child IDs. */
  def danglingRefExerciseNodeGenWithVersion(
      version: SerializationVersion
  ): Gen[Node.Exercise] =
    for {
      targetCoid <- coidGen
      pkgName <- pkgNameGen
      templateId <- idGen
      interfaceId <- Gen.option(idGen)
      choiceId <- nameGen
      consume <- Gen.oneOf(true, false)
      actingParties <- genNonEmptyParties
      chosenValue <- valueGen()
      stakeholders <- genNonEmptyParties
      signatories <- genNonEmptyParties
      choiceObservers <- genMaybeEmptyParties
      choiceAuthorizersList <-
        if (version < SerializationVersion.minChoiceAuthorizers) Gen.const(Set.empty[Party])
        else genMaybeEmptyParties
      choiceAuthorizers = if (choiceAuthorizersList.isEmpty) None else Some(choiceAuthorizersList)
      children <- Gen
        .listOf(Arbitrary.arbInt.arbitrary)
        .map(_.map(NodeId(_)))
        .map(_.to(ImmArray))
      exerciseResult <- Gen.option(valueGen())
      key <-
        if (version < SerializationVersion.minContractKeys)
          Gen.const(Option.empty[GlobalKeyWithMaintainers])
        else Gen.option(keyWithMaintainersGen(templateId, pkgName))
      byKey <-
        if (version < SerializationVersion.minContractKeys) Gen.const(false)
        else Gen.oneOf(true, false)
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
      version <- SerializationVersionGen()
      targetCoid <- coidGen
      pkgName <- pkgNameGen
      templateId <- idGen
      key <- keyWithMaintainersGen(templateId, pkgName)
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
      version <- SerializationVersionGen()
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
    version <- SerializationVersionGen()
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
  def danglingRefGenActionNodeWithVersion(version: SerializationVersion): Gen[(NodeId, Node)] =
    refGenNode(
      Gen.oneOf(
        malformedCreateNodeGenWithVersion(version),
        danglingRefExerciseNodeGenWithVersion(version),
        fetchNodeGenWithVersion(version),
      )
    )

  def danglingRefGenNodeWithVersion(version: SerializationVersion): Gen[(NodeId, Node)] =
    Gen.frequency(
      3 -> danglingRefGenActionNodeWithVersion(version),
      1 -> refGenNode(danglingRefRollbackNodeGen),
    )

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
      txVer <- SerializationVersionGen()
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

  def SerializationVersionGen(
      minVersion: SerializationVersion = SerializationVersion.minVersion, // inclusive
      maxVersion: Option[SerializationVersion] = None, // exclusive if defined
  ): Gen[SerializationVersion] =
    Gen.oneOf(SerializationVersion.All.filter(v => minVersion <= v && maxVersion.forall(v < _)))

  object Implicits {
    implicit val vdateArb: Arbitrary[Time.Date] = Arbitrary(dateGen)
    implicit val vtimestampArb: Arbitrary[Time.Timestamp] = Arbitrary(timestampGen)
    implicit val vpartyArb: Arbitrary[Ref.Party] = Arbitrary(party)
    implicit val scaleArb: Arbitrary[Numeric.Scale] = Arbitrary(Gen.oneOf(Numeric.Scale.values))
  }
}
