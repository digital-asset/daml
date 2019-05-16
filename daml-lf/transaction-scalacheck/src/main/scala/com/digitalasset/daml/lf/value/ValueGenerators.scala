// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.value

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.transaction.Node.{
  KeyWithMaintainers,
  NodeCreate,
  NodeExercises,
  NodeFetch
}
import com.digitalasset.daml.lf.transaction.{Transaction => Tx}
import com.digitalasset.daml.lf.transaction._
import com.digitalasset.daml.lf.value.Value._
import org.scalacheck.{Arbitrary, Gen}
import Arbitrary.arbitrary
import scalaz.Equal
import scalaz.syntax.apply._
import scalaz.scalacheck.ScalaCheckBinding._
import scalaz.std.string.parseInt

object ValueGenerators {

  import Ref.LedgerString.{assertFromString => toContractId}

  /** In string encoding, assume prefix of RCOID::: or ACOID:::. */
  val defaultCidDecode: ValueCoder.DecodeCid[Tx.TContractId] = ValueCoder.DecodeCid(
    { i: String =>
      if (i.startsWith("RCOID")) {
        Right(RelativeContractId(Tx.NodeId.unsafeFromIndex(i.split(":::")(1).toInt)))
      } else if (i.startsWith("ACOID")) {
        Right(AbsoluteContractId(toContractId(i.split(":::")(1))))
      } else {
        Left(ValueCoder.DecodeError(s"Invalid contractId string $i"))
      }
    }, { (i, r) =>
      if (r)
        parseInt(i)
          .bimap(
            e => ValueCoder.DecodeError(e.getMessage),
            n => RelativeContractId(NodeId unsafeFromIndex n))
          .toEither
      else Right(AbsoluteContractId(toContractId(i)))
    }
  )

  val defaultValDecode
    : ValueOuterClass.VersionedValue => Either[ValueCoder.DecodeError, Tx.Value[Tx.TContractId]] =
    a => ValueCoder.decodeVersionedValue(defaultCidDecode, a)

  /** In string encoding, prefix with RCOID::: or ACOID:::. */
  val defaultCidEncode: ValueCoder.EncodeCid[Tx.TContractId] = ValueCoder.EncodeCid(
    {
      case AbsoluteContractId(coid) => s"ACOID:::${coid: String}"
      case RelativeContractId(i) => s"RCOID:::${i.index: Int}"
    }, {
      case AbsoluteContractId(coid) => (coid, false)
      case RelativeContractId(i) => ((i.index: Int).toString, true)
    }
  )

  val defaultValEncode: TransactionCoder.EncodeVal[Tx.Value[Tx.TContractId]] =
    a => ValueCoder.encodeVersionedValueWithCustomVersion(defaultCidEncode, a).map((a.version, _))

  val defaultNidDecode: String => Either[ValueCoder.DecodeError, NodeId] = s => {
    try {
      Right(NodeId.unsafeFromIndex(s.toInt))
    } catch {
      case _: NumberFormatException =>
        Left(ValueCoder.DecodeError(s"invalid node id, not an integer: $s"))
      case e: Throwable =>
        Left(ValueCoder.DecodeError(s"unexpected error during decoding nodeId: ${e.getMessage}"))
    }
  }

  val defaultNidEncode: TransactionCoder.EncodeNid[NodeId] = nid => nid.index.toString

  /** A whose equalIsNatural == false */
  private[lf] final case class Unnatural[+A](a: A)
  private[lf] object Unnatural {
    implicit def arbUA[A: Arbitrary]: Arbitrary[Unnatural[A]] =
      Arbitrary(Arbitrary.arbitrary[A] map (Unnatural(_)))
    implicit def eqUA[A: Equal]: Equal[Unnatural[A]] = Equal.equalBy(_.a)
  }

  //generate decimal values
  val decimalGen: Gen[ValueDecimal] = {
    val integerPart = Gen.listOfN(28, Gen.choose(1, 9)).map(_.mkString)
    val decimalPart = Gen.listOfN(10, Gen.choose(1, 9)).map(_.mkString)
    val bd = integerPart.flatMap(i => decimalPart.map(d => s"$i.$d")).map(BigDecimal(_))
    Gen
      .frequency(
        (1, Gen.const(BigDecimal("0.0"))),
        (1, Gen.const(Decimal.max)),
        (1, Gen.const(Decimal.min)),
        (5, bd)
      )
      .map(d => ValueDecimal(Decimal.assertFromBigDecimal(d)))
  }

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
  private def variantGen(nesting: Int): Gen[ValueVariant[VContractId]] =
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
  def variantGen: Gen[ValueVariant[VContractId]] = variantGen(0)

  private def recordGen(nesting: Int): Gen[ValueRecord[VContractId]] =
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
    } yield ValueRecord[VContractId](toOption(id), ImmArray(labelledValues))
  def recordGen: Gen[ValueRecord[VContractId]] = recordGen(0)

  private def valueOptionalGen(nesting: Int): Gen[ValueOptional[VContractId]] =
    Gen.option(valueGen(nesting)).map(v => ValueOptional(v))
  def valueOptionalGen: Gen[ValueOptional[VContractId]] = valueOptionalGen(0)

  private def valueListGen(nesting: Int): Gen[ValueList[VContractId]] =
    for {
      values <- Gen.listOf(Gen.lzy(valueGen(nesting)))
    } yield ValueList[VContractId](FrontStack(values))
  def valueListGen: Gen[ValueList[VContractId]] = valueListGen(0)

  private def valueMapGen(nesting: Int) =
    for {
      list <- Gen.listOf(for {
        k <- Gen.asciiPrintableStr; v <- Gen.lzy(valueGen(nesting))
      } yield k -> v)
    } yield ValueMap[VContractId](SortedLookupList(Map(list: _*)))
  def valueMapGen: Gen[ValueMap[VContractId]] = valueMapGen(0)

  def coidGen: Gen[VContractId] = {
    val genRel: Gen[VContractId] =
      Arbitrary.arbInt.arbitrary.map(i => RelativeContractId(Transaction.NodeId.unsafeFromIndex(i)))
    val genAbs: Gen[VContractId] =
      Gen.alphaStr.filter(_.nonEmpty).map(s => AbsoluteContractId(toContractId(s)))
    Gen.frequency((1, genRel), (3, genAbs))
  }

  def coidValueGen: Gen[ValueContractId[VContractId]] = {
    coidGen.map(ValueContractId(_))
  }

  private def valueGen(nesting: Int): Gen[Value[VContractId]] = {
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
        (sz + 1, Gen.alphaStr.map(x => ValueText(x))),
        (sz + 1, decimalGen),
        (sz + 1, Arbitrary.arbLong.arbitrary.map(ValueInt64)),
        (sz + 1, Gen.alphaStr.map(x => ValueText(x))),
        (sz + 1, timestampGen.map(ValueTimestamp)),
        (sz + 1, coidValueGen),
        (sz + 1, party.map(ValueParty)),
        (sz + 1, Gen.oneOf(true, false).map(ValueBool))
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

  def valueGen: Gen[Value[VContractId]] = valueGen(0)

  def versionedValueGen: Gen[VersionedValue[VContractId]] =
    for {
      version <- valueVersionGen
      value <- valueGen
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
    } yield NodeCreate(coid, coinst, None, signatories, stakeholders, key)
  }

  @deprecated("use malformedCreateNodeGen instead", since = "100.11.17")
  private[lf] def createNodeGen = malformedCreateNodeGen

  val fetchNodeGen: Gen[NodeFetch[VContractId]] = {
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
        .map(_.map(Transaction.NodeId.unsafeFromIndex))
        .map(ImmArray(_))
      exerciseResultValue <- versionedValueGen
    } yield
      NodeExercises(
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
        Some(exerciseResultValue)
      )
  }

  @deprecated("use danglingRefExerciseNodeGen instead", since = "100.11.17")
  private[lf] def exerciseNodeGen = danglingRefExerciseNodeGen

  /** Makes nodes with the problems listed under `malformedCreateNodeGen`, and
    * `malformedGenTransaction` should they be incorporated into a transaction.
    */
  val danglingRefGenNode: Gen[(NodeId, Transaction.Node)] = {
    for {
      id <- Arbitrary.arbInt.arbitrary.map(NodeId.unsafeFromIndex)
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
  val malformedGenTransaction: Gen[Transaction.Transaction] = {
    for {
      nodes <- Gen.listOf(danglingRefGenNode)
      roots <- Gen.listOf(Arbitrary.arbInt.arbitrary.map(NodeId.unsafeFromIndex))
    } yield GenTransaction(nodes.toMap, ImmArray(roots), Set.empty)
  }

  @deprecated("use malformedGenTransaction instead", since = "100.11.17")
  private[lf] def genTransaction = malformedGenTransaction

  val genBlindingInfo: Gen[BlindingInfo] = {
    val nodePartiesGen = Gen.mapOf(
      arbitrary[Int]
        .map(NodeId.unsafeFromIndex)
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

  def valueVersionGen: Gen[ValueVersion] = Gen.oneOf(ValueVersions.acceptedVersions.toSeq)

  def unsupportedValueVersionGen: Gen[ValueVersion] =
    stringVersionGen.map(ValueVersion).filter(x => !ValueVersions.acceptedVersions.contains(x))

  def transactionVersionGen: Gen[TransactionVersion] =
    Gen.oneOf(TransactionVersions.acceptedVersions.toSeq)

  def unsupportedTransactionVersionGen: Gen[TransactionVersion] =
    stringVersionGen
      .map(TransactionVersion)
      .filter(x => !TransactionVersions.acceptedVersions.contains(x))
}
