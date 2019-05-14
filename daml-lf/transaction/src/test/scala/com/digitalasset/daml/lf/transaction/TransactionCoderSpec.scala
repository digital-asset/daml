// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction

import com.digitalasset.daml.lf.EitherAssertions
import com.digitalasset.daml.lf.data.{ImmArray}
import com.digitalasset.daml.lf.data.Ref.{Identifier, PackageId, Party, QualifiedName}
import com.digitalasset.daml.lf.transaction.Node.{GenNode, NodeCreate, NodeExercises, NodeFetch}
import com.digitalasset.daml.lf.transaction.{Transaction => Tx, TransactionOuterClass => proto}
import com.digitalasset.daml.lf.value.Value.{VContractId, ContractInst, ValueParty, VersionedValue}
import com.digitalasset.daml.lf.value.ValueCoder.{DecodeCid, DecodeError, EncodeCid, EncodeError}
import com.digitalasset.daml.lf.value.{ValueVersion, ValueVersions}
import com.digitalasset.daml.lf.transaction.TransactionVersions._
import com.digitalasset.daml.lf.transaction.VersionTimeline.Implicits._
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Inside, Matchers, WordSpec}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class TransactionCoderSpec
    extends WordSpec
    with Matchers
    with Inside
    with EitherAssertions
    with PropertyChecks {

  import com.digitalasset.daml.lf.value.ValueGenerators._

  private[this] val defaultTransactionVersion = TransactionVersions.acceptedVersions.lastOption getOrElse sys
    .error("there are no allowed versions! impossible! but could it be?")

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 1000, sizeRange = 10)

  "encode-decode" should {
    "do contractInstance" in {
      forAll(contractInstanceGen) { coinst: ContractInst[Tx.Value[Tx.ContractId]] =>
        Right(coinst) shouldEqual TransactionCoder.decodeContractInstance(
          defaultValDecode,
          TransactionCoder.encodeContractInstance(defaultValEncode, coinst).toOption.get)
      }
    }

    "do NodeCreate" in {
      forAll(malformedCreateNodeGen, valueVersionGen) {
        (node: NodeCreate[Tx.ContractId, Tx.Value[Tx.ContractId]], valVer: ValueVersion) =>
          Right((Tx.NodeId.unsafeFromIndex(0), node)) shouldEqual TransactionCoder.decodeNode(
            defaultNidDecode,
            defaultCidDecode,
            defaultValDecode,
            defaultTransactionVersion,
            TransactionCoder
              .encodeNode(
                defaultNidEncode,
                defaultCidEncode,
                defaultValEncode,
                defaultTransactionVersion,
                Tx.NodeId.unsafeFromIndex(0),
                node)
              .toOption
              .get
          )
      }
    }

    "do NodeFetch" in {
      forAll(fetchNodeGen, valueVersionGen) {
        (node: NodeFetch[VContractId], valVer: ValueVersion) =>
          Right((Tx.NodeId.unsafeFromIndex(0), node)) shouldEqual TransactionCoder.decodeNode(
            defaultNidDecode,
            defaultCidDecode,
            defaultValDecode,
            defaultTransactionVersion,
            TransactionCoder
              .encodeNode(
                defaultNidEncode,
                defaultCidEncode,
                defaultValEncode,
                defaultTransactionVersion,
                Tx.NodeId.unsafeFromIndex(0),
                node)
              .toOption
              .get
          )
      }
    }

    "do NodeExercises" in {
      forAll(danglingRefExerciseNodeGen) {
        node: NodeExercises[Tx.NodeId, Tx.ContractId, Tx.Value[Tx.ContractId]] =>
          Right((Tx.NodeId.unsafeFromIndex(0), node)) shouldEqual TransactionCoder.decodeNode(
            defaultNidDecode,
            defaultCidDecode,
            defaultValDecode,
            defaultTransactionVersion,
            TransactionCoder
              .encodeNode(
                defaultNidEncode,
                defaultCidEncode,
                defaultValEncode,
                defaultTransactionVersion,
                Tx.NodeId.unsafeFromIndex(0),
                node)
              .toOption
              .get
          )
      }
    }

    "do transactions with default versions" in {
      forAll(malformedGenTransaction) { t =>
        val encodedTx: proto.Transaction =
          assertRight(
            TransactionCoder
              .encodeTransaction(defaultNidEncode, defaultCidEncode, t))

        val decodedVersionedTx: VersionedTransaction[Tx.NodeId, Tx.ContractId] =
          assertRight(
            TransactionCoder
              .decodeVersionedTransaction(defaultNidDecode, defaultCidDecode, encodedTx))

        decodedVersionedTx.version shouldEqual TransactionVersions.assignVersion(t)
        decodedVersionedTx.transaction shouldEqual t
      }
    }

    "do transactions with version override" in forAll(
      malformedGenTransaction,
      transactionVersionGen) { (tx: Tx.Transaction, txVer: TransactionVersion) =>
      inside(
        TransactionCoder
          .encodeTransactionWithCustomVersion(
            defaultNidEncode,
            defaultCidEncode,
            VersionedTransaction(txVer, tx))) {
        case Left(EncodeError(msg)) =>
          // fuzzy sort of "failed because of the version override" test
          msg should include(txVer.toString)
        case Right(encodedTx) =>
          val decodedVersionedTx = assertRight(
            TransactionCoder
              .decodeVersionedTransaction(defaultNidDecode, defaultCidDecode, encodedTx))
          decodedVersionedTx.transaction shouldBe (if (txVer precedes minExerciseResult)
                                                     withoutExerciseResult(tx)
                                                   else tx)
      }
    }

    "succeed with encoding under later version if succeeded under earlier version" in forAll(
      malformedGenTransaction,
      transactionVersionGen,
      transactionVersionGen) { (tx, txVer1, txVer2) =>
      import VersionTimeline.Implicits._
      import scalaz.std.tuple._
      import scalaz.syntax.bifunctor._
      whenever(txVer1 != txVer2) {
        val orderedVers @ (txvMin, txvMax) =
          if (txVer2 precedes txVer1) (txVer2, txVer1) else (txVer1, txVer2)
        inside(
          orderedVers umap (
              txVer =>
                TransactionCoder
                  .encodeTransactionWithCustomVersion(
                    defaultNidEncode,
                    defaultCidEncode,
                    VersionedTransaction(txVer, tx)))) {
          case (Left(EncodeError(minMsg)), maxEnc) =>
            // fuzzy sort of "failed because of the version override" test
            minMsg should include(txvMin.toString)
            maxEnc.left foreach (_.errorMessage should include(txvMax.toString))
          case (Right(encWithMin), Right(encWithMax)) =>
            inside((encWithMin, encWithMax) umap (TransactionCoder
              .decodeVersionedTransaction(defaultNidDecode, defaultCidDecode, _))) {
              case (Right(decWithMin), Right(decWithMax)) =>
                decWithMin.transaction shouldBe (if (txvMin precedes minExerciseResult)
                                                   withoutExerciseResult(tx)
                                                 else tx)
                decWithMin.transaction shouldBe (if (txvMin precedes minExerciseResult)
                                                   withoutExerciseResult(decWithMax.transaction)
                                                 else decWithMax.transaction)
            }
        }
      }
    }

    "transactions decoding should fail when unsupported value version received" in forAll(
      malformedGenTransaction,
      unsupportedValueVersionGen) { (tx: Tx.Transaction, badValVer: ValueVersion) =>
      whenever(isTransactionWithAtLeastOneVersionedValue(tx)) {

        ValueVersions.acceptedVersions.contains(badValVer) shouldEqual false

        val txWithBadValVersion: Tx.Transaction = changeAllValueVersions(tx, badValVer)
        val encodedTxWithBadValVersion: proto.Transaction = assertRight(
          TransactionCoder
            .encodeTransactionWithCustomVersion(
              defaultNidEncode,
              defaultCidEncode,
              VersionedTransaction(defaultTransactionVersion, txWithBadValVersion)))

        TransactionCoder.decodeVersionedTransaction(
          defaultNidDecode,
          defaultCidDecode,
          encodedTxWithBadValVersion) shouldEqual Left(
          DecodeError(s"Unsupported value version ${badValVer.protoValue}"))
      }
    }

    "transactions decoding should fail when unsupported transaction version received" in forAll(
      malformedGenTransaction,
      unsupportedTransactionVersionGen) { (tx: Tx.Transaction, badTxVer: TransactionVersion) =>
      TransactionVersions.acceptedVersions.contains(badTxVer) shouldEqual false

      val encodedTxWithBadTxVer: proto.Transaction = assertRight(
        TransactionCoder
          .encodeTransactionWithCustomVersion(
            defaultNidEncode,
            defaultCidEncode,
            VersionedTransaction(badTxVer, tx)))

      encodedTxWithBadTxVer.getVersion shouldEqual badTxVer.protoValue

      TransactionCoder.decodeVersionedTransaction(
        defaultNidDecode,
        defaultCidDecode,
        encodedTxWithBadTxVer) shouldEqual Left(
        DecodeError(s"Unsupported transaction version ${badTxVer.protoValue}"))
    }

    "do transaction blinding" in {
      forAll(genBlindingInfo) { bi: BlindingInfo =>
        Right(bi) shouldEqual BlindingCoder.decode(
          BlindingCoder.encode(bi, defaultNidEncode),
          defaultNidDecode)
      }
    }

    "do tx with a lot of root nodes" in {
      val node: Node.NodeCreate[String, VersionedValue[String]] = Node.NodeCreate(
        "test-cid",
        ContractInst(
          Identifier(
            PackageId.assertFromString("pkg-id"),
            QualifiedName.assertFromString("Test:Name")),
          VersionedValue(
            ValueVersions.acceptedVersions.last,
            ValueParty(Party.assertFromString("francesco"))),
          ("agreement")
        ),
        None,
        Set(Party.assertFromString("alice")),
        Set(Party.assertFromString("alice"), Party.assertFromString("bob")),
        None,
      )
      val nodes = ImmArray((1 to 10000).map { nid =>
        (nid.toString, node)
      })
      val tx = GenTransaction(
        nodes = Map(nodes.toSeq: _*),
        roots = nodes.map(_._1),
        usedPackages = Set.empty
      )
      tx shouldEqual TransactionCoder
        .decodeVersionedTransaction(
          Right(_),
          DecodeCid(Right(_), { case (s, _) => Right(s) }),
          TransactionCoder
            .encodeTransaction(
              identity[String],
              EncodeCid(identity[String], (s: String) => (s, false)),
              tx,
            )
            .right
            .get
        )
        .right
        .get
        .transaction
    }
  }

  private def isTransactionWithAtLeastOneVersionedValue(tx: Tx.Transaction): Boolean =
    tx.nodes.values
      .exists {
        case _: Node.NodeCreate[_, _] | _: Node.NodeExercises[_, _, _] |
            _: Node.NodeLookupByKey[_, _] =>
          true
        case _: Node.NodeFetch[_] => false
      }

  private def changeAllValueVersions(tx: Tx.Transaction, ver: ValueVersion): Tx.Transaction =
    tx.mapContractIdAndValue(identity, _.copy(version = ver))

  def withoutExerciseResult[Nid, Cid, Val](gn: GenNode[Nid, Cid, Val]): GenNode[Nid, Cid, Val] =
    gn match {
      case ne: NodeExercises[Nid, Cid, Val] => ne copy (exerciseResult = None)
      case _ => gn
    }

  def withoutExerciseResult[Nid, Cid, Val](
      t: GenTransaction[Nid, Cid, Val]): GenTransaction[Nid, Cid, Val] =
    t copy (nodes = t.nodes transform ((_, gn) => withoutExerciseResult(gn)))

}
