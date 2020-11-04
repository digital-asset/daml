// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{Identifier, PackageId, Party, QualifiedName}
import com.daml.lf.transaction.Node.{GenNode, NodeCreate, NodeExercises, NodeFetch, VersionedNode}
import com.daml.lf.transaction.{Transaction => Tx, TransactionOuterClass => proto}
import com.daml.lf.value.Value.{ContractInst, ValueParty, VersionedValue}
import com.daml.lf.value.ValueCoder.{DecodeError, EncodeError}
import com.daml.lf.value.{Value, ValueCoder, ValueVersion, ValueVersions}
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Inside, Matchers, WordSpec}

class TransactionCoderSpec
    extends WordSpec
    with Matchers
    with Inside
    with EitherAssertions
    with PropertyChecks {

  import com.daml.lf.value.test.ValueGenerators._

  private[this] val defaultTransactionVersion = TransactionVersions.acceptedVersions.lastOption getOrElse sys
    .error("there are no allowed versions! impossible! but could it be?")

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 1000, sizeRange = 10)

  "encode-decode" should {
    "do contractInstance" in {
      forAll(contractInstanceGen) { coinst: ContractInst[Tx.Value[Value.ContractId]] =>
        Right(coinst) shouldEqual TransactionCoder.decodeContractInstance(
          ValueCoder.CidDecoder,
          TransactionCoder.encodeContractInstance(ValueCoder.CidEncoder, coinst).toOption.get,
        )
      }
    }

    "do NodeCreate" in {
      forAll(malformedCreateNodeGen, valueVersionGen()) {
        (node: NodeCreate[Value.ContractId, Tx.Value[Value.ContractId]], _: ValueVersion) =>
          val versionedNode = VersionedNode(defaultTransactionVersion, node)
          val encodedNode = TransactionCoder
            .encodeVersionedNode(
              TransactionCoder.NidEncoder,
              ValueCoder.CidEncoder,
              defaultTransactionVersion,
              NodeId(0),
              versionedNode,
            )
            .toOption
            .get
          Right((NodeId(0), versionedNode)) shouldEqual TransactionCoder.decodeVersionedNodes(
            TransactionCoder.NidDecoder,
            ValueCoder.CidDecoder,
            defaultTransactionVersion,
            encodedNode,
          )

          Right(node.informeesOfNode) shouldEqual
            TransactionCoder
              .protoNodeInfo(defaultTransactionVersion, encodedNode)
              .map(_.informeesOfNode)
      }
    }

    "do NodeFetch" in {
      forAll(fetchNodeGen, valueVersionGen()) {
        (node: NodeFetch.WithTxValue[Value.ContractId], _: ValueVersion) =>
          val versionedNode = VersionedNode(defaultTransactionVersion, withoutByKeyFlag(node))
          val encodedNode =
            TransactionCoder
              .encodeVersionedNode(
                TransactionCoder.NidEncoder,
                ValueCoder.CidEncoder,
                defaultTransactionVersion,
                NodeId(0),
                versionedNode,
              )
              .toOption
              .get
          Right((NodeId(0), versionedNode)) shouldEqual TransactionCoder
            .decodeVersionedNodes(
              TransactionCoder.NidDecoder,
              ValueCoder.CidDecoder,
              defaultTransactionVersion,
              encodedNode,
            )
          Right(node.informeesOfNode) shouldEqual
            TransactionCoder
              .protoNodeInfo(defaultTransactionVersion, encodedNode)
              .map(_.informeesOfNode)
      }
    }

    "do NodeExercises" in {
      forAll(danglingRefExerciseNodeGen) {
        node: NodeExercises[NodeId, Value.ContractId, Tx.Value[Value.ContractId]] =>
          val versionedNode = VersionedNode(defaultTransactionVersion, withoutByKeyFlag(node))
          val encodedNode =
            TransactionCoder
              .encodeVersionedNode(
                TransactionCoder.NidEncoder,
                ValueCoder.CidEncoder,
                defaultTransactionVersion,
                NodeId(0),
                versionedNode,
              )
              .toOption
              .get
          Right((NodeId(0), versionedNode)) shouldEqual TransactionCoder
            .decodeVersionedNodes(
              TransactionCoder.NidDecoder,
              ValueCoder.CidDecoder,
              defaultTransactionVersion,
              encodedNode,
            )

          Right(node.informeesOfNode) shouldEqual
            TransactionCoder
              .protoNodeInfo(defaultTransactionVersion, encodedNode)
              .map(_.informeesOfNode)
      }
    }

    "do transactions with version override" in
      forAll(noDanglingRefGenTransaction, minSuccessful(50)) { tx =>
        forAll(transactionVersionGen, minSuccessful(5)) { txVer =>
          inside(
            TransactionCoder
              .encodeTransactionWithCustomVersion(
                TransactionCoder.NidEncoder,
                ValueCoder.CidEncoder,
                VersionedTransaction(
                  txVer,
                  tx.nodes.transform((_, node) => VersionedNode(txVer, node)),
                  tx.roots),
              ),
          ) {
            case Left(EncodeError(msg)) =>
              // fuzzy sort of "failed because of the version override" test
              msg should include(txVer.toString)
            case Right(encodedTx) =>
              val decodedVersionedTx = assertRight(
                TransactionCoder
                  .decodeTransaction(
                    TransactionCoder.NidDecoder,
                    ValueCoder.CidDecoder,
                    encodedTx,
                  ),
              )
              decodedVersionedTx.transaction shouldBe minimalistTx(txVer, tx)
          }
        }
      }

    "succeed with encoding under later version if succeeded under earlier version" in
      forAll(noDanglingRefGenTransaction, minSuccessful(50)) { tx =>
        forAll(transactionVersionGen, transactionVersionGen, minSuccessful(20)) {
          (txVer1, txVer2) =>
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
                          TransactionCoder.NidEncoder,
                          ValueCoder.CidEncoder,
                          VersionedTransaction(txVer, versionNodes(txVer, tx.nodes), tx.roots),
                        ),
                ),
              ) {
                case (Left(EncodeError(minMsg)), maxEnc) =>
                  // fuzzy sort of "failed because of the version override" test
                  minMsg should include(txvMin.toString)
                  maxEnc.left foreach (_.errorMessage should include(txvMax.toString))
                case (Right(encWithMin), Right(encWithMax)) =>
                  inside(
                    (encWithMin, encWithMax) umap (TransactionCoder
                      .decodeTransaction(
                        TransactionCoder.NidDecoder,
                        ValueCoder.CidDecoder,
                        _,
                      )),
                  ) {
                    case (Right(decWithMin), Right(decWithMax)) =>
                      decWithMin.transaction shouldBe minimalistTx(txvMin, tx)
                      decWithMin.transaction shouldBe
                        minimalistTx(txvMin, decWithMax.transaction)
                  }
              }
            }
        }
      }

    "transactions decoding should fail when unsupported value version received" in
      forAll(noDanglingRefGenTransaction, minSuccessful(50)) { tx =>
        whenever(isTransactionWithAtLeastOneVersionedValue(tx)) {
          forAll(unsupportedValueVersionGen, minSuccessful(20)) { badValVer =>
            ValueVersions.acceptedVersions.contains(badValVer) shouldEqual false

            val txWithBadValVersion = changeAllValueVersions(tx, badValVer)
            val encodedTxWithBadValVersion: proto.Transaction = assertRight(
              TransactionCoder
                .encodeTransactionWithCustomVersion(
                  TransactionCoder.NidEncoder,
                  ValueCoder.CidEncoder,
                  VersionedTransaction(
                    defaultTransactionVersion,
                    versionNodes(defaultTransactionVersion, txWithBadValVersion.nodes),
                    txWithBadValVersion.roots),
                ),
            )

            TransactionCoder.decodeTransaction(
              TransactionCoder.NidDecoder,
              ValueCoder.CidDecoder,
              encodedTxWithBadValVersion,
            ) shouldEqual Left(DecodeError(s"Unsupported value version ${badValVer.protoValue}"))
          }
        }
      }

    "transactions decoding should fail when unsupported transaction version received" in
      forAll(noDanglingRefGenTransaction, minSuccessful(50)) { tx =>
        forAll(unsupportedTransactionVersionGen, minSuccessful(20)) {
          badTxVer: TransactionVersion =>
            TransactionVersions.acceptedVersions.contains(badTxVer) shouldEqual false

            val encodedTxWithBadTxVer: proto.Transaction = assertRight(
              TransactionCoder
                .encodeTransactionWithCustomVersion(
                  TransactionCoder.NidEncoder,
                  ValueCoder.CidEncoder,
                  VersionedTransaction(
                    badTxVer,
                    tx.nodes.mapValues(Node.VersionedNode(badTxVer, _)),
                    tx.roots),
                ),
            )

            encodedTxWithBadTxVer.getVersion shouldEqual badTxVer.protoValue

            TransactionCoder.decodeTransaction(
              TransactionCoder.NidDecoder,
              ValueCoder.CidDecoder,
              encodedTxWithBadTxVer,
            ) shouldEqual Left(
              DecodeError(s"Unsupported transaction version ${badTxVer.protoValue}"),
            )
        }
      }

    "do tx with a lot of root nodes" in {
      val node =
        Node.NodeCreate[Value.ContractId, Value.VersionedValue[Value.ContractId]](
          coid = absCid("#test-cid"),
          coinst = ContractInst(
            Identifier(
              PackageId.assertFromString("pkg-id"),
              QualifiedName.assertFromString("Test:Name"),
            ),
            VersionedValue(
              ValueVersions.acceptedVersions.last,
              ValueParty(Party.assertFromString("francesco")),
            ),
            "agreement",
          ),
          optLocation = None,
          signatories = Set(Party.assertFromString("alice")),
          stakeholders = Set(Party.assertFromString("alice"), Party.assertFromString("bob")),
          key = None,
        )

      val versions = Table("version", v10, vDev)

      forEvery(versions) { version =>
        val versionedNode = VersionedNode(version, node)
        val roots = ImmArray.ImmArraySeq.range(0, 10000).map(NodeId(_)).toImmArray
        val nodes = roots.iterator.map(nid => nid -> versionedNode).toMap
        val tx = VersionedTransaction(
          version,
          versionedNodes = nodes,
          roots = roots
        )

        val decoded = TransactionCoder
          .decodeTransaction(
            TransactionCoder.NidDecoder,
            ValueCoder.CidDecoder,
            TransactionCoder
              .encodeTransaction(
                TransactionCoder.NidEncoder,
                ValueCoder.CidEncoder,
                tx,
              )
              .right
              .get,
          )
          .right
          .get
        tx shouldEqual decoded
      }
    }
  }

  "encode" should {
    // FIXME: https://github.com/digital-asset/daml/issues/7709
    // replace all occurrences of "dev" by "11", once "11" is released
    "fail iff try to encode choice observers in version < dev" in {
      forAll(noDanglingRefGenTransaction, minSuccessful(50)) { tx =>
        val shouldFail = hasChoiceObserves(tx)

        val result = TransactionCoder
          .encodeTransaction(
            TransactionCoder.NidEncoder,
            ValueCoder.CidEncoder,
            VersionedTransaction(
              v10,
              minimalistNodes(vDev, tx.nodes).mapValues(VersionedNode(v10, _)),
              tx.roots
            )
          )

        result.isLeft shouldBe shouldFail
      }
    }
  }

  "decode" should {
    // FIXME: https://github.com/digital-asset/daml/issues/7709
    // replace all occurrences of "dev" by "11", once "11" is released
    "fail if try to decode choice observers in version < dev" in {
      forAll(noDanglingRefGenTransaction, minSuccessful(50)) { tx =>
        whenever(hasChoiceObserves(tx)) {

          val encoded = TransactionCoder.encodeTransaction(
            TransactionCoder.NidEncoder,
            ValueCoder.CidEncoder,
            VersionedTransaction(
              vDev,
              minimalistNodes(vDev, tx.nodes)
                .mapValues(VersionedNode(vDev, _)),
              tx.roots
            ),
          )

          TransactionCoder.decodeTransaction(
            TransactionCoder.NidDecoder,
            ValueCoder.CidDecoder,
            encoded.right.get.toBuilder.setVersion("10").build()
          ) shouldBe 'left
        }
      }
    }
  }

  private def isTransactionWithAtLeastOneVersionedValue(
      tx: GenTransaction.WithTxValue[NodeId, Value.ContractId],
  ): Boolean =
    tx.nodes.values
      .exists {
        case _: Node.NodeCreate[_, _] | _: Node.NodeExercises[_, _, _] |
            _: Node.NodeLookupByKey[_, _] =>
          true
        case f: Node.NodeFetch[_, _] => f.key.isDefined
      }

  private def changeAllValueVersions(
      tx: GenTransaction.WithTxValue[NodeId, Value.ContractId],
      ver: ValueVersion,
  ): GenTransaction.WithTxValue[NodeId, Value.ContractId] =
    tx.map3(identity, identity, _.copy(version = ver))

  def withoutExerciseResult[Nid, Cid, Val](gn: GenNode[Nid, Cid, Val]): GenNode[Nid, Cid, Val] =
    gn match {
      case ne: NodeExercises[Nid, Cid, Val] => ne copy (exerciseResult = None)
      case _ => gn
    }
  def withoutContractKeyInExercise[Nid, Cid, Val](
      gn: GenNode[Nid, Cid, Val],
  ): GenNode[Nid, Cid, Val] =
    gn match {
      case ne: NodeExercises[Nid, Cid, Val] => ne copy (key = None)
      case _ => gn
    }
  def withoutMaintainersInExercise[Nid, Cid, Val](
      gn: GenNode[Nid, Cid, Val],
  ): GenNode[Nid, Cid, Val] =
    gn match {
      case ne: NodeExercises[Nid, Cid, Val] =>
        ne copy (key = ne.key.map(_.copy(maintainers = Set.empty)))
      case _ => gn
    }

  // FIXME: https://github.com/digital-asset/daml/issues/7622
  // Fix the usage of this function in the test, once `byKey` is added to the serialization format.
  def withoutByKeyFlag[Nid, Cid](gn: GenNode.WithTxValue[Nid, Cid]): GenNode.WithTxValue[Nid, Cid] =
    gn match {
      case ne: NodeExercises.WithTxValue[Nid, Cid] =>
        ne.copy(byKey = false)
      case fe: NodeFetch.WithTxValue[Cid] =>
        fe.copy(byKey = false)
      case _ => gn
    }

  def withoutChoiceObservers[Nid, Cid, Val](gn: GenNode[Nid, Cid, Val]): GenNode[Nid, Cid, Val] =
    gn match {
      case ne: NodeExercises[Nid, Cid, Val] =>
        ne.copy(choiceObservers = Set.empty)
      case _ => gn
    }

  def hasChoiceObserves(tx: GenTransaction[_, _, _]): Boolean =
    tx.nodes.values.exists {
      case ne: NodeExercises[_, _, _] => ne.choiceObservers.nonEmpty
      case _ => false
    }

  def nodesWithout[Nid, Cid](
      nodes: Map[Nid, GenNode.WithTxValue[Nid, Cid]],
      f: GenNode.WithTxValue[Nid, Cid] => GenNode.WithTxValue[Nid, Cid],
  ) =
    nodes.transform((_, gn) => f(gn))

  def minimalistNodes[Nid, Cid](
      txvMin: TransactionVersion,
      nodes: Map[Nid, GenNode.WithTxValue[Nid, Cid]],
  ): Map[Nid, GenNode.WithTxValue[Nid, Cid]] = {
    import VersionTimeline.Implicits._

    def condApply(
        before: TransactionVersion,
        f: GenNode.WithTxValue[Nid, Cid] => GenNode.WithTxValue[Nid, Cid],
    ): GenNode.WithTxValue[Nid, Cid] => GenNode.WithTxValue[Nid, Cid] =
      if (txvMin precedes before) f else identity

    nodesWithout(
      nodes,
      condApply(TransactionVersions.minChoiceObservers, withoutChoiceObservers)
        .compose(withoutByKeyFlag[Nid, Cid]),
    )
  }

  // FIXME: https://github.com/digital-asset/daml/issues/7709
  // The following function should be usefull to test choice observers
  def minimalistTx[Nid, Cid, Val](
      txvMin: TransactionVersion,
      nodes: Map[Nid, GenNode.WithTxValue[Nid, Cid]],
  ): Map[Nid, GenNode.WithTxValue[Nid, Cid]] = {
    import VersionTimeline.Implicits._

    def condApply(
        before: TransactionVersion,
        f: GenNode.WithTxValue[Nid, Cid] => GenNode.WithTxValue[Nid, Cid],
    ): GenNode.WithTxValue[Nid, Cid] => GenNode.WithTxValue[Nid, Cid] =
      if (txvMin precedes before) f else identity

    nodesWithout(
      nodes,
      condApply(TransactionVersions.minChoiceObservers, withoutChoiceObservers)
        .compose(withoutByKeyFlag[Nid, Cid]),
    )
  }

  def minimalistTx[Nid, Cid](
      txvMin: TransactionVersion,
      tx: GenTransaction.WithTxValue[Nid, Cid]
  ) =
    tx.copy(nodes = minimalistNodes(txvMin, tx.nodes))

  private def absCid(s: String): Value.ContractId =
    Value.ContractId.assertFromString(s)

  private def versionNodes[Nid, Cid](
      version: TransactionVersion,
      nodes: Map[Nid, Node.GenNode[Nid, Cid, Tx.Value[Cid]]],
  ): Map[Nid, VersionedNode[Nid, Cid]] =
    nodes.mapValues(VersionedNode(version, _))

  private val vDev: TransactionVersion = TransactionVersion("dev")
  private val v10: TransactionVersion = TransactionVersion("10")

}
