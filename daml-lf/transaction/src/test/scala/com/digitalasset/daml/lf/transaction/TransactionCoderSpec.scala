// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{Identifier, PackageId, Party, QualifiedName}
import com.daml.lf.transaction.Node._
import com.daml.lf.transaction.{Transaction => Tx, TransactionOuterClass => proto}
import com.daml.lf.value.Value.{ContractId, ContractInst, ValueParty, VersionedValue}
import com.daml.lf.value.ValueCoder.{DecodeError, EncodeError}
import com.daml.lf.value.test.ValueGenerators.transactionVersionGen
import com.daml.lf.value.{Value, ValueCoder, ValueVersion, ValueVersions}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.JavaConverters._

class TransactionCoderSpec
    extends AnyWordSpec
    with Matchers
    with Inside
    with EitherAssertions
    with ScalaCheckPropertyChecks {

  import VersionTimeline.Implicits._

  import com.daml.lf.value.test.ValueGenerators._

  private[this] val defaultTransactionVersion = TransactionVersions.acceptedVersions.lastOption getOrElse sys
    .error("there are no allowed versions! impossible! but could it be?")

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 1000, sizeRange = 10)

  private val vDev: TransactionVersion = TransactionVersion("dev")
  private val v10: TransactionVersion = TransactionVersion("10")

  private[this] val transactionVersions = Table("transaction version", v10, vDev)

  "encode-decode" should {

    "do contractInstance" in {
      forAll(contractInstanceGen)(
        coinst =>
          Right(coinst) shouldEqual TransactionCoder.decodeContractInstance(
            ValueCoder.CidDecoder,
            TransactionCoder.encodeContractInstance(ValueCoder.CidEncoder, coinst).toOption.get,
        ))
    }

    "do NodeCreate" in {
      forAll(malformedCreateNodeGen, transactionVersionGen, transactionVersionGen) {
        (createNode, version1, version2) =>
          val (nodeVersion, txVersion) = inIncreasingOrder(version1, version2)
          val versionedNode = createNode.updateVersion(nodeVersion)
          val Right(encodedNode) = TransactionCoder
            .encodeNode(
              TransactionCoder.NidEncoder,
              ValueCoder.CidEncoder,
              txVersion,
              NodeId(0),
              versionedNode,
            )

          Right((NodeId(0), versionedNode)) shouldEqual TransactionCoder.decodeVersionedNode(
            TransactionCoder.NidDecoder,
            ValueCoder.CidDecoder,
            txVersion,
            encodedNode,
          )

          Right(createNode.informeesOfNode) shouldEqual
            TransactionCoder
              .protoNodeInfo(txVersion, encodedNode)
              .map(_.informeesOfNode)
      }
    }

    "do NodeFetch" in {
      forAll(fetchNodeGen, transactionVersionGen, transactionVersionGen) {
        (fetchNode, version1, version2) =>
          val (nodeVersion, txVersion) = inIncreasingOrder(version1, version2)

          val versionedNode = withoutByKeyFlag(fetchNode).updateVersion(nodeVersion)
          val encodedNode =
            TransactionCoder
              .encodeNode(
                TransactionCoder.NidEncoder,
                ValueCoder.CidEncoder,
                txVersion,
                NodeId(0),
                versionedNode,
              )
              .toOption
              .get
          Right((NodeId(0), versionedNode)) shouldEqual TransactionCoder
            .decodeVersionedNode(
              TransactionCoder.NidDecoder,
              ValueCoder.CidDecoder,
              txVersion,
              encodedNode,
            )
          Right(fetchNode.informeesOfNode) shouldEqual
            TransactionCoder
              .protoNodeInfo(txVersion, encodedNode)
              .map(_.informeesOfNode)
      }
    }

    "do NodeExercises" in {
      forAll(danglingRefExerciseNodeGen, transactionVersionGen, transactionVersionGen) {
        (exerciseNode, version1, version2) =>
          val (nodeVersion, txVersion) = inIncreasingOrder(version1, version2)

          val normalizedNode = minimalistNode(nodeVersion)(exerciseNode)
          val versionedNode = normalizedNode.updateVersion(nodeVersion)
          val Right(encodedNode) =
            TransactionCoder
              .encodeNode(
                TransactionCoder.NidEncoder,
                ValueCoder.CidEncoder,
                txVersion,
                NodeId(0),
                versionedNode,
              )
          Right((NodeId(0), versionedNode)) shouldEqual TransactionCoder
            .decodeVersionedNode(
              TransactionCoder.NidDecoder,
              ValueCoder.CidDecoder,
              txVersion,
              encodedNode,
            )

          Right(normalizedNode.informeesOfNode) shouldEqual
            TransactionCoder
              .protoNodeInfo(txVersion, encodedNode)
              .map(_.informeesOfNode)
      }
    }

    "do transactions" in
      forAll(noDanglingRefGenVersionedTransaction, minSuccessful(50)) { tx =>
        val tx2 = VersionedTransaction(
          tx.version,
          tx.nodes.transform((_, node) =>
            minimalistNode(node.version)(node.updateVersion(node.version))),
          tx.roots,
        )
        inside(
          TransactionCoder
            .encodeTransactionWithCustomVersion(
              TransactionCoder.NidEncoder,
              ValueCoder.CidEncoder,
              tx2,
            ),
        ) {
          case Left(EncodeError(msg)) =>
            // fuzzy sort of "failed because of the version override" test
            msg should include(tx2.version.protoValue)
          case Right(encodedTx) =>
            val decodedVersionedTx = assertRight(
              TransactionCoder
                .decodeTransaction(
                  TransactionCoder.NidDecoder,
                  ValueCoder.CidDecoder,
                  encodedTx,
                ),
            )
            decodedVersionedTx shouldBe tx2
        }
      }

    "succeed with encoding under later version if succeeded under earlier version" in {
      def overrideNodeVersions[Nid, Cid](tx: GenTransaction.WithTxValue[Nid, Cid]) = {
        tx.copy(nodes = tx.nodes.transform((_, node) =>
          node.updateVersion(TransactionVersions.minVersion)))
      }

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
                      overrideNodeVersions(decWithMin.transaction) shouldBe
                        overrideNodeVersions(minimalistTx(txvMin, tx))
                      overrideNodeVersions(decWithMin.transaction) shouldBe
                        overrideNodeVersions(minimalistTx(txvMin, decWithMax.transaction))
                  }
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
                    tx.nodes.mapValues(_.updateVersion(badTxVer)),
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
        NodeCreate[ContractId, Value.VersionedValue[ContractId]](
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
          version = TransactionVersions.minVersion,
        )

      forEvery(transactionVersions) { version =>
        val versionedNode = node.updateVersion(version)
        val roots = ImmArray.ImmArraySeq.range(0, 10000).map(NodeId(_)).toImmArray
        val nodes = roots.iterator.map(nid => nid -> versionedNode).toMap
        val tx = VersionedTransaction(
          version,
          nodes = nodes.mapValues(_.copy(version = version)),
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

  "encodeVersionedNode" should {

    // FIXME: https://github.com/digital-asset/daml/issues/7709
    // replace all occurrences of "dev" by "11", once "11" is released
    "fail iff try to encode choice observers in version < dev" in {
      val normalize = minimalistNode(v10)

      forAll(danglingRefExerciseNodeGen, minSuccessful(10)) { node =>
        val shouldFail = node.choiceObservers.nonEmpty

        val normalized = normalize(node) match {
          case exe: NodeExercises.WithTxValue[NodeId, ContractId] =>
            exe.copy(choiceObservers = node.choiceObservers)
          case otherwise => otherwise
        }

        forEvery(transactionVersions) { txVersion =>
          val result = TransactionCoder
            .encodeNode(
              TransactionCoder.NidEncoder,
              ValueCoder.CidEncoder,
              txVersion,
              NodeId(0),
              normalized.updateVersion(v10),
            )

          result.isLeft shouldBe shouldFail
        }
      }
    }

    "fail if try to encode a node in a version newer than the transaction" in {

      forAll(danglingRefGenNode, transactionVersionGen, transactionVersionGen, minSuccessful(10)) {
        case ((nodeId, node), version1, version2) =>
          whenever(version1 != version2) {
            val (txVersion, nodeVersion) = inIncreasingOrder(version1, version2)

            val normalizedNode = minimalistNode(nodeVersion)(node)

            TransactionCoder
              .encodeNode(
                TransactionCoder.NidEncoder,
                ValueCoder.CidEncoder,
                txVersion,
                nodeId,
                normalizedNode.updateVersion(nodeVersion),
              ) shouldBe 'left
          }
      }
    }

  }

  "decodeVersionedNode" should {

    """ignore field version if enclosing Transaction message is of version 10""" in {
      val normalize = minimalistNode(v10)

      forAll(danglingRefGenNode, Gen.asciiStr, minSuccessful(10)) {
        case ((nodeId, node), str) =>
          val normalizedNode = normalize(node.updateVersion(v10))

          val Right(encoded) = for {
            encoded <- TransactionCoder
              .encodeNode(
                TransactionCoder.NidEncoder,
                ValueCoder.CidEncoder,
                v10,
                nodeId,
                normalizedNode
              )
          } yield encoded.toBuilder.setVersion(str).build()

          TransactionCoder.decodeVersionedNode(
            TransactionCoder.NidDecoder,
            ValueCoder.CidDecoder,
            v10,
            encoded,
          ) shouldBe Right(nodeId -> normalizedNode)
      }
    }

    // FIXME: https://github.com/digital-asset/daml/issues/7709
    // enable the test when we have more that one version newer than 10
    "fail if try to decode a node in a version newer than the enclosing Transaction message version" ignore {

      forAll(
        danglingRefGenNode,
        transactionVersionGen.filter(_ != v10),
        transactionVersionGen.filter(_ != v10),
        minSuccessful(10)) {
        case ((nodeId, node), version1, version2) =>
          whenever(version1 != version2) {
            val (txVersion, nodeVersion) = inIncreasingOrder(version1, version2)

            val normalizedNode = minimalistNode(nodeVersion)(node).updateVersion(nodeVersion)

            val Right(encoded) = TransactionCoder
              .encodeNode(
                TransactionCoder.NidEncoder,
                ValueCoder.CidEncoder,
                nodeVersion,
                nodeId,
                normalizedNode,
              )

            TransactionCoder.decodeVersionedNode(
              TransactionCoder.NidDecoder,
              ValueCoder.CidDecoder,
              txVersion,
              encoded,
            ) shouldBe 'left
          }
      }
    }

    // FIXME: https://github.com/digital-asset/daml/issues/7709
    // replace all occurrences of "dev" by "11", once "11" is released
    "ignore field observers in version < dev" in {
      val normalize = minimalistNode(v10)

      forAll(
        Arbitrary.arbInt.arbitrary,
        danglingRefExerciseNodeGen,
        Gen.listOf(Gen.asciiStr),
        minSuccessful(50),
      ) { (nodeIdx, node, strings) =>
        val nodeId = NodeId(nodeIdx)
        val normalizedNode = normalize(node).updateVersion(v10)

        val Right(encoded) =
          for {
            encoded <- TransactionCoder
              .encodeNode(
                TransactionCoder.NidEncoder,
                ValueCoder.CidEncoder,
                v10,
                nodeId,
                normalizedNode,
              )
          } yield
            encoded.toBuilder
              .setExercise(encoded.getExercise.toBuilder.addAllObservers(strings.asJava).build)
              .build()

        forEvery(transactionVersions) { txVersion =>
          val result = TransactionCoder.decodeVersionedNode(
            TransactionCoder.NidDecoder,
            ValueCoder.CidDecoder,
            txVersion,
            encoded
          )

          result shouldBe Right(nodeId -> normalizedNode)
        }
      }
    }
  }

  private def isTransactionWithAtLeastOneVersionedValue(
      tx: GenTransaction.WithTxValue[NodeId, ContractId],
  ): Boolean =
    tx.nodes.values
      .exists {
        case _: NodeCreate[_, _] | _: NodeExercises[_, _, _] | _: NodeLookupByKey[_, _] =>
          true
        case f: NodeFetch[_, _] => f.key.isDefined
      }

  private def changeAllValueVersions(
      tx: GenTransaction.WithTxValue[NodeId, ContractId],
      ver: ValueVersion,
  ): GenTransaction.WithTxValue[NodeId, ContractId] =
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

  def minimalistNode(txvMin: TransactionVersion)
    : Node.GenNode.WithTxValue[NodeId, ContractId] => Node.GenNode.WithTxValue[NodeId, ContractId] = {
    def condApply(
        before: TransactionVersion,
        f: GenNode.WithTxValue[NodeId, ContractId] => GenNode.WithTxValue[NodeId, ContractId],
    ): GenNode.WithTxValue[NodeId, ContractId] => GenNode.WithTxValue[NodeId, ContractId] = {
      if (txvMin precedes before) f else identity
    }

    condApply(TransactionVersions.minChoiceObservers, withoutChoiceObservers)
      .compose(withoutByKeyFlag[NodeId, ContractId])

  }

  def nodesWithout(
      nodes: Map[NodeId, GenNode.WithTxValue[NodeId, ContractId]],
      f: GenNode.WithTxValue[NodeId, ContractId] => GenNode.WithTxValue[NodeId, ContractId],
  ) =
    nodes.transform((_, gn) => f(gn))

  def minimalistNodes(
      txvMin: TransactionVersion,
      nodes: Map[NodeId, GenNode.WithTxValue[NodeId, ContractId]],
  ): Map[NodeId, GenNode.WithTxValue[NodeId, ContractId]] =
    nodes.transform((_, gn) => minimalistNode(txvMin)(gn))

  // FIXME: https://github.com/digital-asset/daml/issues/7709
  // The following function should be usefull to test choice observers
  def minimalistTx(
      txvMin: TransactionVersion,
      nodes: Map[NodeId, GenNode.WithTxValue[NodeId, ContractId]],
  ): Map[NodeId, GenNode.WithTxValue[NodeId, ContractId]] = {

    def condApply(
        before: TransactionVersion,
        f: GenNode.WithTxValue[NodeId, ContractId] => GenNode.WithTxValue[NodeId, ContractId],
    ): GenNode.WithTxValue[NodeId, ContractId] => GenNode.WithTxValue[NodeId, ContractId] =
      if (txvMin precedes before) f else identity

    nodesWithout(
      nodes,
      condApply(TransactionVersions.minChoiceObservers, withoutChoiceObservers)
        .compose(withoutByKeyFlag[NodeId, ContractId]),
    )
  }

  def minimalistTx(
      txvMin: TransactionVersion,
      tx: GenTransaction.WithTxValue[NodeId, ContractId]
  ) =
    tx.copy(nodes = minimalistNodes(txvMin, tx.nodes))

  private def absCid(s: String): ContractId =
    ContractId.assertFromString(s)

  private def versionNodes[Nid, Cid](
      version: TransactionVersion,
      nodes: Map[Nid, GenNode[Nid, Cid, Tx.Value[Cid]]],
  ): Map[Nid, GenNode.WithTxValue[Nid, Cid]] =
    nodes.mapValues(_.updateVersion(version))

  private[this] def inIncreasingOrder(version1: TransactionVersion, version2: TransactionVersion) =
    if (version1 precedes version2)
      (version1, version2)
    else
      (version2, version1)

}
