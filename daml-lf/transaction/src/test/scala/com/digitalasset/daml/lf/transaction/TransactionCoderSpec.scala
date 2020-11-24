// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package lf
package transaction

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{Identifier, Party}
import com.daml.lf.transaction.Node._
import com.daml.lf.transaction.{TransactionOuterClass => proto}
import com.daml.lf.value.Value.{ContractId, ContractInst, ValueParty}
import com.daml.lf.value.ValueCoder.{DecodeError, EncodeError}
import com.daml.lf.value.{Value, ValueCoder}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.compat._
import scala.Ordering.Implicits.infixOrderingOps
import scala.jdk.CollectionConverters._

class TransactionCoderSpec
    extends AnyWordSpec
    with Matchers
    with Inside
    with EitherAssertions
    with ScalaCheckPropertyChecks {

  import com.daml.lf.value.test.ValueGenerators._

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 1000, sizeRange = 10)

  import TransactionVersion.{V10, V11, VDev}

  private[this] val transactionVersions = Table("transaction version", V10, V11, VDev)

  "encode-decode" should {

    "do contractInstance" in {
      forAll(versionedContractInstanceGen)(coinst =>
        TransactionCoder.decodeVersionedContractInstance(
          ValueCoder.CidDecoder,
          TransactionCoder.encodeContractInstance(ValueCoder.CidEncoder, coinst).toOption.get,
        ) shouldBe Right(normalizeContract(coinst))
      )
    }

    "do NodeCreate" in {
      forAll(malformedCreateNodeGen, versionInIncreasingOrder()) {
        case (createNode, (nodeVersion, txVersion)) =>
          val versionedNode = createNode.updateVersion(nodeVersion)
          val Right(encodedNode) = TransactionCoder
            .encodeNode(
              TransactionCoder.NidEncoder,
              ValueCoder.CidEncoder,
              txVersion,
              NodeId(0),
              versionedNode,
            )

          TransactionCoder.decodeVersionedNode(
            TransactionCoder.NidDecoder,
            ValueCoder.CidDecoder,
            txVersion,
            encodedNode,
          ) shouldBe Right((NodeId(0), normalizeCreate(versionedNode)))

          Right(createNode.informeesOfNode) shouldEqual
            TransactionCoder
              .protoNodeInfo(txVersion, encodedNode)
              .map(_.informeesOfNode)
      }
    }

    "do NodeFetch" in {
      forAll(fetchNodeGen, versionInIncreasingOrder()) {
        case (fetchNode, (nodeVersion, txVersion)) =>
          val versionedNode = fetchNode.updateVersion(nodeVersion)
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
          TransactionCoder
            .decodeVersionedNode(
              TransactionCoder.NidDecoder,
              ValueCoder.CidDecoder,
              txVersion,
              encodedNode,
            ) shouldBe Right((NodeId(0), normalizeFetch(versionedNode)))
          Right(fetchNode.informeesOfNode) shouldEqual
            TransactionCoder
              .protoNodeInfo(txVersion, encodedNode)
              .map(_.informeesOfNode)
      }
    }

    "do NodeExercises" in {
      forAll(danglingRefExerciseNodeGen, versionInIncreasingOrder()) {
        case (exerciseNode, (nodeVersion, txVersion)) =>
          val normalizedNode = normalizeExe(exerciseNode.updateVersion(nodeVersion))
          val Right(encodedNode) =
            TransactionCoder
              .encodeNode(
                TransactionCoder.NidEncoder,
                ValueCoder.CidEncoder,
                txVersion,
                NodeId(0),
                normalizedNode,
              )
          TransactionCoder
            .decodeVersionedNode(
              TransactionCoder.NidDecoder,
              ValueCoder.CidDecoder,
              txVersion,
              encodedNode,
            ) shouldBe Right((NodeId(0), normalizedNode))

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
            minimalistNode(node.version)(node.updateVersion(node.version))
          ),
          tx.roots,
        )
        inside(
          TransactionCoder
            .encodeTransactionWithCustomVersion(
              TransactionCoder.NidEncoder,
              ValueCoder.CidEncoder,
              tx2,
            )
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
                )
            )
            decodedVersionedTx shouldBe tx2
        }
      }

    "succeed with encoding under later version if succeeded under earlier version" ignore {
      def overrideNodeVersions[Nid, Cid](tx: GenTransaction[Nid, Cid]) = {
        tx.copy(nodes =
          tx.nodes.transform((_, node) => node.updateVersion(TransactionVersion.minVersion))
        )
      }

      forAll(noDanglingRefGenTransaction, minSuccessful(50)) { tx =>
        forAll(transactionVersionGen(), transactionVersionGen(), minSuccessful(20)) {
          (txVer1, txVer2) =>
            import scalaz.std.tuple._
            import scalaz.syntax.bifunctor._
            whenever(txVer1 != txVer2) {
              val orderedVers @ (txvMin, txvMax) =
                if (txVer2 <= txVer1) (txVer2, txVer1) else (txVer1, txVer2)
              inside(
                orderedVers umap (txVer =>
                  TransactionCoder
                    .encodeTransactionWithCustomVersion(
                      TransactionCoder.NidEncoder,
                      ValueCoder.CidEncoder,
                      VersionedTransaction(txVer, versionNodes(txVer, tx.nodes), tx.roots),
                    ),
                )
              ) {
                case (Left(EncodeError(minMsg)), maxEnc) =>
                  // fuzzy sort of "failed because of the version override" test
                  minMsg should include(txvMin.protoValue)
                  maxEnc.left foreach (_.errorMessage should include(txvMax.protoValue))
                case (Right(encWithMin), Right(encWithMax)) =>
                  inside(
                    (encWithMin, encWithMax) umap (TransactionCoder
                      .decodeTransaction(
                        TransactionCoder.NidDecoder,
                        ValueCoder.CidDecoder,
                        _,
                      ))
                  ) { case (Right(decWithMin), Right(decWithMax)) =>
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

    "transactions decoding should fail when unsupported transaction version received" in
      forAll(noDanglingRefGenTransaction, minSuccessful(50)) { tx =>
        forAll(stringVersionGen, minSuccessful(20)) { badTxVer =>
          whenever(TransactionVersion.fromString(badTxVer).isLeft) {
            val encodedTxWithBadTxVer: proto.Transaction = assertRight(
              TransactionCoder
                .encodeTransactionWithCustomVersion(
                  TransactionCoder.NidEncoder,
                  ValueCoder.CidEncoder,
                  VersionedTransaction(
                    TransactionVersion.VDev,
                    tx.nodes.view.mapValues(_.updateVersion(TransactionVersion.VDev)).toMap,
                    tx.roots,
                  ),
                )
            ).toBuilder.setVersion(badTxVer).build()

            encodedTxWithBadTxVer.getVersion shouldEqual badTxVer

            TransactionCoder.decodeTransaction(
              TransactionCoder.NidDecoder,
              ValueCoder.CidDecoder,
              encodedTxWithBadTxVer,
            ) shouldEqual Left(
              DecodeError(s"Unsupported transaction version '$badTxVer'")
            )
          }
        }
      }

    "do tx with a lot of root nodes" in {
      val node =
        NodeCreate[ContractId](
          coid = absCid("#test-cid"),
          templateId = Identifier.assertFromString("pkg-id:Test:Name"),
          arg = ValueParty(Party.assertFromString("francesco")),
          agreementText = "agreement",
          optLocation = None,
          signatories = Set(Party.assertFromString("alice")),
          stakeholders = Set(Party.assertFromString("alice"), Party.assertFromString("bob")),
          key = None,
          version = TransactionVersion.minVersion,
        )

      forEvery(transactionVersions) { version =>
        val versionedNode = node.updateVersion(version)
        val roots = ImmArray.ImmArraySeq.range(0, 10000).map(NodeId(_)).toImmArray
        val nodes = roots.iterator.map(nid => nid -> versionedNode).toMap
        val tx = VersionedTransaction(
          version,
          nodes = nodes.view.mapValues(_.copy(version = version)).toMap,
          roots = roots,
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
              .toOption
              .get,
          )
          .toOption
          .get
        tx shouldEqual decoded
      }
    }
  }

  "encodeVersionedNode" should {

    "fail iff try to encode choice observers in version < 11" in {
      val normalize = minimalistNode(V10)

      forAll(danglingRefExerciseNodeGen, minSuccessful(10)) { node =>
        val shouldFail = node.choiceObservers.nonEmpty

        val normalized = normalize(node) match {
          case exe: NodeExercises[NodeId, ContractId] =>
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
              normalized.updateVersion(V10),
            )

          result.isLeft shouldBe shouldFail
        }
      }
    }

    "fail if try to encode a node in a version newer than the transaction" in {

      forAll(danglingRefGenNode, versionInStrictIncreasingOrder(), minSuccessful(10)) {
        case ((nodeId, node), (txVersion, nodeVersion)) =>
          val normalizedNode = minimalistNode(nodeVersion)(node)

          TransactionCoder
            .encodeNode(
              TransactionCoder.NidEncoder,
              ValueCoder.CidEncoder,
              txVersion,
              nodeId,
              normalizedNode.updateVersion(nodeVersion),
            ) shouldBe Symbol("left")
      }
    }

    def changeVersion(
        value: com.daml.lf.value.ValueOuterClass.VersionedValue,
        version: String,
    ) =
      value.toBuilder.setVersion(version).build()

    "fail if try to encode a create node containing value with version different from node" in {
      forAll(malformedCreateNodeGen, transactionVersionGen(), minSuccessful(5)) { (node, version) =>
        whenever(node.version != version) {
          val nodeVersion = node.version
          val encodeVersion = ValueCoder.encodeValueVersion(version)
          val Right(encodedNode) = TransactionCoder.encodeNode(
            TransactionCoder.NidEncoder,
            ValueCoder.CidEncoder,
            nodeVersion,
            NodeId(0),
            minimalistNode(nodeVersion)(node),
          )
          val encodedCreate = encodedNode.getCreate
          var cases = List(
            encodedCreate.toBuilder
              .setContractInstance(
                encodedCreate.getContractInstance.toBuilder.setValue(
                  changeVersion(encodedCreate.getContractInstance.getValue, encodeVersion)
                )
              )
              .build()
          )
          if (encodedCreate.hasKeyWithMaintainers)
            cases = encodedCreate.toBuilder
              .setKeyWithMaintainers(
                encodedCreate.getKeyWithMaintainers.toBuilder.setKeyVersioned(
                  changeVersion(encodedCreate.getKeyWithMaintainers.getKeyVersioned, encodeVersion)
                )
              )
              .build() :: cases
          cases.foreach(node =>
            TransactionCoder.decodeVersionedNode(
              TransactionCoder.NidDecoder,
              ValueCoder.CidDecoder,
              nodeVersion,
              encodedNode.toBuilder.setCreate(node).build(),
            ) shouldBe a[Left[_, _]]
          )
        }
      }
    }

    "fail if try to encode a fetch node containing value with version different from node" in {
      forAll(fetchNodeGen, transactionVersionGen(), minSuccessful(5)) { (node, version) =>
        whenever(node.version != version && node.key.isDefined) {
          val nodeVersion = node.version
          val encodeVersion = ValueCoder.encodeValueVersion(version)
          val Right(encodedNode) = TransactionCoder.encodeNode(
            TransactionCoder.NidEncoder,
            ValueCoder.CidEncoder,
            nodeVersion,
            NodeId(0),
            minimalistNode(nodeVersion)(node),
          )
          val encodedFetch = encodedNode.getFetch

          val testCase = encodedFetch.toBuilder
            .setKeyWithMaintainers(
              encodedFetch.getKeyWithMaintainers.toBuilder.setKeyVersioned(
                changeVersion(encodedFetch.getKeyWithMaintainers.getKeyVersioned, encodeVersion)
              )
            )
            .build()

          TransactionCoder.decodeVersionedNode(
            TransactionCoder.NidDecoder,
            ValueCoder.CidDecoder,
            nodeVersion,
            encodedNode.toBuilder.setFetch(testCase).build(),
          ) shouldBe a[Left[_, _]]
        }
      }
    }

    "fail if try to encode a lookup node containing value with version different from node" in {
      forAll(lookupNodeGen, transactionVersionGen(), minSuccessful(5)) { (node, version) =>
        whenever(node.version != version) {
          val nodeVersion = node.version
          val encodeVersion = ValueCoder.encodeValueVersion(version)
          val Right(encodedNode) = TransactionCoder.encodeNode(
            TransactionCoder.NidEncoder,
            ValueCoder.CidEncoder,
            nodeVersion,
            NodeId(0),
            minimalistNode(nodeVersion)(node),
          )
          val encodedLookup = encodedNode.getLookupByKey

          val testCase = encodedLookup.toBuilder
            .setKeyWithMaintainers(
              encodedLookup.getKeyWithMaintainers.toBuilder.setKeyVersioned(
                changeVersion(encodedLookup.getKeyWithMaintainers.getKeyVersioned, encodeVersion)
              )
            )
            .build()

          TransactionCoder.decodeVersionedNode(
            TransactionCoder.NidDecoder,
            ValueCoder.CidDecoder,
            nodeVersion,
            encodedNode.toBuilder.setLookupByKey(testCase).build(),
          ) shouldBe a[Left[_, _]]
        }
      }
    }

    "fail if try to encode a exercise node containing value with version different from node" in {

      forAll(
        danglingRefExerciseNodeGen,
        transactionVersionGen(maxVersion = TransactionVersion.minNoVersionValue),
        minSuccessful(5),
      ) { (exeNode, version) =>
        whenever(exeNode.version != version) {
          val nodeVersion = exeNode.version
          val encodeVersion = ValueCoder.encodeValueVersion(version)
          val Right(encodedNode) = TransactionCoder.encodeNode(
            TransactionCoder.NidEncoder,
            ValueCoder.CidEncoder,
            nodeVersion,
            NodeId(0),
            minimalistNode(nodeVersion)(exeNode),
          )
          val encodedExe = encodedNode.getExercise
          var cases = List(
            encodedExe.toBuilder
              .setArgVersioned(changeVersion(encodedExe.getArgVersioned, encodeVersion))
              .build(),
            encodedExe.toBuilder
              .setResultVersioned(changeVersion(encodedExe.getResultVersioned, encodeVersion))
              .build(),
          )
          if (encodedExe.hasKeyWithMaintainers)
            cases = encodedExe.toBuilder
              .setKeyWithMaintainers(
                encodedExe.getKeyWithMaintainers.toBuilder.setKeyVersioned(
                  changeVersion(encodedExe.getKeyWithMaintainers.getKeyVersioned, encodeVersion)
                )
              )
              .build() :: cases
          cases.foreach(node =>
            TransactionCoder.decodeVersionedNode(
              TransactionCoder.NidDecoder,
              ValueCoder.CidDecoder,
              nodeVersion,
              encodedNode.toBuilder.setExercise(node).build(),
            ) shouldBe a[Left[_, _]]
          )
        }
      }
    }

  }

  "decodeVersionedNode" should {

    """ignore field version if enclosing Transaction message is of version 10""" in {
      val normalize = minimalistNode(V10)

      forAll(danglingRefGenNode, Gen.asciiStr, minSuccessful(10)) { case ((nodeId, node), str) =>
        val normalizedNode = normalize(node.updateVersion(V10))

        val Right(encoded) = for {
          encoded <- TransactionCoder
            .encodeNode(
              TransactionCoder.NidEncoder,
              ValueCoder.CidEncoder,
              V10,
              nodeId,
              normalizedNode,
            )
        } yield encoded.toBuilder.setVersion(str).build()

        TransactionCoder.decodeVersionedNode(
          TransactionCoder.NidDecoder,
          ValueCoder.CidDecoder,
          V10,
          encoded,
        ) shouldBe Right(nodeId -> normalizedNode)
      }
    }

    "fail if try to decode a node in a version newer than the enclosing Transaction message version" in {

      val postV10versions = TransactionVersion.All.filter(_ > V10)

      forAll(danglingRefGenNode, versionInStrictIncreasingOrder(postV10versions)) {
        case ((nodeId, node), (txVersion, nodeVersion)) =>
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
          ) shouldBe Symbol("left")
      }
    }

    "ignore field observers in version < 11" in {
      val normalize = minimalistNode(V10)

      forAll(
        Arbitrary.arbInt.arbitrary,
        danglingRefExerciseNodeGen,
        Gen.listOf(Gen.asciiStr),
        minSuccessful(50),
      ) { (nodeIdx, node, strings) =>
        val nodeId = NodeId(nodeIdx)
        val normalizedNode = normalize(node).updateVersion(V10)

        val Right(encoded) =
          for {
            encoded <- TransactionCoder
              .encodeNode(
                TransactionCoder.NidEncoder,
                ValueCoder.CidEncoder,
                V10,
                nodeId,
                normalizedNode,
              )
          } yield encoded.toBuilder
            .setExercise(encoded.getExercise.toBuilder.addAllObservers(strings.asJava).build)
            .build()

        forEvery(transactionVersions) { txVersion =>
          val result = TransactionCoder.decodeVersionedNode(
            TransactionCoder.NidDecoder,
            ValueCoder.CidDecoder,
            txVersion,
            encoded,
          )

          result shouldBe Right(nodeId -> normalizedNode)
        }
      }
    }
  }

  def withoutExerciseResult[Nid, Cid](gn: GenNode[Nid, Cid]): GenNode[Nid, Cid] =
    gn match {
      case ne: NodeExercises[Nid, Cid] => ne copy (exerciseResult = None)
      case _ => gn
    }
  def withoutContractKeyInExercise[Nid, Cid](gn: GenNode[Nid, Cid]): GenNode[Nid, Cid] =
    gn match {
      case ne: NodeExercises[Nid, Cid] => ne copy (key = None)
      case _ => gn
    }
  def withoutMaintainersInExercise[Nid, Cid](gn: GenNode[Nid, Cid]): GenNode[Nid, Cid] =
    gn match {
      case ne: NodeExercises[Nid, Cid] =>
        ne copy (key = ne.key.map(_.copy(maintainers = Set.empty)))
      case _ => gn
    }

  // FIXME: https://github.com/digital-asset/daml/issues/7622
  // Fix the usage of this function in the test, once `byKey` is added to the serialization format.
  def withoutByKeyFlag[Nid, Cid](gn: GenNode[Nid, Cid]): GenNode[Nid, Cid] =
    gn match {
      case ne: NodeExercises[Nid, Cid] =>
        ne.copy(byKey = false)
      case fe: NodeFetch[Cid] =>
        fe.copy(byKey = false)
      case _ => gn
    }

  def withoutChoiceObservers[Nid, Cid](gn: GenNode[Nid, Cid]): GenNode[Nid, Cid] =
    gn match {
      case ne: NodeExercises[Nid, Cid] =>
        ne.copy(choiceObservers = Set.empty)
      case _ => gn
    }

  def hasChoiceObserves(tx: GenTransaction[_, _]): Boolean =
    tx.nodes.values.exists {
      case ne: NodeExercises[_, _] => ne.choiceObservers.nonEmpty
      case _ => false
    }

  def minimalistNode(
      txvMin: TransactionVersion
  ): Node.GenNode[NodeId, ContractId] => Node.GenNode[NodeId, ContractId] = {
    def condApply(
        before: TransactionVersion,
        f: GenNode[NodeId, ContractId] => GenNode[NodeId, ContractId],
    ): GenNode[NodeId, ContractId] => GenNode[NodeId, ContractId] = {
      if (txvMin < before) f else identity
    }

    condApply(TransactionVersion.minChoiceObservers, withoutChoiceObservers)
      .compose(withoutByKeyFlag[NodeId, ContractId])

  }

  def nodesWithout(
      nodes: Map[NodeId, GenNode[NodeId, ContractId]],
      f: GenNode[NodeId, ContractId] => GenNode[NodeId, ContractId],
  ) =
    nodes.transform((_, gn) => f(gn))

  def minimalistNodes(
      txvMin: TransactionVersion,
      nodes: Map[NodeId, GenNode[NodeId, ContractId]],
  ): Map[NodeId, GenNode[NodeId, ContractId]] =
    nodes.transform((_, gn) => minimalistNode(txvMin)(gn))

  // TODO https://github.com/digital-asset/daml/issues/7709
  // The following function should be usefull to test choice observers
  def minimalistTx(
      txvMin: TransactionVersion,
      nodes: Map[NodeId, GenNode[NodeId, ContractId]],
  ): Map[NodeId, GenNode[NodeId, ContractId]] = {

    def condApply(
        before: TransactionVersion,
        f: GenNode[NodeId, ContractId] => GenNode[NodeId, ContractId],
    ): GenNode[NodeId, ContractId] => GenNode[NodeId, ContractId] =
      if (txvMin < before) f else identity

    nodesWithout(
      nodes,
      condApply(TransactionVersion.minChoiceObservers, withoutChoiceObservers)
        .compose(withoutByKeyFlag[NodeId, ContractId]),
    )
  }

  def minimalistTx(
      txvMin: TransactionVersion,
      tx: GenTransaction[NodeId, ContractId],
  ) =
    tx.copy(nodes = minimalistNodes(txvMin, tx.nodes))

  private def absCid(s: String): ContractId =
    ContractId.assertFromString(s)

  private def versionNodes[Nid, Cid](
      version: TransactionVersion,
      nodes: Map[Nid, GenNode[Nid, Cid]],
  ): Map[Nid, GenNode[Nid, Cid]] =
    nodes.view.mapValues(_.updateVersion(version)).toMap

  private def versionInIncreasingOrder(
      versions: Seq[TransactionVersion] = TransactionVersion.All
  ): Gen[(TransactionVersion, TransactionVersion)] =
    for {
      v1 <- Gen.oneOf(versions)
      v2 <- Gen.oneOf(versions.filter(_ >= v1))
    } yield (v1, v2)

  private def versionInStrictIncreasingOrder(
      versions: Seq[TransactionVersion] = TransactionVersion.All
  ): Gen[(TransactionVersion, TransactionVersion)] =
    for {
      v1 <- Gen.oneOf(versions.dropRight(1))
      v2 <- Gen.oneOf(versions.filter(_ > v1))
    } yield (v1, v2)

  private def normalizeCreate(create: Node.NodeCreate[ContractId]): Node.NodeCreate[ContractId] = {
    create.copy(
      arg = normalize(create.arg, create.version),
      key = create.key.map(normalizeKey(_, create.version)),
    )
  }

  private def normalizeFetch(fetch: Node.NodeFetch[ContractId]) =
    fetch.copy(
      key = fetch.key.map(normalizeKey(_, fetch.version)),
      byKey = false,
    )

  private def normalizeExe[Nid](exe: Node.NodeExercises[Nid, ContractId]) =
    exe.copy(
      chosenValue = normalize(exe.chosenValue, exe.version),
      exerciseResult = exe.exerciseResult.map(normalize(_, exe.version)),
      choiceObservers =
        exe.choiceObservers.filter(_ => exe.version >= TransactionVersion.minChoiceObservers),
      key = exe.key.map(normalizeKey(_, exe.version)),
      byKey = false,
    )

  private def normalizeKey(
      key: KeyWithMaintainers[Value[ContractId]],
      version: TransactionVersion,
  ) = {
    key.copy(key = normalize(key.key, version))
  }

  private def normalizeContract(contract: ContractInst[Value.VersionedValue[Value.ContractId]]) = {
    contract.copy(arg = normalizeValue(contract.arg))
  }

  private def normalizeValue(versionedValue: Value.VersionedValue[Value.ContractId]) = {
    val Value.VersionedValue(version, value) = versionedValue
    Value.VersionedValue(version, normalize(value, version))
  }

  private def normalize(
      value0: Value[ContractId],
      version: TransactionVersion,
  ): Value[ContractId] = lf.value.test.ValueNormalizer.normalize(value0, version)

}
