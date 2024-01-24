// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package lf
package transaction

import com.daml.lf.crypto.Hash
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{PackageName, Party, Identifier}
import com.daml.lf.transaction.{TransactionOuterClass => proto}
import com.daml.lf.value.Value.ContractId
import com.daml.lf.value.ValueCoder.{EncodeError, DecodeError}
import com.daml.lf.value.{Value, ValueCoder}
import com.google.protobuf
import com.google.protobuf.{ByteString, Message}
import org.scalacheck.{Gen, Arbitrary}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import collection.immutable.TreeSet
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

  private[this] val transactionVersions =
    Table("transaction version", TransactionVersion.All: _*)

  "encode-decode" should {

    "do contractInstance" in {
      forAll(versionedContraactInstanceWithAgreement)(coinst =>
        TransactionCoder.decodeVersionedContractInstance(
          ValueCoder.CidDecoder,
          TransactionCoder
            .encodeContractInstance(ValueCoder.CidEncoder, coinst)
            .toOption
            .get,
        ) shouldBe Right(normalizeContract(coinst))
      )
    }

    "do Node.Create" in {
      forAll(malformedCreateNodeGen(), versionInIncreasingOrder()) {
        case (createNode, (nodeVersion, txVersion)) =>
          val versionedNode = normalizeCreate(createNode.updateVersion(nodeVersion))
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
          ) shouldBe Right((NodeId(0), versionedNode))
      }
    }

    "do Node.Fetch" in {
      forAll(fetchNodeGen, versionInIncreasingOrder()) {
        case (fetchNode, (nodeVersion, txVersion)) =>
          val versionedNode = normalizeFetch(fetchNode.updateVersion(nodeVersion))
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
            ) shouldBe Right((NodeId(0), versionedNode))
      }
    }

    "do Node.Exercise" in {
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
      }
    }

    "do Node.Rollback" in {
      forAll(danglingRefRollbackNodeGen) { node =>
        forEvery(transactionVersions) { txVersion =>
          val normalizedNode = normalizeNode(node)
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
        }
      }
    }

    "do transactions" in
      forAll(noDanglingRefGenVersionedTransaction, minSuccessful(50)) { tx =>
        val tx2 = VersionedTransaction(
          tx.version,
          tx.nodes.transform((_, node) => normalizeNode(node)),
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

    "transactions decoding should fail when unsupported transaction version received" in
      forAll(noDanglingRefGenTransaction, minSuccessful(30)) { tx =>
        forAll(stringVersionGen, minSuccessful(10)) { badTxVer =>
          whenever(TransactionVersion.fromString(badTxVer).isLeft) {
            val encodedTxWithBadTxVer: proto.Transaction = assertRight(
              TransactionCoder
                .encodeTransactionWithCustomVersion(
                  TransactionCoder.NidEncoder,
                  ValueCoder.CidEncoder,
                  VersionedTransaction(
                    TransactionVersion.VDev,
                    tx.nodes.view.mapValues(updateVersion(_, TransactionVersion.VDev)).toMap,
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
        Node.Create(
          coid = absCid("#test-cid"),
          packageName = None,
          templateId = Identifier.assertFromString("pkg-id:Test:Name"),
          arg = Value.ValueParty(Party.assertFromString("francesco")),
          agreementText = "agreement",
          signatories = Set(Party.assertFromString("alice")),
          stakeholders = Set(Party.assertFromString("alice"), Party.assertFromString("bob")),
          keyOpt = None,
          version = TransactionVersion.minVersion,
        )

      forEvery(transactionVersions) { version =>
        val versionedNode = node.updateVersion(version)
        val roots = ImmArray.ImmArraySeq.range(0, 10000).map(NodeId(_)).toImmArray
        val nodes = roots.iterator.map(nid => nid -> versionedNode).toMap
        val tx = VersionedTransaction(
          version,
          nodes = nodes.view.mapValues(updateVersion(_, version)).toMap,
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

    "fail if try to encode a node in a version newer than the transaction" in {

      forAll(danglingRefGenActionNode, versionInStrictIncreasingOrder(), minSuccessful(10)) {
        case ((nodeId, node), (txVersion, nodeVersion)) =>
          val normalizedNode = updateVersion(node, nodeVersion)

          TransactionCoder
            .encodeNode(
              TransactionCoder.NidEncoder,
              ValueCoder.CidEncoder,
              txVersion,
              nodeId,
              normalizedNode,
            ) shouldBe Symbol("left")
      }
    }

    def changeVersion(
        value: com.daml.lf.value.ValueOuterClass.VersionedValue,
        version: String,
    ) =
      value.toBuilder.setVersion(version).build()

    "fail if try to encode a fetch node containing value with version different from node" in {
      forAll(fetchNodeGen, transactionVersionGen(), minSuccessful(5)) { (node, version) =>
        whenever(node.version != version && node.keyOpt.isDefined) {
          val nodeVersion = node.version
          val encodeVersion = ValueCoder.encodeValueVersion(version)
          val Right(encodedNode) = TransactionCoder.encodeNode(
            TransactionCoder.NidEncoder,
            ValueCoder.CidEncoder,
            nodeVersion,
            NodeId(0),
            normalizeNode(node),
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
            normalizeNode(node),
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

    // TODO: https://github.com/digital-asset/daml/issues/17995
    //  enable the test
    "accept to encode ContractInstance with a packageName iff version >= 1.16" ignore {
      forAll(
        malformedCreateNodeGen(),
        pkgNameGen(TransactionVersion.minUpgrade),
        minSuccessful(5),
      ) { (create, pkgName) =>
        val wrongPackageName = pkgName.filter(_ => create.version < TransactionVersion.minUpgrade)

        val normalizedCreate =
          adjustStakeholders(normalizeCreate(create).copy(packageName = wrongPackageName))
        val instance = normalizedCreate.versionedCoinst.map(
          Value.ContractInstanceWithAgreement(_, normalizedCreate.agreementText)
        )
        TransactionCoder
          .encodeContractInstance(ValueCoder.CidEncoder, instance) shouldBe a[Left[_, _]]
      }
    }

    // TODO: https://github.com/digital-asset/daml/issues/17995
    //  enable the test
    "accept to encode FatContractInstance with a packageName iff version >= 1.16" ignore {
      forAll(
        malformedCreateNodeGen(),
        pkgNameGen(TransactionVersion.minUpgrade),
        timestampGen,
        bytesGen,
        minSuccessful(5),
      ) { (create, pkgName, time, salt) =>
        val wrongPackageName = pkgName.filter(_ => create.version < TransactionVersion.minUpgrade)

        val normalizedCreate =
          adjustStakeholders(normalizeCreate(create).copy(packageName = wrongPackageName))
        val instance = FatContractInstance.fromCreateNode(
          normalizedCreate,
          time,
          data.Bytes.fromByteString(salt),
        )
        TransactionCoder.encodeFatContractInstance(instance) shouldBe a[Left[_, _]]
      }
    }
  }

  "decodeNodeVersion" should {

    "succeed as expected when the node is encoded with a version older than the transaction version" in {

      val gen = for {
        ver <- versionInIncreasingOrder(TransactionVersion.All)
        (nodeVersion, txVersion) = ver
        node <- danglingRefGenActionNodeWithVersion(nodeVersion)
      } yield (ver, node)

      forAll(gen, minSuccessful(5)) { case ((nodeVersion, txVersion), (nodeId, node)) =>
        val normalizedNode = updateVersion(node, nodeVersion)

        val Right(encoded) = TransactionCoder
          .encodeNode(
            TransactionCoder.NidEncoder,
            ValueCoder.CidEncoder,
            nodeVersion,
            nodeId,
            normalizedNode,
          )

        TransactionCoder.decodeNodeVersion(txVersion, encoded) shouldBe Right(nodeVersion)
      }
    }

    "fail when the node is encoded with a version newer than the transaction version" in {

      forAll(
        danglingRefGenActionNode,
        versionInStrictIncreasingOrder(TransactionVersion.All),
        minSuccessful(5),
      ) { case ((nodeId, node), (v1, v2)) =>
        val normalizedNode = updateVersion(node, v2)

        val Right(encoded) = TransactionCoder
          .encodeNode(
            TransactionCoder.NidEncoder,
            ValueCoder.CidEncoder,
            v2,
            nodeId,
            normalizedNode,
          )

        TransactionCoder.decodeNodeVersion(v1, encoded) shouldBe a[Left[_, _]]
      }
    }

    "do Versioned" in {
      forAll(Gen.oneOf(TransactionVersion.All), bytesGen, minSuccessful(5)) { (version, bytes) =>
        val encoded = TransactionCoder.encodeVersioned(version, bytes)
        val Right(decoded) = TransactionCoder.decodeVersioned(encoded)
        decoded shouldBe Versioned(version, bytes)
      }
    }

    "reject versioned message with trailing data" in {
      forAll(
        Gen.oneOf(TransactionVersion.All),
        bytesGen,
        bytesGen.filterNot(_.isEmpty),
        minSuccessful(5),
      ) { (version, bytes1, bytes2) =>
        val encoded = TransactionCoder.encodeVersioned(version, bytes1)
        TransactionCoder.decodeVersioned(encoded concat bytes2) shouldBe a[Left[_, _]]
      }
    }

    "reject Versioned message with unknown fields" in {
      forAll(
        Gen.oneOf(TransactionVersion.All),
        bytesGen,
        Arbitrary.arbInt.arbitrary,
        bytesGen.filterNot(_.isEmpty),
        minSuccessful(5),
      ) { (version, payload, i, extraData) =>
        val encoded = TransactionCoder.encodeVersioned(version, payload)
        val proto = TransactionOuterClass.Versioned.parseFrom(encoded)
        val reencoded = addUnknownField(proto.toBuilder, i, extraData).toByteString
        assert(reencoded != encoded)
        inside(TransactionCoder.decodeVersioned(reencoded)) {
          case Left(DecodeError(errorMessage)) =>
            errorMessage should include("unexpected field(s)")
        }
      }
    }

    // TODO: https://github.com/digital-asset/daml/issues/17995
    //  enable the test
    "accept to encode action node with a packageName iff version >= 1.16" ignore {
      forAll(
        danglingRefGenActionNode,
        pkgNameGen(TransactionVersion.minUpgrade),
        minSuccessful(5),
      ) { case ((nodeId, node), pkgName) =>
        val wrongPackageName = pkgName.filter(_ => node.version < TransactionVersion.minUpgrade)
        val wronNode = updatePackageName(normalizeNode(node), wrongPackageName)

        TransactionCoder.encodeNode(
          TransactionCoder.NidEncoder,
          ValueCoder.CidEncoder,
          node.version,
          nodeId,
          wronNode,
        ) shouldBe a[Left[_, _]]
      }
    }

    "do ContractInstance" in {
      forAll(
        malformedCreateNodeGen(),
        minSuccessful(5),
      ) { (create) =>
        val normalizedCreate = adjustStakeholders(normalizeCreate(create))
        val instance = normalizedCreate.versionedCoinst.map(
          Value.ContractInstanceWithAgreement(_, normalizedCreate.agreementText)
        )
        val Right(encoded) =
          TransactionCoder.encodeContractInstance(ValueCoder.CidEncoder, instance)
        val Right(decoded) =
          TransactionCoder.decodeVersionedContractInstance(ValueCoder.CidDecoder, encoded)

        decoded shouldBe instance
      }
    }

    "do FatContractInstance" in {
      forAll(
        malformedCreateNodeGen(),
        timestampGen,
        bytesGen,
        minSuccessful(5),
      ) { (create, time, salt) =>
        val normalizedCreate = adjustStakeholders(normalizeCreate(create))
        val instance = FatContractInstance.fromCreateNode(
          normalizedCreate,
          time,
          data.Bytes.fromByteString(salt),
        )
        val Right(encoded) = TransactionCoder.encodeFatContractInstance(instance)
        val Right(decoded) = TransactionCoder.decodeFatContractInstance(encoded)

        decoded shouldBe instance
      }
    }

    def hackProto(
        instance: FatContractInstance,
        f: TransactionOuterClass.FatContractInstance.Builder => Message,
    ): ByteString = {
      val Right(encoded) = TransactionCoder.encodeFatContractInstance(instance)
      val Right(Versioned(v, bytes)) = TransactionCoder.decodeVersioned(encoded)
      val builder = TransactionOuterClass.FatContractInstance.parseFrom(bytes).toBuilder
      TransactionCoder.encodeVersioned(v, f(builder).toByteString)
    }

    // TODO: https://github.com/digital-asset/daml/issues/17995
    //  enable the test
    "accept to decode ContractInstance with packageName iff version >= 1.16" ignore {
      forAll(
        malformedCreateNodeGen(),
        pkgNameGen(TransactionVersion.minUpgrade),
        minSuccessful(5),
      ) { (create, pkgName) =>
        val wrongPackageName = pkgName.filter(_ => create.version < TransactionVersion.minUpgrade)

        val normalizedCreate = adjustStakeholders(normalizeCreate(create))
        val instance = normalizedCreate.versionedCoinst.map(
          Value.ContractInstanceWithAgreement(_, normalizedCreate.agreementText)
        )
        val Right(encoded) =
          TransactionCoder.encodeContractInstance(ValueCoder.CidEncoder, instance)
        val wrongProto = encoded.toBuilder.setPackageName(wrongPackageName.getOrElse("")).build()
        TransactionCoder.decodeContractInstance(ValueCoder.CidDecoder, wrongProto)
      }
    }

    // TODO: https://github.com/digital-asset/daml/issues/17995
    //  enable the test
    "accept to decode FatContractInstance with packageName iff version >= 1.16" ignore {
      forAll(
        malformedCreateNodeGen(),
        pkgNameGen(TransactionVersion.minUpgrade),
        timestampGen,
        bytesGen,
        minSuccessful(5),
      ) { (create, pkgName, time, salt) =>
        val wrongPackageName = pkgName.filter(_ => create.version < TransactionVersion.minUpgrade)

        val normalizedCreate = adjustStakeholders(normalizeCreate(create))
        val instance = FatContractInstance.fromCreateNode(
          normalizedCreate,
          time,
          data.Bytes.fromByteString(salt),
        )
        val bytes = hackProto(instance, _.setPackageName(wrongPackageName.getOrElse("")).build())
        TransactionCoder.decodeFatContractInstance(bytes) shouldBe a[Left[_, _]]
      }
    }

    "reject FatContractInstance with unknown fields" in {
      forAll(
        malformedCreateNodeGen(),
        timestampGen,
        bytesGen,
        Arbitrary.arbInt.arbitrary,
        bytesGen.filterNot(_.isEmpty),
        minSuccessful(5),
      ) { (create, time, salt, i, extraBytes) =>
        val normalizedCreate = adjustStakeholders(normalizeCreate(create))
        val instance = FatContractInstance.fromCreateNode(
          normalizedCreate,
          time,
          data.Bytes.fromByteString(salt),
        )
        val bytes = hackProto(instance, addUnknownField(_, i, extraBytes))
        inside(TransactionCoder.decodeFatContractInstance(bytes)) {
          case Left(DecodeError(errorMessage)) =>
            errorMessage should include("unexpected field(s)")
        }
      }
    }

    "reject FatContractInstance with key but empty maintainers" in {
      forAll(
        malformedCreateNodeGen(),
        timestampGen,
        bytesGen,
        minSuccessful(2),
      ) { (create, time, salt) =>
        forAll(
          keyWithMaintainersGen(create.templateId, create.version),
          minSuccessful(2),
        ) { key =>
          val normalizedCreate = adjustStakeholders(normalizeCreate(create))
          val instance = FatContractInstance.fromCreateNode(
            normalizedCreate,
            time,
            data.Bytes.fromByteString(salt),
          )
          val Right(protoKey) = TransactionCoder.encodeKeyWithMaintainers(create.version, key)
          val bytes = hackProto(
            instance,
            _.setContractKeyWithMaintainers(protoKey.toBuilder.clearMaintainers()).build(),
          )
          inside(TransactionCoder.decodeFatContractInstance(bytes)) {
            case Left(DecodeError(errorMessage)) =>
              errorMessage should include("key without maintainers")
          }
        }
      }
    }

    "reject FatContractInstance with empty signatories" in {
      forAll(
        malformedCreateNodeGen(),
        timestampGen,
        bytesGen,
        minSuccessful(3),
      ) { (create, time, salt) =>
        val normalizedCreate = adjustStakeholders(normalizeCreate(create))
        val instance = FatContractInstance.fromCreateNode(
          normalizedCreate,
          time,
          data.Bytes.fromByteString(salt),
        )
        val bytes =
          hackProto(
            instance,
            _.clearContractKeyWithMaintainers().clearNonMaintainerSignatories().build(),
          )
        inside(TransactionCoder.decodeFatContractInstance(bytes)) {
          case Left(DecodeError(errorMessage)) =>
            errorMessage should include(
              "maintainers or non_maintainer_signatories should be non empty"
            )
        }
      }
    }

    def hackKeyProto(
        version: TransactionVersion,
        key: GlobalKeyWithMaintainers,
        f: TransactionOuterClass.KeyWithMaintainers.Builder => TransactionOuterClass.KeyWithMaintainers.Builder,
    ): TransactionOuterClass.KeyWithMaintainers = {
      val Right(encoded) = TransactionCoder.encodeKeyWithMaintainers(version, key)
      f(encoded.toBuilder).build()
    }

    "reject FatContractInstance with nonMaintainerSignatories containing maintainers" in {
      forAll(
        party,
        malformedCreateNodeGen(),
        timestampGen,
        bytesGen,
        minSuccessful(2),
      ) { (party, create, time, salt) =>
        forAll(
          keyWithMaintainersGen(create.templateId, create.version),
          minSuccessful(2),
        ) { key =>
          val normalizedCreate = adjustStakeholders(normalizeCreate(create))
          val instance = FatContractInstance.fromCreateNode(
            normalizedCreate,
            time,
            data.Bytes.fromByteString(salt),
          )
          val nonMaintainerSignatories = (instance.nonMaintainerSignatories + party)
          val maintainers = TreeSet.from(key.maintainers + party)
          val protoKey = hackKeyProto(
            create.version,
            key,
            { builder =>
              builder.clearMaintainers()
              maintainers.foreach(builder.addMaintainers)
              builder
            },
          )

          val bytes = hackProto(
            instance,
            { builder =>
              builder.clearNonMaintainerSignatories()
              nonMaintainerSignatories.foreach(builder.addNonMaintainerSignatories)
              builder.setContractKeyWithMaintainers(protoKey)
              builder.build()
            },
          )
          inside(TransactionCoder.decodeFatContractInstance(bytes)) {
            case Left(DecodeError(errorMessage)) =>
              errorMessage should include("is declared as maintainer and nonMaintainerSignatory")
          }
        }
      }
    }

    "reject FatContractInstance with nonSignatoryStakeholders containing maintainers" in {
      forAll(
        party,
        malformedCreateNodeGen(),
        timestampGen,
        bytesGen,
        minSuccessful(2),
      ) { (party, create, time, salt) =>
        forAll(
          keyWithMaintainersGen(create.templateId, create.version),
          minSuccessful(2),
        ) { key =>
          val normalizedCreate = adjustStakeholders(normalizeCreate(create))
          val instance = FatContractInstance.fromCreateNode(
            normalizedCreate,
            time,
            data.Bytes.fromByteString(salt),
          )
          val maintainers = TreeSet.from(key.maintainers + party)
          val nonMaintainerSignatories =
            instance.nonMaintainerSignatories -- key.maintainers - party
          val nonSignatoryStakeholders = instance.nonSignatoryStakeholders + party
          val protoKey = hackKeyProto(
            create.version,
            key,
            { builder =>
              builder.clearMaintainers()
              maintainers.foreach(builder.addMaintainers)
              builder
            },
          )

          val bytes = hackProto(
            instance,
            { builder =>
              builder.setContractKeyWithMaintainers(protoKey)
              builder.clearNonMaintainerSignatories()
              nonMaintainerSignatories.foreach(builder.addNonMaintainerSignatories)
              builder.clearNonSignatoryStakeholders()
              nonSignatoryStakeholders.foreach(builder.addNonSignatoryStakeholders)
              builder.build()
            },
          )
          inside(TransactionCoder.decodeFatContractInstance(bytes)) {
            case Left(DecodeError(errorMessage)) =>
              errorMessage should include("is declared as signatory and nonSignatoryStakeholder")
          }
        }
      }
    }

    "reject FatContractInstance with nonSignatoryStakeholders containing nonMaintainerSignatories" in {
      forAll(
        party,
        malformedCreateNodeGen(),
        timestampGen,
        bytesGen,
        minSuccessful(4),
      ) { (party, create, time, salt) =>
        val normalizedCreate = adjustStakeholders(normalizeCreate(create))
        val instance = FatContractInstance.fromCreateNode(
          normalizedCreate,
          time,
          data.Bytes.fromByteString(salt),
        )
        val nonMaintainerSignatories = instance.nonMaintainerSignatories + party
        val nonSignatoryStakeholders = instance.nonSignatoryStakeholders + party

        val bytes = hackProto(
          instance,
          { builder =>
            builder.clearNonMaintainerSignatories()
            nonMaintainerSignatories.foreach(builder.addNonMaintainerSignatories)
            builder.clearNonSignatoryStakeholders()
            nonSignatoryStakeholders.foreach(builder.addNonSignatoryStakeholders)
            builder.build()
          },
        )
        inside(TransactionCoder.decodeFatContractInstance(bytes)) {
          case Left(DecodeError(errorMessage)) =>
            errorMessage should include("is declared as signatory and nonSignatoryStakeholder")

        }
      }
    }

  }

  "decodeVersionedNode" should {

    "fail if try to decode a node in a version newer than the enclosing Transaction message version" in {

      val gen = for {
        ver <- versionInStrictIncreasingOrder(TransactionVersion.All)
        (txVersion, nodeVersion) = ver
        node <- danglingRefGenActionNodeWithVersion(nodeVersion)
      } yield (ver, node)

      forAll(gen) { case ((txVersion, nodeVersion), (nodeId, node)) =>
        val normalizedNode = updateVersion(node, nodeVersion)

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

    // TODO: https://github.com/digital-asset/daml/issues/17995
    //  enable the test
    "accept to decode action node with packageName iff version >= 1.16" ignore {
      forAll(
        danglingRefGenActionNode,
        pkgNameGen(TransactionVersion.minUpgrade),
        minSuccessful(5),
      ) { case ((nodeId, node), pkgName) =>
        val wrongPackageName =
          pkgName.filter(_ => node.version < TransactionVersion.minUpgrade).getOrElse("")

        val normalizedNode = normalizeNode(node)
        val Right(encoded) = TransactionCoder.encodeNode(
          TransactionCoder.NidEncoder,
          ValueCoder.CidEncoder,
          node.version,
          nodeId,
          normalizedNode,
        )
        import proto.Node.NodeTypeCase._
        val wrongProto = encoded.getNodeTypeCase match {
          case CREATE =>
            encoded.toBuilder.setCreate(
              encoded.getCreate.toBuilder.setPackageName(wrongPackageName)
            )
          case FETCH =>
            encoded.toBuilder.setFetch(encoded.getFetch.toBuilder.setPackageName(wrongPackageName))
          case EXERCISE =>
            encoded.toBuilder.setExercise(
              encoded.getExercise.toBuilder.setPackageName(wrongPackageName)
            )
          case LOOKUP_BY_KEY =>
            encoded.toBuilder.setLookupByKey(
              encoded.getLookupByKey.toBuilder.setPackageName(wrongPackageName)
            )
          case _ =>
            fail()
        }
        val x = TransactionCoder.decodeVersionedNode(
          TransactionCoder.NidDecoder,
          ValueCoder.CidDecoder,
          node.version,
          wrongProto.build(),
        )
        x shouldBe a[Left[_, _]]
      }
    }
  }

  "toOrderPartySet" should {
    import com.google.protobuf.LazyStringArrayList
    import scala.util.Random.shuffle

    def toProto(strings: Seq[String]) = {
      val l = new LazyStringArrayList()
      strings.foreach(s => l.add(ByteString.copyFromUtf8(s)))
      l
    }

    "accept strictly order list of parties" in {
      forAll(Gen.listOf(party)) { parties =>
        val sortedParties = parties.sorted.distinct
        val proto = toProto(sortedParties)
        inside(TransactionCoder.toPartyTreeSet(proto)) { case Right(decoded: TreeSet[Party]) =>
          decoded shouldBe TreeSet.from(sortedParties)
        }
      }
    }

    "reject non sorted list of parties" in {
      forAll(party, Gen.nonEmptyListOf(party)) { (party0, parties0) =>
        val party = Iterator
          .iterate(party0)(p => Party.assertFromString("_" + p))
          .filterNot(parties0.contains)
          .next()
        val parties = party :: parties0
        val sortedParties = parties.sorted
        val nonSortedParties =
          Iterator.iterate(parties)(shuffle(_)).filterNot(_ == sortedParties).next()
        val proto = toProto(nonSortedParties)
        TransactionCoder.toPartyTreeSet(proto) shouldBe a[Left[_, _]]
      }
    }

    "reject non list with duplicate" in {
      forAll(party, Gen.listOf(party)) { (party, parties) =>
        val partiesWithDuplicate = (party :: party :: parties).sorted
        val proto = toProto(partiesWithDuplicate)
        TransactionCoder.toPartyTreeSet(proto) shouldBe a[Left[_, _]]
      }
    }
  }

  def withoutExerciseResult(gn: Node): Node =
    gn match {
      case ne: Node.Exercise => ne copy (exerciseResult = None)
      case _ => gn
    }
  def withoutContractKeyInExercise(gn: Node): Node =
    gn match {
      case ne: Node.Exercise => ne copy (keyOpt = None)
      case _ => gn
    }
  def withoutMaintainersInExercise(gn: Node): Node =
    gn match {
      case ne: Node.Exercise =>
        ne copy (keyOpt = ne.keyOpt.map(_.copy(maintainers = Set.empty)))
      case _ => gn
    }

  def withoutChoiceObservers(gn: Node): Node =
    gn match {
      case ne: Node.Exercise =>
        ne.copy(choiceObservers = Set.empty)
      case _ => gn
    }

  def hasChoiceObserves(tx: Transaction): Boolean =
    tx.nodes.values.exists {
      case ne: Node.Exercise => ne.choiceObservers.nonEmpty
      case _ => false
    }

  private def absCid(s: String): ContractId =
    ContractId.V1(Hash.hashPrivateKey(s))

  def versionNodes(
      version: TransactionVersion,
      nodes: Map[NodeId, Node],
  ): Map[NodeId, Node] =
    nodes.view.mapValues(updateVersion(_, version)).toMap

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

  private val bytesGen: Gen[ByteString] =
    Gen
      .listOf(Arbitrary.arbByte.arbitrary)
      .map(x => ByteString.copyFrom(x.toArray))

  private[this] def normalizeNode(node: Node) =
    node match {
      case rb: Node.Rollback => rb // nothing to normalize
      case exe: Node.Exercise => normalizeExe(exe)
      case fetch: Node.Fetch => normalizeFetch(fetch)
      case create: Node.Create => normalizeCreate(create)
      case lookup: Node.LookupByKey => lookup
    }

  private[this] def adjustStakeholders(create: Node.Create) = {
    val maintainers = create.keyOpt.fold(Set.empty[Party])(_.maintainers)
    val signatories = create.signatories | maintainers
    val stakeholders = create.stakeholders | signatories
    create.copy(
      signatories = signatories,
      stakeholders = stakeholders,
    )
  }

  private[this] val dummyPackageName = Some(PackageName.assertFromString("package-name"))

  private[this] def normalizeCreate(
      create: Node.Create
  ): Node.Create = {
    val node = create.packageName match {
      case Some(_) if create.version < TransactionVersion.minUpgrade =>
        create.copy(packageName = None)
      case None if create.version >= TransactionVersion.minUpgrade =>
        create.copy(packageName = dummyPackageName)
      case _ => create
    }
    node.copy(
      arg = normalize(create.arg, create.version),
      keyOpt = create.keyOpt.map(normalizeKey(_, create.version)),
    )
  }

  private[this] def normalizeFetch(fetch: Node.Fetch) = {
    val node = fetch.packageName match {
      case Some(_) if fetch.version < TransactionVersion.minUpgrade =>
        fetch.copy(packageName = None)
      case None if fetch.version >= TransactionVersion.minUpgrade =>
        fetch.copy(packageName = dummyPackageName)
      case _ => fetch
    }
    node.copy(
      keyOpt = fetch.keyOpt.map(normalizeKey(_, fetch.version))
    )
  }

  private[this] def normalizeExe(exe: Node.Exercise) = {
    val node = exe.packageName match {
      case Some(_) if exe.version < TransactionVersion.minUpgrade => exe.copy(packageName = None)
      case None if exe.version >= TransactionVersion.minUpgrade =>
        exe.copy(packageName = dummyPackageName)
      case _ => exe
    }
    node.copy(
      interfaceId =
        if (exe.version >= TransactionVersion.minInterfaces)
          exe.interfaceId
        else None,
      chosenValue = normalize(exe.chosenValue, exe.version),
      exerciseResult = exe.exerciseResult.map(normalize(_, exe.version)),
      choiceObservers = exe.choiceObservers,
      choiceAuthorizers =
        if (exe.version >= TransactionVersion.minChoiceAuthorizers) exe.choiceAuthorizers else None,
      keyOpt = exe.keyOpt.map(normalizeKey(_, exe.version)),
      byKey = exe.byKey,
    )
  }

  private[this] def normalizeKey(
      key: GlobalKeyWithMaintainers,
      version: TransactionVersion,
  ) =
    key.copy(globalKey =
      GlobalKey.assertBuild(
        key.globalKey.templateId,
        normalize(key.value, version),
        GlobalKey.isShared(key.globalKey),
      )
    )

  private[this] def normalizeContract(contract: Versioned[Value.ContractInstanceWithAgreement]) =
    contract.map(
      _.copy(contractInstance =
        contract.unversioned.contractInstance.copy(
          arg = normalize(contract.unversioned.contractInstance.arg, contract.version)
        )
      )
    )

  private[this] def normalize(
      value0: Value,
      version: TransactionVersion,
  ): Value = Util.assertNormalizeValue(value0, version)

  private def updateVersion(
      node: Node,
      version: TransactionVersion,
  ): Node = node match {
    case node: Node.Action => normalizeNode(node.updateVersion(version))
    case node: Node.Rollback => node
  }

  private def updatePackageName(node: Node, pkgName: Option[PackageName]): Node =
    node match {
      case node: Node.Create => node.copy(packageName = pkgName)
      case node: Node.Fetch => node.copy(packageName = pkgName)
      case node: Node.LookupByKey => node.copy(packageName = pkgName)
      case node: Node.Exercise => node.copy(packageName = pkgName)
      case node: Node.Rollback => node
    }

  def addUnknownField(
      builder: Message.Builder,
      i: Int,
      content: ByteString,
  ): Message = {
    require(!content.isEmpty)
    def norm(i: Int) = (i % 536870911).abs + 1 // valid proto field index are 1 to 536870911
    val knownFieldIndex = builder.getDescriptorForType.getFields.asScala.map(_.getNumber).toSet
    val j = Iterator.iterate(norm(i))(i => norm(i + 1)).filterNot(knownFieldIndex).next()
    val field = protobuf.UnknownFieldSet.Field.newBuilder().addLengthDelimited(content).build()
    val extraFields = protobuf.UnknownFieldSet.newBuilder().addField(j, field).build()
    builder.setUnknownFields(extraFields).build()
  }

}
