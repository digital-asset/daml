// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.protocol.hash.TransactionHash.NodeHashingError
import com.digitalasset.canton.protocol.hash.TransactionHash.NodeHashingError.IncompleteTransactionTree
import com.digitalasset.daml.lf.data.*
import com.digitalasset.daml.lf.data.Ref.{ChoiceName, PackageName, Party}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.*
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml.lf.value.test.TypedValueGenerators.ValueAddend as VA
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class NodeHashV1Test extends BaseTest with AnyWordSpecLike with Matchers with HashUtilsTest {

  private val globalKey = GlobalKeyWithMaintainers(
    GlobalKey.assertBuild(
      defRef("module_key", "name"),
      VA.text.inj("hello"),
      PackageName.assertFromString("package_name_key"),
    ),
    Set[Party](Ref.Party.assertFromString("david")),
  )

  private val globalKey2 = GlobalKeyWithMaintainers(
    GlobalKey.assertBuild(
      defRef("module_key", "name"),
      VA.text.inj("bye"),
      PackageName.assertFromString("package_name_key"),
    ),
    Set[Party](Ref.Party.assertFromString("david")),
  )

  private val contractId1 = "0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5"
  private val contractId2 = "0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b"

  private val createNode = Node.Create(
    coid = ContractId.V1.assertFromString(contractId1),
    packageName = packageName0,
    templateId = defRef("module", "name"),
    arg = VA.text.inj("hello"),
    signatories =
      Set[Party](Ref.Party.assertFromString("alice"), Ref.Party.assertFromString("bob")),
    stakeholders =
      Set[Party](Ref.Party.assertFromString("alice"), Ref.Party.assertFromString("charlie")),
    keyOpt = None,
    version = LanguageVersion.v2_1,
  )

  private val createNodeEncoding = """'01' # 01 (Node Encoding Version)
                                     |# Create Node
                                     |# Node Version
                                     |'00000003' # 3 (int)
                                     |'322e31' # 2.1 (string)
                                     |'00' # Create Node Tag
                                     |# Node Seed
                                     |'01' # Some
                                     |'926bbb6f341bc0092ae65d06c6e284024907148cc29543ef6bff0930f5d52c19' # node seed
                                     |# Contract Id
                                     |'00000021' # 33 (int)
                                     |'0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5' # 0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5 (contractId)
                                     |# Package Name
                                     |'0000000e' # 14 (int)
                                     |'7061636b6167652d6e616d652d30' # package-name-0 (string)
                                     |# Template Id
                                     |'00000007' # 7 (int)
                                     |'7061636b616765' # package (string)
                                     |'00000001' # 1 (int)
                                     |'00000006' # 6 (int)
                                     |'6d6f64756c65' # module (string)
                                     |'00000001' # 1 (int)
                                     |'00000004' # 4 (int)
                                     |'6e616d65' # name (string)
                                     |# Arg
                                     |'07' # Text Type Tag
                                     |'00000005' # 5 (int)
                                     |'68656c6c6f' # hello (string)
                                     |# Signatories
                                     |'00000002' # 2 (int)
                                     |'00000005' # 5 (int)
                                     |'616c696365' # alice (string)
                                     |'00000003' # 3 (int)
                                     |'626f62' # bob (string)
                                     |# Stakeholders
                                     |'00000002' # 2 (int)
                                     |'00000005' # 5 (int)
                                     |'616c696365' # alice (string)
                                     |'00000007' # 7 (int)
                                     |'636861726c6965' # charlie (string)""".stripMargin

  private val createNodeHash = "6d2cfe58c2294000592034f4bdfe397fe246901bb8b63e3b9e041bb478e174b7"
  private val createNode2 = createNode.copy(
    coid = ContractId.V1.assertFromString(contractId2)
  )

  private val fetchNode = Node.Fetch(
    coid = ContractId.V1.assertFromString(contractId1),
    packageName = packageName0,
    templateId = defRef("module", "name"),
    actingParties =
      Set[Party](Ref.Party.assertFromString("alice"), Ref.Party.assertFromString("bob")),
    signatories = Set[Party](Ref.Party.assertFromString("alice")),
    stakeholders = Set[Party](Ref.Party.assertFromString("charlie")),
    keyOpt = None,
    byKey = false,
    interfaceId = None,
    version = LanguageVersion.v2_1,
  )

  private val fetchNodeEncoding = """'01' # 01 (Node Encoding Version)
                                    |# Fetch Node
                                    |# Node Version
                                    |'00000003' # 3 (int)
                                    |'322e31' # 2.1 (string)
                                    |'02' # Fetch Node Tag
                                    |# Contract Id
                                    |'00000021' # 33 (int)
                                    |'0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5' # 0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5 (contractId)
                                    |# Package Name
                                    |'0000000e' # 14 (int)
                                    |'7061636b6167652d6e616d652d30' # package-name-0 (string)
                                    |# Template Id
                                    |'00000007' # 7 (int)
                                    |'7061636b616765' # package (string)
                                    |'00000001' # 1 (int)
                                    |'00000006' # 6 (int)
                                    |'6d6f64756c65' # module (string)
                                    |'00000001' # 1 (int)
                                    |'00000004' # 4 (int)
                                    |'6e616d65' # name (string)
                                    |# Signatories
                                    |'00000001' # 1 (int)
                                    |'00000005' # 5 (int)
                                    |'616c696365' # alice (string)
                                    |# Stakeholders
                                    |'00000001' # 1 (int)
                                    |'00000007' # 7 (int)
                                    |'636861726c6965' # charlie (string)
                                    |# Interface Id
                                    |'00' # None
                                    |# Acting Parties
                                    |'00000002' # 2 (int)
                                    |'00000005' # 5 (int)
                                    |'616c696365' # alice (string)
                                    |'00000003' # 3 (int)
                                    |'626f62' # bob (string)""".stripMargin

  private val fetchNodeHash = "c962c6098394f3d11cd6f0c795de9517d32a8e3e1979cec76cd2f66254efc610"
  private val fetchNode2 = fetchNode.copy(
    coid = ContractId.V1.assertFromString(contractId2)
  )

  private val exerciseNode = Node.Exercise(
    targetCoid = ContractId.V1.assertFromString(
      "0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5"
    ),
    packageName = packageName0,
    templateId = defRef("module", "name"),
    interfaceId = Some(defRef("interface_module", "interface_name")),
    choiceId = ChoiceName.assertFromString("choice"),
    consuming = true,
    actingParties =
      Set[Party](Ref.Party.assertFromString("alice"), Ref.Party.assertFromString("bob")),
    chosenValue = VA.int64.inj(31380L),
    stakeholders = Set[Party](Ref.Party.assertFromString("charlie")),
    signatories = Set[Party](Ref.Party.assertFromString("alice")),
    choiceObservers = Set[Party](Ref.Party.assertFromString("david")),
    choiceAuthorizers = None,
    children = ImmArray(NodeId(0), NodeId(2)), // Create and Fetch
    exerciseResult = Some(VA.text.inj("result")),
    keyOpt = None,
    byKey = false,
    version = LanguageVersion.v2_1,
  )

  private val exerciseNodeHash = "070970eb4b2de72561dafb67017ca33850650a8103e5134e16044ba78991f48c"

  private val lookupNode = Node.LookupByKey(
    packageName = packageName0,
    templateId = defRef("module", "name"),
    key = globalKey,
    result = Some(
      ContractId.V1.assertFromString(contractId1)
    ),
    version = LanguageVersion.v2_1,
  )

  private val rollbackNode = Node.Rollback(
    children = ImmArray(NodeId(2), NodeId(4)) // Fetch2 and Exercise
  )

  private val rollbackNodeHash = "7264d5da2fd714427453bedc0d1cdb21f52ac7aec8d4bb5ac0598d25c5fcaed9"

  private val nodeSeedCreate =
    LfHash.assertFromString("926bbb6f341bc0092ae65d06c6e284024907148cc29543ef6bff0930f5d52c19")
  private val nodeSeedFetch =
    LfHash.assertFromString("4d2a522e9ee44e31b9bef2e3c8a07d43475db87463c6a13c4ea92f898ac8a930")
  private val nodeSeedExercise =
    LfHash.assertFromString("a867edafa1277f46f879ab92c373a15c2d75c5d86fec741705cee1eb01ef8c9e")
  private val nodeSeedRollback =
    LfHash.assertFromString("5483d5df9b245e662c0e4368b8062e8a0fd24c17ce4ded1a0e452e4ee879dd81")
  private val nodeSeedCreate2 =
    LfHash.assertFromString("e0c69eae8afb38872fa425c2cdba794176f3b9d97e8eefb7b0e7c831f566458f")
  private val nodeSeedFetch2 =
    LfHash.assertFromString("b4f0534e651ac8d5e10d95ddfcafdb123550a5e3185e3fe61ec1746a7222a88e")

  private val defaultNodeSeedsMap = Map(
    NodeId(0) -> nodeSeedCreate,
    NodeId(1) -> nodeSeedCreate2,
    NodeId(2) -> nodeSeedFetch,
    NodeId(3) -> nodeSeedFetch2,
    NodeId(4) -> nodeSeedExercise,
    NodeId(5) -> nodeSeedRollback,
  )

  private val subNodesMap = Map(
    NodeId(0) -> createNode,
    NodeId(1) -> createNode2,
    NodeId(2) -> fetchNode,
    NodeId(3) -> fetchNode2,
    NodeId(4) -> exerciseNode,
    NodeId(5) -> rollbackNode,
  )

  private def hashNodeV1(
      node: Node,
      nodeSeed: Option[LfHash],
      subNodes: Map[NodeId, Node] = subNodesMap,
      hashTracer: HashTracer,
  ) = TransactionHash.tryHashNodeV1(
    node,
    defaultNodeSeedsMap,
    nodeSeed,
    subNodes,
    hashTracer,
  )

  private def shiftNodeIds(array: ImmArray[NodeId]): ImmArray[NodeId] = array.map {
    case NodeId(i) => NodeId(i + 1)
  }

  private def shiftNodeIdsSeeds(map: Map[NodeId, LfHash]): Map[NodeId, LfHash] = map.map {
    case (NodeId(i), value) => NodeId(i + 1) -> value
  }

  private def shiftNodeIds(map: Map[NodeId, Node]): Map[NodeId, Node] = map.map {
    case (NodeId(i), exercise: Node.Exercise) =>
      NodeId(i + 1) -> exercise.copy(children = shiftNodeIds(exercise.children))
    case (NodeId(i), rollback: Node.Rollback) =>
      NodeId(i + 1) -> rollback.copy(children = shiftNodeIds(rollback.children))
    case (NodeId(i), node) => NodeId(i + 1) -> node
  }

  "V1Encoding" should {
    "not encode lookup nodes" in {
      a[NodeHashingError.UnsupportedFeature] shouldBe thrownBy {
        TransactionHash.tryHashNodeV1(lookupNode)
      }
    }

    "not encode create nodes without node seed" in {
      a[NodeHashingError.MissingNodeSeed] shouldBe thrownBy {
        TransactionHash.tryHashNodeV1(createNode, enforceNodeSeedForCreateNodes = true)
      }
    }

    "not encode exercise nodes without node seed" in {
      a[NodeHashingError.MissingNodeSeed] shouldBe thrownBy {
        TransactionHash.tryHashNodeV1(exerciseNode, enforceNodeSeedForCreateNodes = true)
      }
    }

    "encode create nodes without node seed if explicitly allowed" in {
      scala.util
        .Try(TransactionHash.tryHashNodeV1(createNode, enforceNodeSeedForCreateNodes = false))
        .isSuccess shouldBe true
    }
  }

  "CreateNodeBuilder V1" should {
    val defaultHash = Hash
      .fromHexStringRaw(createNodeHash)
      .getOrElse(fail("Invalid hash"))

    def hashCreateNode(node: Node.Create, hashTracer: HashTracer = HashTracer.NoOp) =
      hashNodeV1(node, Some(nodeSeedCreate), hashTracer = hashTracer)

    "be stable" in {
      hashCreateNode(createNode).toHexString shouldBe defaultHash.toHexString
    }

    "fails global keys" in {
      a[NodeHashingError.UnsupportedFeature] shouldBe thrownBy(
        hashCreateNode(
          createNode.copy(keyOpt = Some(globalKey2))
        )
      )
    }

    "not produce collision in contractId" in {
      hashCreateNode(
        createNode.copy(
          coid = ContractId.V1.assertFromString(
            "0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b"
          )
        )
      ) should !==(defaultHash)
    }

    "not produce collision in package name" in {
      hashCreateNode(
        createNode.copy(
          packageName = PackageName.assertFromString("another_package_name")
        )
      ) should !==(defaultHash)
    }

    "not produce collision in template ID" in {
      hashCreateNode(
        createNode.copy(
          templateId = defRef("othermodule", "othername")
        )
      ) should !==(defaultHash)
    }

    "not produce collision in arg" in {
      hashCreateNode(
        createNode.copy(
          arg = VA.bool.inj(true)
        )
      ) should !==(defaultHash)
    }

    "not produce collision in signatories" in {
      hashCreateNode(
        createNode.copy(
          signatories = Set[Party](Ref.Party.assertFromString("alice"))
        )
      ) should !==(defaultHash)
    }

    "not produce collision in stakeholders" in {
      hashCreateNode(
        createNode.copy(
          stakeholders = Set[Party](Ref.Party.assertFromString("alice"))
        )
      ) should !==(defaultHash)
    }

    "explain encoding" in {
      val hashTracer = HashTracer.StringHashTracer()
      val hash = hashCreateNode(createNode, hashTracer = hashTracer)
      hash shouldBe defaultHash
      hashTracer.result shouldBe
        s"""$createNodeEncoding
             |""".stripMargin
      assertStringTracer(hashTracer, hash)
    }
  }

  "FetchNodeBuilder V1" should {
    val defaultHash = Hash
      .fromHexStringRaw(fetchNodeHash)
      .getOrElse(fail("Invalid hash"))

    def hashFetchNode(node: Node.Fetch, hashTracer: HashTracer = HashTracer.NoOp) =
      hashNodeV1(node, nodeSeed = Some(nodeSeedFetch), hashTracer = hashTracer)

    "be stable" in {
      hashFetchNode(fetchNode).toHexString shouldBe defaultHash.toHexString
    }

    "fail if node includes global keys" in {
      a[NodeHashingError.UnsupportedFeature] shouldBe thrownBy(
        hashFetchNode(fetchNode.copy(keyOpt = Some(globalKey2)))
      )
    }

    "fail if node includes byKey" in {
      a[NodeHashingError.UnsupportedFeature] shouldBe thrownBy(
        hashFetchNode(fetchNode.copy(byKey = true))
      )
    }

    "not produce collision in contractId" in {
      hashFetchNode(
        fetchNode.copy(
          coid = ContractId.V1.assertFromString(
            "0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b"
          )
        )
      ) should !==(defaultHash)
    }

    "not produce collision in package name" in {
      hashFetchNode(
        fetchNode.copy(
          packageName = PackageName.assertFromString("another_package_name")
        )
      ) should !==(defaultHash)
    }

    "not produce collision in template ID" in {
      hashFetchNode(
        fetchNode.copy(
          templateId = defRef("othermodule", "othername")
        )
      ) should !==(defaultHash)
    }

    "not produce collision in actingParties" in {
      hashFetchNode(
        fetchNode.copy(
          actingParties = Set[Party](Ref.Party.assertFromString("charlie"))
        )
      ) should !==(defaultHash)
    }

    "not produce collision in signatories" in {
      hashFetchNode(
        fetchNode.copy(
          signatories = Set[Party](Ref.Party.assertFromString("bob"))
        )
      ) should !==(defaultHash)
    }

    "not produce collision in stakeholders" in {
      hashFetchNode(
        fetchNode.copy(
          stakeholders = Set[Party](Ref.Party.assertFromString("alice"))
        )
      ) should !==(defaultHash)
    }

    "explain encoding" in {
      val hashTracer = HashTracer.StringHashTracer()
      val hash = hashFetchNode(fetchNode, hashTracer = hashTracer)
      hash shouldBe defaultHash
      hashTracer.result shouldBe s"""$fetchNodeEncoding
                                    |""".stripMargin

      assertStringTracer(hashTracer, hash)
    }
  }

  "ExerciseNodeBuilder V1" should {
    val defaultHash = Hash
      .fromHexStringRaw(exerciseNodeHash)
      .getOrElse(fail("Invalid hash"))

    def hashExerciseNode(
        node: Node.Exercise,
        subNodes: Map[NodeId, Node] = subNodesMap,
        hashTracer: HashTracer = HashTracer.NoOp,
    ) =
      hashNodeV1(
        node,
        nodeSeed = Some(nodeSeedExercise),
        subNodes = subNodes,
        hashTracer = hashTracer,
      )

    "be stable" in {
      hashExerciseNode(exerciseNode).toHexString shouldBe defaultHash.toHexString
    }

    "not include global keys" in {
      a[NodeHashingError.UnsupportedFeature] shouldBe thrownBy(
        hashExerciseNode(
          exerciseNode.copy(keyOpt = Some(globalKey2))
        )
      )
    }

    "not include choiceAuthorizers" in {
      a[NodeHashingError.UnsupportedFeature] shouldBe thrownBy(
        hashExerciseNode(
          exerciseNode.copy(choiceAuthorizers =
            Some(Set[Party](Ref.Party.assertFromString("alice")))
          )
        )
      )
    }

    "not include byKey" in {
      hashExerciseNode(
        exerciseNode.copy(
          byKey = false
        )
      ) shouldBe defaultHash
    }

    "throw if some nodes are missing" in {
      an[IncompleteTransactionTree] shouldBe thrownBy {
        hashExerciseNode(exerciseNode, subNodes = Map.empty)
      }
    }

    "not hash NodeIds" in {
      TransactionHash.tryHashNodeV1(
        exerciseNode
          // Shift all node ids by one and expect it to have no impact
          .copy(children = shiftNodeIds(exerciseNode.children)),
        subNodes = shiftNodeIds(subNodesMap),
        nodeSeed = Some(nodeSeedExercise),
        nodeSeeds = shiftNodeIdsSeeds(defaultNodeSeedsMap),
      ) shouldBe defaultHash
    }

    "not produce collision in contractId" in {
      hashExerciseNode(
        exerciseNode.copy(
          targetCoid = ContractId.V1.assertFromString(
            "0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b"
          )
        )
      ) should !==(defaultHash)
    }

    "not produce collision in package name" in {
      hashExerciseNode(
        exerciseNode.copy(
          packageName = PackageName.assertFromString("another_package_name")
        )
      ) should !==(defaultHash)
    }

    "not produce collision in template ID" in {
      hashExerciseNode(
        exerciseNode.copy(
          templateId = defRef("othermodule", "othername")
        )
      ) should !==(defaultHash)
    }

    "not produce collision in actingParties" in {
      hashExerciseNode(
        exerciseNode.copy(
          actingParties = Set[Party](Ref.Party.assertFromString("charlie"))
        )
      ) should !==(defaultHash)
    }

    "not produce collision in signatories" in {
      hashExerciseNode(
        exerciseNode.copy(
          signatories = Set[Party](Ref.Party.assertFromString("bob"))
        )
      ) should !==(defaultHash)
    }

    "not produce collision in stakeholders" in {
      hashExerciseNode(
        exerciseNode.copy(
          stakeholders = Set[Party](Ref.Party.assertFromString("alice"))
        )
      ) should !==(defaultHash)
    }

    "not produce collision in choiceObservers" in {
      hashExerciseNode(
        exerciseNode.copy(
          choiceObservers = Set[Party](Ref.Party.assertFromString("alice"))
        )
      ) should !==(defaultHash)
    }

    "not produce collision in children" in {
      hashExerciseNode(
        exerciseNode.copy(
          children = exerciseNode.children.reverse
        )
      ) should !==(defaultHash)
    }

    "not produce collision in interface Id" in {
      hashExerciseNode(
        exerciseNode.copy(
          interfaceId = None
        )
      ) should !==(defaultHash)
    }

    "not produce collision in exercise result" in {
      hashExerciseNode(
        exerciseNode.copy(
          exerciseResult = None
        )
      ) should !==(defaultHash)
    }

    "explain encoding" in {
      val hashTracer = HashTracer.StringHashTracer()
      val hash = hashExerciseNode(exerciseNode, hashTracer = hashTracer)
      hash shouldBe defaultHash
      hashTracer.result shouldBe s"""'01' # 01 (Node Encoding Version)
                                    |# Exercise Node
                                    |# Node Version
                                    |'00000003' # 3 (int)
                                    |'322e31' # 2.1 (string)
                                    |'01' # Exercise Node Tag
                                    |# Node Seed
                                    |'a867edafa1277f46f879ab92c373a15c2d75c5d86fec741705cee1eb01ef8c9e' # seed
                                    |# Contract Id
                                    |'00000021' # 33 (int)
                                    |'0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5' # 0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5 (contractId)
                                    |# Package Name
                                    |'0000000e' # 14 (int)
                                    |'7061636b6167652d6e616d652d30' # package-name-0 (string)
                                    |# Template Id
                                    |'00000007' # 7 (int)
                                    |'7061636b616765' # package (string)
                                    |'00000001' # 1 (int)
                                    |'00000006' # 6 (int)
                                    |'6d6f64756c65' # module (string)
                                    |'00000001' # 1 (int)
                                    |'00000004' # 4 (int)
                                    |'6e616d65' # name (string)
                                    |# Signatories
                                    |'00000001' # 1 (int)
                                    |'00000005' # 5 (int)
                                    |'616c696365' # alice (string)
                                    |# Stakeholders
                                    |'00000001' # 1 (int)
                                    |'00000007' # 7 (int)
                                    |'636861726c6965' # charlie (string)
                                    |# Acting Parties
                                    |'00000002' # 2 (int)
                                    |'00000005' # 5 (int)
                                    |'616c696365' # alice (string)
                                    |'00000003' # 3 (int)
                                    |'626f62' # bob (string)
                                    |# Interface Id
                                    |'01' # Some
                                    |'00000007' # 7 (int)
                                    |'7061636b616765' # package (string)
                                    |'00000001' # 1 (int)
                                    |'00000010' # 16 (int)
                                    |'696e746572666163655f6d6f64756c65' # interface_module (string)
                                    |'00000001' # 1 (int)
                                    |'0000000e' # 14 (int)
                                    |'696e746572666163655f6e616d65' # interface_name (string)
                                    |# Choice Id
                                    |'00000006' # 6 (int)
                                    |'63686f696365' # choice (string)
                                    |# Chosen Value
                                    |'02' # Int64 Type Tag
                                    |'0000000000007a94' # 31380 (long)
                                    |# Consuming
                                    |'01' # true (bool)
                                    |# Exercise Result
                                    |'01' # Some
                                    |'07' # Text Type Tag
                                    |'00000006' # 6 (int)
                                    |'726573756c74' # result (string)
                                    |# Choice Observers
                                    |'00000001' # 1 (int)
                                    |'00000005' # 5 (int)
                                    |'6461766964' # david (string)
                                    |# Children
                                    |'00000002' # 2 (int)
                                    |'$createNodeHash' # (Hashed Inner Node)
                                    |'$fetchNodeHash' # (Hashed Inner Node)
                                    |""".stripMargin

      assertStringTracer(hashTracer, hash)
    }
  }

  "RollbackNode Builder V1" should {
    val defaultHash = Hash
      .fromHexStringRaw(rollbackNodeHash)
      .getOrElse(fail("Invalid hash"))

    def hashRollbackNode(
        node: Node.Rollback,
        subNodes: Map[NodeId, Node] = subNodesMap,
        hashTracer: HashTracer = HashTracer.NoOp,
    ) =
      TransactionHash.tryHashNodeV1(
        node,
        nodeSeed = Some(nodeSeedRollback),
        nodeSeeds = defaultNodeSeedsMap,
        subNodes = subNodes,
        hashTracer = hashTracer,
      )

    "be stable" in {
      hashRollbackNode(rollbackNode).toHexString shouldBe defaultHash.toHexString
    }

    "throw if some nodes are missing" in {
      an[IncompleteTransactionTree] shouldBe thrownBy {
        hashRollbackNode(rollbackNode, subNodes = Map.empty)
      }
    }

    "not hash NodeIds" in {
      TransactionHash.tryHashNodeV1(
        rollbackNode
          // Change the node Ids values but not the nodes
          .copy(children = shiftNodeIds(rollbackNode.children)),
        subNodes = shiftNodeIds(subNodesMap),
        nodeSeed = Some(nodeSeedRollback),
        nodeSeeds = shiftNodeIdsSeeds(defaultNodeSeedsMap),
      ) shouldBe defaultHash
    }

    "not produce collision in children" in {
      hashRollbackNode(
        rollbackNode.copy(
          children = rollbackNode.children.reverse
        )
      ) should !==(defaultHash)
    }

    "explain encoding" in {
      val hashTracer = HashTracer.StringHashTracer()
      val hash = hashRollbackNode(rollbackNode, hashTracer = hashTracer)
      hash shouldBe defaultHash
      hashTracer.result shouldBe s"""'01' # 01 (Node Encoding Version)
                                      |# Rollback Node
                                      |'03' # Rollback Node Tag
                                      |# Children
                                      |'00000002' # 2 (int)
                                      |'$fetchNodeHash' # (Hashed Inner Node)
                                      |'$exerciseNodeHash' # (Hashed Inner Node)
                                      |""".stripMargin

      assertStringTracer(hashTracer, hash)
    }
  }

  "TransactionBuilder" should {
    val roots = ImmArray(NodeId(0), NodeId(5))
    val transaction = VersionedTransaction(
      version = LanguageVersion.v2_1,
      roots = roots,
      nodes = subNodesMap,
    )

    val defaultHash = Hash
      .fromHexStringRaw("154f334d24a8a5e4d0ce51ac87d93821b3256f885f21d3f779a1640abf481983")
      .getOrElse(fail("Invalid hash"))

    "be stable" in {
      TransactionHash
        .tryHashTransactionV1(transaction, defaultNodeSeedsMap)
        .toHexString shouldBe defaultHash.toHexString
    }

    "throw if some nodes are missing" in {
      an[IncompleteTransactionTree] shouldBe thrownBy {
        TransactionHash.tryHashTransactionV1(
          VersionedTransaction(
            version = LanguageVersion.v2_1,
            roots = roots,
            nodes = Map.empty,
          ),
          defaultNodeSeedsMap,
        )
      }
    }

    "not hash NodeIds" in {
      TransactionHash.tryHashTransactionV1(
        VersionedTransaction(
          version = LanguageVersion.v2_1,
          roots = shiftNodeIds(roots),
          nodes = shiftNodeIds(subNodesMap),
        ),
        shiftNodeIdsSeeds(defaultNodeSeedsMap),
      ) shouldBe defaultHash
    }

    "not produce collision in children" in {
      TransactionHash.tryHashTransactionV1(
        VersionedTransaction(
          version = LanguageVersion.v2_1,
          roots = roots.reverse,
          nodes = subNodesMap,
        ),
        defaultNodeSeedsMap,
      ) should !==(defaultHash)
    }

    "explain encoding" in {
      val hashTracer = HashTracer.StringHashTracer()
      val hash = TransactionHash.tryHashTransactionV1(
        transaction,
        defaultNodeSeedsMap,
        hashTracer = hashTracer,
      )
      hashTracer.result shouldBe s"""'00000030' # Hash Purpose
                                      |# Transaction Version
                                      |'00000003' # 3 (int)
                                      |'322e31' # 2.1 (string)
                                      |# Root Nodes
                                      |'00000002' # 2 (int)
                                      |'$createNodeHash' # (Hashed Inner Node)
                                      |'$rollbackNodeHash' # (Hashed Inner Node)
                                      |""".stripMargin
      assertStringTracer(hashTracer, hash)
    }
  }

  "Full Transaction Hash" should {
    val roots = ImmArray(NodeId(0), NodeId(5))
    val transaction = VersionedTransaction(
      version = LanguageVersion.v2_1,
      roots = roots,
      nodes = subNodesMap,
    )

    val defaultHash = Hash
      .fromHexStringRaw("adcef14ae479ab5b9424a89d5bf28ee666f336a5463f65b097d587120123d019")
      .getOrElse(fail("Invalid hash"))

    "be stable" in {
      TransactionHash
        .tryHashTransactionWithMetadataV1(transaction, defaultNodeSeedsMap, metadata)
        .toHexString shouldBe defaultHash.toHexString
    }

    "explain encoding" in {
      val hashTracer = HashTracer.StringHashTracer()
      val hash = TransactionHash.tryHashTransactionWithMetadataV1(
        transaction,
        defaultNodeSeedsMap,
        metadata,
        hashTracer = hashTracer,
      )
      hashTracer.result shouldBe s"""'00000030' # Hash Purpose
                                      |'02' # 02 (Hashing Scheme Version)
                                      |'154f334d24a8a5e4d0ce51ac87d93821b3256f885f21d3f779a1640abf481983' # Transaction
                                      |'2a0690693367f70fbe83e5e99df6930dbd2336618a3a0721bb6fa3bcc88d5a53' # Metadata
                                      |""".stripMargin
      assertStringTracer(hashTracer, hash)
    }
  }
}
