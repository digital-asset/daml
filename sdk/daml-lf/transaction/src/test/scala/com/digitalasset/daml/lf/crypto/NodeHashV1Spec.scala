// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package crypto

import com.digitalasset.daml.lf.crypto.Hash.NodeHashingError
import com.digitalasset.daml.lf.crypto.HashUtils.HashTracer
import com.digitalasset.daml.lf.crypto.Hash.NodeHashingError.IncompleteTransactionTree
import com.digitalasset.daml.lf.data.Ref.{ChoiceName, PackageName, Party}
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml.lf.value.test.TypedValueGenerators.{ValueAddend => VA}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class NodeHashV1Spec extends AnyWordSpec with Matchers with HashUtils {

  private val packageId0 = Ref.PackageId.assertFromString("package")
  private val packageName0 = Ref.PackageName.assertFromString("package-name-0")

  private def defRef(module: String, name: String): Ref.Identifier =
    Ref.Identifier(
      packageId0,
      Ref.QualifiedName(
        Ref.DottedName.assertFromString(module),
        Ref.DottedName.assertFromString(name),
      ),
    )

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
    packageVersion = None,
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
                                     |'00' # Node Tag
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

  private val createNodeHash = "14f24b00a2a28468e375fbc905ee6e9cff787cbf5bfc90dfaf59c8100f54bdfb"
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
                                    |'02' # Node Tag
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
                                    |'626f62' # bob (string)""".stripMargin

  private val fetchNodeHash = "a148c7872b41fc17349d82a5e7dc460b3477c8d083f08b7b144c8275ed5adfcc"
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

  private val exerciseNodeHash = "5b9af41fe9032a70a772063301907c823e933d2df5bae2b48293f33cf3992611"

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

  private val rollbackNodeHash = "5905d4c994867f927acbbea20a19d797656c9616c5bdd6507cd72b583529e983"

  private val nodeSeedCreate =
    Hash.assertFromString("926bbb6f341bc0092ae65d06c6e284024907148cc29543ef6bff0930f5d52c19")
  private val nodeSeedFetch =
    Hash.assertFromString("4d2a522e9ee44e31b9bef2e3c8a07d43475db87463c6a13c4ea92f898ac8a930")
  private val nodeSeedExercise =
    Hash.assertFromString("a867edafa1277f46f879ab92c373a15c2d75c5d86fec741705cee1eb01ef8c9e")
  private val nodeSeedRollback =
    Hash.assertFromString("5483d5df9b245e662c0e4368b8062e8a0fd24c17ce4ded1a0e452e4ee879dd81")
  private val nodeSeedCreate2 =
    Hash.assertFromString("e0c69eae8afb38872fa425c2cdba794176f3b9d97e8eefb7b0e7c831f566458f")
  private val nodeSeedFetch2 =
    Hash.assertFromString("b4f0534e651ac8d5e10d95ddfcafdb123550a5e3185e3fe61ec1746a7222a88e")

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
      nodeSeed: Option[Hash],
      subNodes: Map[NodeId, Node] = subNodesMap,
      hashTracer: HashTracer,
  ) = Hash.hashNodeV1(
    node,
    defaultNodeSeedsMap,
    nodeSeed,
    subNodes,
    hashTracer,
  )

  private def shiftNodeIds(array: ImmArray[NodeId]): ImmArray[NodeId] = array.map {
    case NodeId(i) => NodeId(i + 1)
  }

  private def shiftNodeIdsSeeds(map: Map[NodeId, Hash]): Map[NodeId, Hash] = map.map {
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
        Hash.hashNodeV1(lookupNode)
      }
    }

    "not encode create nodes without node seed" in {
      a[NodeHashingError.MissingNodeSeed] shouldBe thrownBy {
        Hash.hashNodeV1(createNode, enforceNodeSeedForCreateNodes = true)
      }
    }

    "not encode exercise nodes without node seed" in {
      a[NodeHashingError.MissingNodeSeed] shouldBe thrownBy {
        Hash.hashNodeV1(exerciseNode, enforceNodeSeedForCreateNodes = true)
      }
    }

    "encode create nodes without node seed if explicitly allowed" in {
      scala.util
        .Try(Hash.hashNodeV1(createNode, enforceNodeSeedForCreateNodes = false))
        .isSuccess shouldBe true
    }
  }

  "CreateNodeBuilder V1" should {
    val defaultHash = Hash
      .fromString(createNodeHash)
      .getOrElse(fail("Invalid hash"))

    def hashCreateNode(node: Node.Create, hashTracer: HashTracer = HashTracer.NoOp) = {
      hashNodeV1(node, Some(nodeSeedCreate), hashTracer = hashTracer)
    }

    "be stable" in {
      hashCreateNode(createNode) shouldBe defaultHash
    }

    "not include agreement text" in {
      hashCreateNode(createNode.copy(agreementText = "SOMETHING_ELSE")) shouldBe defaultHash
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
      {
        val hashTracer = HashTracer.StringHashTracer()
        val hash = hashCreateNode(createNode, hashTracer = hashTracer)
        hash shouldBe defaultHash
        hashTracer.result shouldBe s"""'00' # 00 (Value Encoding Version)
                             |'07' # 07 (Value Encoding Purpose)
                             |$createNodeEncoding
                             |""".stripMargin
        assertStringTracer(hashTracer, hash)
      }
    }
  }

  "FetchNodeBuilder V1" should {
    val defaultHash = Hash
      .fromString(fetchNodeHash)
      .getOrElse(fail("Invalid hash"))

    def hashFetchNode(node: Node.Fetch, hashTracer: HashTracer = HashTracer.NoOp) = {
      hashNodeV1(node, nodeSeed = Some(nodeSeedFetch), hashTracer = hashTracer)
    }

    "be stable" in {
      hashFetchNode(fetchNode) shouldBe defaultHash
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
      hashTracer.result shouldBe s"""'00' # 00 (Value Encoding Version)
                                    |'07' # 07 (Value Encoding Purpose)
                                    |$fetchNodeEncoding
                                    |""".stripMargin

      assertStringTracer(hashTracer, hash)
    }
  }

  "ExerciseNodeBuilder V1" should {
    val defaultHash = Hash
      .fromString(exerciseNodeHash)
      .getOrElse(fail("Invalid hash"))

    def hashExerciseNode(
        node: Node.Exercise,
        subNodes: Map[NodeId, Node] = subNodesMap,
        hashTracer: HashTracer = HashTracer.NoOp,
    ) = {
      hashNodeV1(
        node,
        nodeSeed = Some(nodeSeedExercise),
        subNodes = subNodes,
        hashTracer = hashTracer,
      )
    }

    "be stable" in {
      hashExerciseNode(exerciseNode) shouldBe defaultHash
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
      Hash.hashNodeV1(
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
      hashTracer.result shouldBe s"""'00' # 00 (Value Encoding Version)
                                      |'07' # 07 (Value Encoding Purpose)
                                      |'01' # 01 (Node Encoding Version)
                                      |# Exercise Node
                                      |# Node Version
                                      |'00000003' # 3 (int)
                                      |'322e31' # 2.1 (string)
                                      |'01' # Node Tag
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
                                      |'0000000000007a94' # 31380 (long)
                                      |# Consuming
                                      |'01' # true (bool)
                                      |# Exercise Result
                                      |'01' # Some
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
      .fromString(rollbackNodeHash)
      .getOrElse(fail("Invalid hash"))

    def hashRollbackNode(
        node: Node.Rollback,
        subNodes: Map[NodeId, Node] = subNodesMap,
        hashTracer: HashTracer = HashTracer.NoOp,
    ) = {
      Hash.hashNodeV1(
        node,
        nodeSeed = Some(nodeSeedRollback),
        nodeSeeds = defaultNodeSeedsMap,
        subNodes = subNodes,
        hashTracer = hashTracer,
      )
    }

    "be stable" in {
      hashRollbackNode(rollbackNode) shouldBe defaultHash
    }

    "throw if some nodes are missing" in {
      an[IncompleteTransactionTree] shouldBe thrownBy {
        hashRollbackNode(rollbackNode, subNodes = Map.empty)
      }
    }

    "not hash NodeIds" in {
      Hash.hashNodeV1(
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
      {
        val hashTracer = HashTracer.StringHashTracer()
        val hash = hashRollbackNode(rollbackNode, hashTracer = hashTracer)
        hash shouldBe defaultHash
        hashTracer.result shouldBe s"""'00' # 00 (Value Encoding Version)
                                      |'07' # 07 (Value Encoding Purpose)
                                      |'01' # 01 (Node Encoding Version)
                                      |# Rollback Node
                                      |'04' # Node Tag
                                      |# Children
                                      |'00000002' # 2 (int)
                                      |'$fetchNodeHash' # (Hashed Inner Node)
                                      |'$exerciseNodeHash' # (Hashed Inner Node)
                                      |""".stripMargin

        assertStringTracer(hashTracer, hash)
      }
    }
  }

  "ValueBuilder" should {
    def withValueBuilder(f: (Hash.ValueHashBuilder, HashTracer.StringHashTracer) => Assertion) = {
      {
        val hashTracer = HashTracer.StringHashTracer()
        val builder = Hash.valueBuilderForV1Node(hashTracer)
        f(builder, hashTracer)
      }
    }

    def assertEncode(value: Value, expectedHash: String, expectedDebugEncoding: String) = {
      withValueBuilder { case (builder, hashTracer) =>
        val hash = builder.addTypedValue(value).build
        hash.toHexString shouldBe expectedHash
        hashTracer.result shouldBe expectedDebugEncoding
        assertStringTracer(hashTracer, hash)
      }
    }

    "encode unit value" in {
      assertEncode(
        Value.ValueUnit,
        "6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d",
        """'00' # 00 (unit)
          |""".stripMargin,
      )
    }

    "encode true value" in {
      assertEncode(
        Value.ValueBool(true),
        "4bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a",
        """'01' # true (bool)
          |""".stripMargin,
      )
    }

    "encode false value" in {
      assertEncode(
        Value.ValueBool(false),
        "6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d",
        """'00' # false (bool)
          |""".stripMargin,
      )
    }

    "encode text value" in {
      assertEncode(
        Value.ValueText("hello world!"),
        "5c565bbe3c8230ef9614db8546c67aef5bce169628e0bd6b1c7cc33687ce0af9",
        """'0000000c' # 12 (int)
          |'68656c6c6f20776f726c6421' # hello world! (string)
          |""".stripMargin,
      )
    }

    "encode numeric value" in {
      // Numerics are encoded from their string representation
      assertEncode(
        Value.ValueNumeric(data.Numeric.assertFromString("125.1002")),
        "0fc95b51582bace59f230996c4cd303de53c09071854f77e2700344d1b2555c7",
        """'00000008' # 8 (int)
          |'3132352e31303032' # 125.1002 (numeric)
          |""".stripMargin,
      )
    }

    "encode contract id value" in {
      assertEncode(
        Value.ValueContractId(
          Value.ContractId.V1
            .assertFromString("0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b")
        ),
        "e0b332966cef8940f0a8dbc08129a8868d3b1c36dc3f2fffd955c100558e8ac1",
        """'00000021' # 33 (int)
          |'0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b' # 0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b (contractId)
          |""".stripMargin,
      )
    }

    "encode enum value" in {
      assertEncode(
        Value.ValueEnum(Some(defRef("module", "name")), Ref.Name.assertFromString("ENUM")),
        "9917214dd61c334d5436ad6de190812e3a20d908f7c414ed3c1b01d904ab17c1",
        """'00000004' # 4 (int)
          |'454e554d' # ENUM (string)
          |""".stripMargin,
      )
    }

    "encode int64 value" in {
      assertEncode(
        Value.ValueInt64(10L),
        "8d85f8467240628a94819b26bee26e3a9b2804334c63482deacec8d64ab4e1e7",
        """'000000000000000a' # 10 (long)
          |""".stripMargin,
      )
    }

    "encode variant value" in {
      assertEncode(
        Value.ValueVariant(
          Some(defRef("module", "name")),
          Ref.Name.assertFromString("ENUM"),
          Value.ValueTrue,
        ),
        "fdb1c8d0beed4ac69e4d3204612c639ef4d0112ec47b5a0892c03fabc822546d",
        """'00000004' # 4 (int)
          |'454e554d' # ENUM (string)
          |'01' # true (bool)
          |""".stripMargin,
      )
    }

    "encode list value" in {
      assertEncode(
        Value.ValueList(
          FrontStack.from(
            List(
              Value.ValueText("five"),
              Value.ValueInt64(5L),
              Value.ValueTrue,
            )
          )
        ),
        "85a4041b88750e001d13b3cac6353cf41819c52c94937d2771c978854247b157",
        """'00000003' # 3 (int)
          |'00000004' # 4 (int)
          |'66697665' # five (string)
          |'0000000000000005' # 5 (long)
          |'01' # true (bool)
          |""".stripMargin,
      )
    }

    "encode text map value" in {
      assertEncode(
        Value.ValueTextMap(
          SortedLookupList(
            Map(
              "foo" -> Value.ValueNumeric(data.Numeric.assertFromString("31380.0")),
              "bar" -> Value.ValueText("1284"),
            )
          )
        ),
        "dfbb7030b50a33138ab21fea4acf65a4dcc728f79273252c28873867301a7768",
        """'00000002' # 2 (int)
          |'00000003' # 3 (int)
          |'626172' # bar (string)
          |'00000004' # 4 (int)
          |'31323834' # 1284 (string)
          |'00000003' # 3 (int)
          |'666f6f' # foo (string)
          |'00000007' # 7 (int)
          |'33313338302e30' # 31380.0 (numeric)
          |""".stripMargin,
      )
    }

    "encode gen map value" in {
      assertEncode(
        Value.ValueGenMap(
          ImmArray(
            (Value.ValueInt64(5L), Value.ValueText("five")),
            (Value.ValueInt64(10L), Value.ValueText("ten")),
          )
        ),
        "b092566476b5a3209642237c2f3c05868f30aa04a8d144bef640bcd9450f1fdd",
        """'00000002' # 2 (int)
          |'0000000000000005' # 5 (long)
          |'00000004' # 4 (int)
          |'66697665' # five (string)
          |'000000000000000a' # 10 (long)
          |'00000003' # 3 (int)
          |'74656e' # ten (string)
          |""".stripMargin,
      )
    }

    "encode optional empty value" in {
      assertEncode(
        Value.ValueOptional(None),
        "df3f619804a92fdb4057192dc43dd748ea778adc52bc498ce80524c014b81119",
        """'00000000' # 0 (int)
          |""".stripMargin,
      )
    }

    "encode optional defined value" in {
      assertEncode(
        Value.ValueOptional(Some(Value.ValueText("hello"))),
        "48a912ec1cbf8f3a5ee629c859e646a36fb50fb0c213dc6a01d250f14b436343",
        """'00000001' # 1 (int)
          |'00000005' # 5 (int)
          |'68656c6c6f' # hello (string)
          |""".stripMargin,
      )
    }

    "encode timestamp value" in {
      assertEncode(
        // Thursday, 24 October 2024 16:43:46
        Value.ValueTimestamp(
          Time.Timestamp.assertFromInstant(Instant.ofEpochMilli(1729788226000L))
        ),
        "07cf7b5fc18777a69daed0a5cf18b0af3b99922841f9dce07642bff5e29d1572",
        """'0006253bb4bf5480' # 1729788226000000 (long)
          |""".stripMargin,
      )
    }

    "encode date value" in {
      assertEncode(
        // Thursday, 24 October 2024
        Value.ValueDate(Time.Date.assertFromDaysSinceEpoch(20020)),
        "0437201334bcf43caa3632db5b12c4900b461b34391e89ed2317d934c6cf4b76",
        """'00004e34' # 20020 (int)
          |""".stripMargin,
      )
    }

    "encode record value" in {
      assertEncode(
        Value.ValueRecord(
          Some(defRef("module", "name")), // identifier is NOT part of the hash
          ImmArray(
            (
              Some(Ref.Name.assertFromString("field1")),
              Value.ValueTrue,
            ),
            (
              Some(Ref.Name.assertFromString("field2")),
              Value.ValueText("hello"),
            ),
          ),
        ),
        "8501e9fda13fab67faeb117b11c9c5d5b4429ebbf42037dd8a4b1d1367f288b7",
        """'00000002' # 2 (int)
          |'01' # Some
          |'00000006' # 6 (int)
          |'6669656c6431' # field1 (string)
          |'01' # true (bool)
          |'01' # Some
          |'00000006' # 6 (int)
          |'6669656c6432' # field2 (string)
          |'00000005' # 5 (int)
          |'68656c6c6f' # hello (string)
          |""".stripMargin,
      )
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
      .fromString("c70d6e53a6b2d402b384febbe3c1990520d89073ff9b8150a4699b30c16e5e1f")
      .getOrElse(fail("Invalid hash"))

    "be stable" in {
      Hash.hashTransactionV1(transaction, defaultNodeSeedsMap) shouldBe defaultHash
    }

    "throw if some nodes are missing" in {
      an[IncompleteTransactionTree] shouldBe thrownBy {
        Hash.hashTransactionV1(
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
      Hash.hashTransactionV1(
        VersionedTransaction(
          version = LanguageVersion.v2_1,
          roots = shiftNodeIds(roots),
          nodes = shiftNodeIds(subNodesMap),
        ),
        shiftNodeIdsSeeds(defaultNodeSeedsMap),
      ) shouldBe defaultHash
    }

    "not produce collision in children" in {
      Hash.hashTransactionV1(
        VersionedTransaction(
          version = LanguageVersion.v2_1,
          roots = roots.reverse,
          nodes = subNodesMap,
        ),
        defaultNodeSeedsMap,
      ) should !==(defaultHash)
    }

    "explain encoding" in {
      {
        val hashTracer = HashTracer.StringHashTracer()
        val hash = Hash.hashTransactionV1(
          transaction,
          defaultNodeSeedsMap,
          hashTracer = hashTracer,
        )
        hashTracer.result shouldBe s"""# Transaction Version
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
  }
}
