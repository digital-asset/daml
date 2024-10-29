// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package crypto

import com.daml.crypto.MessageDigestPrototype
import com.digitalasset.daml.lf.crypto.Hash.NodeHashingError
import com.digitalasset.daml.lf.crypto.HashUtils.HashTracer
import com.digitalasset.daml.lf.crypto.Hash.NodeHashingError.IncompleteTransactionTree
import com.digitalasset.daml.lf.crypto.HashUtils.HashTracer.StringHashTracer
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

class NodeHashSpec extends AnyWordSpec with Matchers {

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
    agreementText = "NOT_PART_OF_HASH",
    signatories =
      Set[Party](Ref.Party.assertFromString("alice"), Ref.Party.assertFromString("bob")),
    stakeholders =
      Set[Party](Ref.Party.assertFromString("alice"), Ref.Party.assertFromString("charlie")),
    keyOpt = Some(globalKey),
    version = LanguageVersion.v2_1,
  )

  private val createNodeEncoding = """'01' # 01 (node_version)
                                     |# Create Node
                                     |'00' # Node Tag
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

  private val createNodeHash = "3de658958601401fc344b999d0ab7fc6a3761dea3f0aa8d86f5b119626ed9516"
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
    keyOpt = Some(
      GlobalKeyWithMaintainers(
        GlobalKey.assertBuild(
          defRef("module_key", "name"),
          VA.text.inj("hello"),
          PackageName.assertFromString("package_name_key"),
        ),
        Set[Party](Ref.Party.assertFromString("david")),
      )
    ),
    byKey = true,
    interfaceId = Some(defRef("interface_module", "interface_name")),
    version = LanguageVersion.v2_1,
  )

  private val fetchNodeEncoding = """'01' # 01 (node_version)
                                    |# Fetch Node
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
                                    |'696e746572666163655f6e616d65' # interface_name (string)""".stripMargin

  private val fetchNodeHash = "90a81b06c8125fbdf3ace171554c397b185381a01856c48dd301409ef2734aa6"
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
    choiceAuthorizers = Some(Set[Party](Ref.Party.assertFromString("eve"))),
    children = ImmArray(NodeId(0), NodeId(1)),
    exerciseResult = Some(VA.text.inj("result")),
    keyOpt = Some(globalKey),
    byKey = true,
    version = LanguageVersion.v2_1,
  )

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
    children = ImmArray(NodeId(3), NodeId(4))
  )

  // Function to assert that the tracing does match the hash
  private def assertStringTracer(stringHashTracer: StringHashTracer, hash: Hash) = {
    val messageDigest = MessageDigestPrototype.Sha256.newDigest
    messageDigest.update(stringHashTracer.asByteArray)
    Hash.assertFromByteArray(messageDigest.digest()) shouldBe hash
  }

  "V1Encoding" should {
    "not encode lookup nodes" in {
      a[NodeHashingError.UnsupportedNode] shouldBe thrownBy {
        Hash.hashNode(lookupNode)
      }
    }
  }

  "CreateNodeBuilder V1" should {
    val defaultHash = Hash
      .fromString(createNodeHash)
      .getOrElse(fail("Invalid hash"))

    "be stable" in {
      Hash.hashNode(createNode) shouldBe defaultHash
    }

    "not include agreement text" in {
      Hash.hashNode(createNode.copy(agreementText = "SOMETHING_ELSE")) shouldBe defaultHash
    }

    "not include global keys" in {
      Hash.hashNode(
        createNode.copy(
          keyOpt = Some(globalKey2)
        )
      ) shouldBe defaultHash
    }

    "not produce collision in contractId" in {
      Hash.hashNode(
        createNode.copy(
          coid = ContractId.V1.assertFromString(
            "0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b"
          )
        )
      ) should !==(defaultHash)
    }

    "not produce collision in package name" in {
      Hash.hashNode(
        createNode.copy(
          packageName = PackageName.assertFromString("another_package_name")
        )
      ) should !==(defaultHash)
    }

    "not produce collision in template ID" in {
      Hash.hashNode(
        createNode.copy(
          templateId = defRef("othermodule", "othername")
        )
      ) should !==(defaultHash)
    }

    "not produce collision in arg" in {
      Hash.hashNode(
        createNode.copy(
          arg = VA.bool.inj(true)
        )
      ) should !==(defaultHash)
    }

    "not produce collision in signatories" in {
      Hash.hashNode(
        createNode.copy(
          signatories = Set[Party](Ref.Party.assertFromString("alice"))
        )
      ) should !==(defaultHash)
    }

    "not produce collision in stakeholders" in {
      Hash.hashNode(
        createNode.copy(
          stakeholders = Set[Party](Ref.Party.assertFromString("alice"))
        )
      ) should !==(defaultHash)
    }

    "explain encoding" in {
      {
        val hashTracer = new HashTracer.StringHashTracer()
        val hash = Hash.hashNode(createNode, hashTracer = hashTracer)
        hash shouldBe defaultHash
        hashTracer.result shouldBe s"""'00' # 00 (value_version)
                             |'07' # 07 (value_purpose)
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

    "be stable" in {
      Hash.hashNode(fetchNode) shouldBe defaultHash
    }

    "not include global keys" in {
      Hash.hashNode(
        fetchNode.copy(
          keyOpt = Some(globalKey2)
        )
      ) shouldBe defaultHash
    }

    "not include byKey" in {
      Hash.hashNode(
        fetchNode.copy(
          byKey = false
        )
      ) shouldBe defaultHash
    }

    "not produce collision in contractId" in {
      Hash.hashNode(
        fetchNode.copy(
          coid = ContractId.V1.assertFromString(
            "0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b"
          )
        )
      ) should !==(defaultHash)
    }

    "not produce collision in package name" in {
      Hash.hashNode(
        fetchNode.copy(
          packageName = PackageName.assertFromString("another_package_name")
        )
      ) should !==(defaultHash)
    }

    "not produce collision in template ID" in {
      Hash.hashNode(
        fetchNode.copy(
          templateId = defRef("othermodule", "othername")
        )
      ) should !==(defaultHash)
    }

    "not produce collision in actingParties" in {
      Hash.hashNode(
        fetchNode.copy(
          actingParties = Set[Party](Ref.Party.assertFromString("charlie"))
        )
      ) should !==(defaultHash)
    }

    "not produce collision in signatories" in {
      Hash.hashNode(
        fetchNode.copy(
          signatories = Set[Party](Ref.Party.assertFromString("bob"))
        )
      ) should !==(defaultHash)
    }

    "not produce collision in stakeholders" in {
      Hash.hashNode(
        fetchNode.copy(
          stakeholders = Set[Party](Ref.Party.assertFromString("alice"))
        )
      ) should !==(defaultHash)
    }

    "not produce collision in interface Id" in {
      Hash.hashNode(
        fetchNode.copy(
          interfaceId = None
        )
      ) should !==(defaultHash)
    }

    "explain encoding" in {
      val hashTracer = new HashTracer.StringHashTracer()
      val hash = Hash.hashNode(fetchNode, hashTracer = hashTracer)
      hash shouldBe defaultHash
      hashTracer.result shouldBe s"""'00' # 00 (value_version)
                                    |'07' # 07 (value_purpose)
                                    |$fetchNodeEncoding
                                    |""".stripMargin

      assertStringTracer(hashTracer, hash)
    }
  }

  "ExerciseNodeBuilder V1" should {
    val defaultHash = Hash
      .fromString("ffc9d43e5dc25e16a63d12c877ecce429f8c572bee9e5be3e24ba5763f5a0b13")
      .getOrElse(fail("Invalid hash"))

    val subNodes = Map(NodeId(0) -> createNode, NodeId(1) -> fetchNode)
    def hashExerciseNode(node: Node.Exercise) = {
      Hash.hashNode(node, subNodes)
    }

    "be stable" in {
      hashExerciseNode(exerciseNode) shouldBe defaultHash
    }

    "not include global keys" in {
      hashExerciseNode(
        exerciseNode.copy(
          keyOpt = Some(globalKey2)
        )
      ) shouldBe defaultHash
    }

    "not include choiceAuthorizers" in {
      hashExerciseNode(
        exerciseNode.copy(
          choiceAuthorizers = Some(Set[Party](Ref.Party.assertFromString("alice")))
        )
      ) shouldBe defaultHash
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
        Hash.hashNode(exerciseNode)
      }
    }

    "not hash NodeIds" in {
      Hash.hashNode(
        exerciseNode
          // Change the node Ids values
          .copy(children = exerciseNode.children.map(nodeId => NodeId(nodeId.index + 1))),
        subNodes.map { case (nodeId, node) =>
          NodeId(nodeId.index + 1) -> node
        },
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
      val hashTracer = new HashTracer.StringHashTracer()
      val hash = Hash.hashNode(exerciseNode, subNodes, hashTracer = hashTracer)
      hash shouldBe defaultHash
      hashTracer.result shouldBe s"""'00' # 00 (value_version)
                                      |'07' # 07 (value_purpose)
                                      |'01' # 01 (node_version)
                                      |# Exercise Node
                                      |'01' # Node Tag
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
      .fromString("3c765116f7941cc496fa49e8fa98e79e50b1506725258ba7d93eaaf78219c88a")
      .getOrElse(fail("Invalid hash"))

    val subNodes = Map(NodeId(3) -> createNode, NodeId(4) -> fetchNode)
    def hashRollbackNode(node: Node.Rollback) = {
      Hash.hashNode(node, subNodes)
    }

    "be stable" in {
      hashRollbackNode(rollbackNode) shouldBe defaultHash
    }

    "throw if some nodes are missing" in {
      an[IncompleteTransactionTree] shouldBe thrownBy {
        Hash.hashNode(rollbackNode)
      }
    }

    "not hash NodeIds" in {
      Hash.hashNode(
        rollbackNode
          // Change the node Ids values but not the nodes
          .copy(children = rollbackNode.children.map(nodeId => NodeId(nodeId.index + 1))),
        subNodes.map { case (nodeId, node) =>
          NodeId(nodeId.index + 1) -> node
        },
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
        val hashTracer = new HashTracer.StringHashTracer()
        val hash = Hash.hashNode(rollbackNode, subNodes, hashTracer = hashTracer)
        hash shouldBe defaultHash
        hashTracer.result shouldBe s"""'00' # 00 (value_version)
                                      |'07' # 07 (value_purpose)
                                      |'01' # 01 (node_version)
                                      |# Rollback Node
                                      |'04' # Node Tag
                                      |# Children
                                      |'00000002' # 2 (int)
                                      |'$createNodeHash' # (Hashed Inner Node)
                                      |'$fetchNodeHash' # (Hashed Inner Node)
                                      |""".stripMargin

        assertStringTracer(hashTracer, hash)
      }
    }
  }

  "ValueBuilder" should {
    def withValueBuilder(f: (Hash.ValueHashBuilder, HashTracer.StringHashTracer) => Assertion) = {
      {
        val hashTracer = new HashTracer.StringHashTracer()
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
              Some(Ref.Name.assertFromString("field1")), // fields are NOT part of the hash
              Value.ValueTrue,
            ),
            (
              Some(Ref.Name.assertFromString("field2")), // fields are NOT part of the hash
              Value.ValueText("hello"),
            ),
          ),
        ),
        "fa5f5d67d77d85f097e84ab6afc800794e8faa2a1f633e3c7ee9e6dc95e7466c",
        """'00000002' # 2 (int)
          |'01' # true (bool)
          |'00000005' # 5 (int)
          |'68656c6c6f' # hello (string)
          |""".stripMargin,
      )
    }
  }

  "TransactionBuilder" should {
    val roots = ImmArray(NodeId(2), NodeId(5))
    val nodes = Map(
      NodeId(0) -> createNode,
      NodeId(1) -> fetchNode,
      NodeId(2) -> exerciseNode,
      NodeId(3) -> createNode2,
      NodeId(4) -> fetchNode2,
      NodeId(5) -> rollbackNode,
    )
    val transaction = VersionedTransaction(
      version = LanguageVersion.v2_1,
      roots = roots,
      nodes = nodes,
    )

    val defaultHash = Hash
      .fromString("930995f3dc176ac4cd608fa4d5e1388633bfd0a76abe360f24a98c8fc8ea3f89")
      .getOrElse(fail("Invalid hash"))

    "be stable" in {
      Hash.hashTransaction(transaction) shouldBe defaultHash
    }

    "throw if some nodes are missing" in {
      an[IncompleteTransactionTree] shouldBe thrownBy {
        Hash.hashTransaction(
          VersionedTransaction(
            version = LanguageVersion.v2_1,
            roots = roots,
            nodes = nodes.drop(2),
          )
        )
      }
    }

    "not hash NodeIds" in {
      Hash.hashTransaction(
        VersionedTransaction(
          version = LanguageVersion.v2_1,
          roots = ImmArray(NodeId(8), NodeId(44)),
          nodes = Map(
            NodeId(75) -> createNode,
            NodeId(84) -> fetchNode,
            NodeId(8) -> exerciseNode.copy(children = ImmArray(NodeId(75), NodeId(84))),
            NodeId(15) -> createNode2,
            NodeId(2009) -> fetchNode2,
            NodeId(44) -> rollbackNode.copy(children = ImmArray(NodeId(15), NodeId(2009))),
          ),
        )
      ) shouldBe defaultHash
    }

    "not produce collision in children" in {
      Hash.hashTransaction(
        VersionedTransaction(
          version = LanguageVersion.v2_1,
          roots = roots.reverse,
          nodes = nodes,
        )
      ) should !==(defaultHash)
    }

    "explain encoding" in {
      {
        val hashTracer = new HashTracer.StringHashTracer()
        val hash = Hash.hashTransaction(
          VersionedTransaction(
            version = LanguageVersion.v2_1,
            roots = ImmArray(NodeId(1), NodeId(2)),
            nodes = Map(NodeId(1) -> createNode, NodeId(2) -> fetchNode),
          ),
          hashTracer = hashTracer,
        )
        hashTracer.result shouldBe s"""# Transaction Version
                                      |'00000003' # 3 (int)
                                      |'322e31' # 2.1 (string)
                                      |# Root Nodes
                                      |'00000002' # 2 (int)
                                      |'3de658958601401fc344b999d0ab7fc6a3761dea3f0aa8d86f5b119626ed9516' # (Hashed Inner Node)
                                      |'90a81b06c8125fbdf3ace171554c397b185381a01856c48dd301409ef2734aa6' # (Hashed Inner Node)
                                      |""".stripMargin
        assertStringTracer(hashTracer, hash)
      }
    }
  }
}
