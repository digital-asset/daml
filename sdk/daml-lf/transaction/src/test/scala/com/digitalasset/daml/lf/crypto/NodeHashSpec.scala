// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.crypto.Hash.NodeHashingError.IncompleteTransactionTree
import com.digitalasset.daml.lf.crypto.HashUtils.DebugStringOutputStream
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.data.Ref.{ChoiceName, PackageName, Party}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.{
  GlobalKey,
  GlobalKeyWithMaintainers,
  Node,
  NodeId,
  VersionedTransaction,
}
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml.lf.value.test.TypedValueGenerators.{ValueAddend => VA}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

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

  private val createNode = Node.Create(
    coid = ContractId.V1.assertFromString(
      "0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5"
    ),
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

  private val fetchNode = Node.Fetch(
    coid = ContractId.V1.assertFromString(
      "0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5"
    ),
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
      ContractId.V1.assertFromString(
        "0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5"
      )
    ),
    version = LanguageVersion.v2_1,
  )

  private val lookupNode2 = Node.LookupByKey(
    packageName = packageName0,
    templateId = defRef("module", "name"),
    key = globalKey2,
    result = Some(
      ContractId.V1.assertFromString(
        "0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b"
      )
    ),
    version = LanguageVersion.v2_1,
  )

  private val rollbackNode = Node.Rollback(
    children = ImmArray(NodeId(3), NodeId(4))
  )

  "CreateNodeBuilder V1" should {
    val defaultHash = Hash
      .fromString("bada68ce6be10bbb6eb8ea3769fdaa6aa828022e219f2f83a54458b7f46c9c32")
      .getOrElse(fail("Invalid hash"))

    "be stable" in {
      Hash.hashNode(createNode) shouldBe defaultHash
    }

    "not include agreement text" in {
      Hash.hashNode(createNode.copy(agreementText = "SOMETHING_ELSE")) shouldBe defaultHash
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

    "not produce collision in global keys" in {
      Hash.hashNode(
        createNode.copy(
          keyOpt = Some(
            GlobalKeyWithMaintainers(
              GlobalKey.assertBuild(
                defRef("module_key", "name"),
                VA.text.inj("bye"), // Different from control value
                PackageName.assertFromString("package_name_key"),
              ),
              Set[Party](Ref.Party.assertFromString("david")),
            )
          )
        )
      ) should !==(defaultHash)
    }
  }

  "FetchNodeBuilder V1" should {
    val defaultHash = Hash
      .fromString("2d94f4f8a53f1dd0531e1095c677916442e32430a3fc5614069e57f695fcf8a8")
      .getOrElse(fail("Invalid hash"))

    "be stable" in {
      Hash.hashNode(fetchNode) shouldBe defaultHash
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

    "not produce collision in global keys" in {
      Hash.hashNode(
        fetchNode.copy(
          keyOpt = Some(globalKey2)
        )
      ) should !==(defaultHash)
    }

    "not produce collision in byKey" in {
      Hash.hashNode(
        fetchNode.copy(
          byKey = false
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
  }

  "ExerciseNodeBuilder V1" should {
    val defaultHash = Hash
      .fromString("625b53d3ac0c83160759b5f462433aa58c50a93ff4c1af909124bafa61ab7931")
      .getOrElse(fail("Invalid hash"))

    val subNodes = Map(NodeId(0) -> createNode, NodeId(1) -> fetchNode)
    def hashExerciseNode(node: Node.Exercise) = {
      Hash.hashNode(node, subNodes)
    }

    "be stable" in {
      hashExerciseNode(exerciseNode) shouldBe defaultHash
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

    "not produce collision in choiceAuthorizers" in {
      hashExerciseNode(
        exerciseNode.copy(
          choiceAuthorizers = Some(Set[Party](Ref.Party.assertFromString("alice")))
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

    "not produce collision in global keys" in {
      hashExerciseNode(
        exerciseNode.copy(
          keyOpt = Some(globalKey2)
        )
      ) should !==(defaultHash)
    }

    "not produce collision in byKey" in {
      hashExerciseNode(
        exerciseNode.copy(
          byKey = false
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
  }

  "LookupUpByKeyNodeBuilder V1" should {
    val defaultHash = Hash
      .fromString("6a8fffe7a3e178d7253e1e91a6f5f483ae5e3e35c31eb74caa4d71f8d2c6649a")
      .getOrElse(fail("Invalid hash"))

    "be stable" in {
      Hash.hashNode(lookupNode) shouldBe defaultHash
    }

    "not produce collision in package name" in {
      Hash.hashNode(
        lookupNode.copy(
          packageName = PackageName.assertFromString("another_package_name")
        )
      ) should !==(defaultHash)
    }

    "not produce collision in template ID" in {
      Hash.hashNode(
        lookupNode.copy(
          templateId = defRef("othermodule", "othername")
        )
      ) should !==(defaultHash)
    }

    "not produce collision in key" in {
      Hash.hashNode(
        lookupNode.copy(
          key = globalKey2
        )
      ) should !==(defaultHash)
    }

    "not produce collision in result" in {
      Hash.hashNode(
        lookupNode.copy(
          result = None
        )
      ) should !==(defaultHash)
    }
  }

  "RollbackNode Builder V1" should {
    val defaultHash = Hash
      .fromString("0d6628eb5db957ea614a19b0cfc0f0c6dd57b53b4c2ae48c5e978df9d230e706")
      .getOrElse(fail("Invalid hash"))

    val subNodes = Map(NodeId(3) -> lookupNode, NodeId(4) -> fetchNode)
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
  }

  "TransactionBuilder" should {
    val roots = ImmArray(NodeId(2), NodeId(5))
    val nodes = Map(
      NodeId(0) -> createNode,
      NodeId(1) -> fetchNode,
      NodeId(2) -> exerciseNode,
      NodeId(3) -> lookupNode,
      NodeId(4) -> lookupNode2,
      NodeId(5) -> rollbackNode,
    )
    val transaction = VersionedTransaction(
      version = LanguageVersion.v2_1,
      roots = roots,
      nodes = nodes,
    )

    val defaultHash = Hash
      .fromString("43cbd72afc15db4c35f88a3abcc5812f228b403ef6d9a312dd8e0bce564d54aa")
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
            NodeId(15) -> lookupNode,
            NodeId(2009) -> lookupNode2,
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

    "produce a debug output" in {
      val os = new DebugStringOutputStream
      val hash = Hash.hashTransactionDebug(transaction, os)
      hash shouldBe defaultHash

      os.result shouldBe """_Transaction_
       |_Root Nodes_
       |00000002 - [2 (int)]
       |0 - [0 (node_version)]
       |0 - [0 (value_version)]
       |7 - [7 (value_purpose)]
       |_Fetch Node_
       |_Contract Id_
       |00000021 - [33 (int)]
       |0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5 - [0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5 (contractId)]
       |_Package Name_
       |0000000e - [14 (int)]
       |7061636b6167652d6e616d652d30 - [package-name-0 (string)]
       |_Template Id_
       |00000007 - [7 (int)]
       |7061636b616765 - [package (string)]
       |00000001 - [1 (int)]
       |00000006 - [6 (int)]
       |6d6f64756c65 - [module (string)]
       |00000001 - [1 (int)]
       |00000004 - [4 (int)]
       |6e616d65 - [name (string)]
       |_Signatories_
       |00000001 - [1 (int)]
       |00000005 - [5 (int)]
       |616c696365 - [alice (string)]
       |_Stakeholders_
       |00000001 - [1 (int)]
       |00000007 - [7 (int)]
       |636861726c6965 - [charlie (string)]
       |_Acting Parties_
       |00000002 - [2 (int)]
       |00000005 - [5 (int)]
       |616c696365 - [alice (string)]
       |00000003 - [3 (int)]
       |626f62 - [bob (string)]
       |_Global Keys_
       |00000001 - [1 (int)]
       |_Maintainers_
       |00000001 - [1 (int)]
       |00000005 - [5 (int)]
       |6461766964 - [david (string)]
       |_Template Id_
       |00000001 - [1 (int)]
       |0000000a - [10 (int)]
       |6d6f64756c655f6b6579 - [module_key (string)]
       |00000001 - [1 (int)]
       |00000004 - [4 (int)]
       |6e616d65 - [name (string)]
       |_Package Name_
       |00000010 - [16 (int)]
       |7061636b6167655f6e616d655f6b6579 - [package_name_key (string)]
       |_Key_
       |00000005 - [5 (int)]
       |68656c6c6f - [hello (string)]
       |_By Key_
       |01 - [true]
       |_Interface Id_
       |00000001 - [1 (int)]
       |00000007 - [7 (int)]
       |7061636b616765 - [package (string)]
       |00000001 - [1 (int)]
       |00000010 - [16 (int)]
       |696e746572666163655f6d6f64756c65 - [interface_module (string)]
       |00000001 - [1 (int)]
       |0000000e - [14 (int)]
       |696e746572666163655f6e616d65 - [interface_name (string)]
       |_Choice Id_
       |00000006 - [6 (int)]
       |63686f696365 - [choice (string)]
       |_Chosen Value_
       |0000000000007a94 - [31380 (long)]
       |_Consuming_
       |01 - [true]
       |_Exercise Result_
       |00000001 - [1 (int)]
       |00000006 - [6 (int)]
       |726573756c74 - [result (string)]
       |_Choice Observers_
       |00000001 - [1 (int)]
       |00000005 - [5 (int)]
       |6461766964 - [david (string)]
       |_Choice Authorizers_
       |00000001 - [1 (int)]
       |00000001 - [1 (int)]
       |00000003 - [3 (int)]
       |657665 - [eve (string)]
       |_Children_
       |00000002 - [2 (int)]
       |0 - [0 (node_version)]
       |0 - [0 (value_version)]
       |7 - [7 (value_purpose)]
       |_Create Node_
       |_Contract Id_
       |00000021 - [33 (int)]
       |0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5 - [0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5 (contractId)]
       |_Package Name_
       |0000000e - [14 (int)]
       |7061636b6167652d6e616d652d30 - [package-name-0 (string)]
       |_Template Id_
       |00000007 - [7 (int)]
       |7061636b616765 - [package (string)]
       |00000001 - [1 (int)]
       |00000006 - [6 (int)]
       |6d6f64756c65 - [module (string)]
       |00000001 - [1 (int)]
       |00000004 - [4 (int)]
       |6e616d65 - [name (string)]
       |_Arg_
       |00000005 - [5 (int)]
       |68656c6c6f - [hello (string)]
       |_Signatories_
       |00000002 - [2 (int)]
       |00000005 - [5 (int)]
       |616c696365 - [alice (string)]
       |00000003 - [3 (int)]
       |626f62 - [bob (string)]
       |_Stakeholders_
       |00000002 - [2 (int)]
       |00000005 - [5 (int)]
       |616c696365 - [alice (string)]
       |00000007 - [7 (int)]
       |636861726c6965 - [charlie (string)]
       |_Global Keys_
       |00000001 - [1 (int)]
       |_Maintainers_
       |00000001 - [1 (int)]
       |00000005 - [5 (int)]
       |6461766964 - [david (string)]
       |_Template Id_
       |00000001 - [1 (int)]
       |0000000a - [10 (int)]
       |6d6f64756c655f6b6579 - [module_key (string)]
       |00000001 - [1 (int)]
       |00000004 - [4 (int)]
       |6e616d65 - [name (string)]
       |_Package Name_
       |00000010 - [16 (int)]
       |7061636b6167655f6e616d655f6b6579 - [package_name_key (string)]
       |_Key_
       |00000005 - [5 (int)]
       |68656c6c6f - [hello (string)]
       |0 - [0 (node_version)]
       |0 - [0 (value_version)]
       |7 - [7 (value_purpose)]
       |_Fetch Node_
       |_Contract Id_
       |00000021 - [33 (int)]
       |0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5 - [0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5 (contractId)]
       |_Package Name_
       |0000000e - [14 (int)]
       |7061636b6167652d6e616d652d30 - [package-name-0 (string)]
       |_Template Id_
       |00000007 - [7 (int)]
       |7061636b616765 - [package (string)]
       |00000001 - [1 (int)]
       |00000006 - [6 (int)]
       |6d6f64756c65 - [module (string)]
       |00000001 - [1 (int)]
       |00000004 - [4 (int)]
       |6e616d65 - [name (string)]
       |_Signatories_
       |00000001 - [1 (int)]
       |00000005 - [5 (int)]
       |616c696365 - [alice (string)]
       |_Stakeholders_
       |00000001 - [1 (int)]
       |00000007 - [7 (int)]
       |636861726c6965 - [charlie (string)]
       |_Acting Parties_
       |00000002 - [2 (int)]
       |00000005 - [5 (int)]
       |616c696365 - [alice (string)]
       |00000003 - [3 (int)]
       |626f62 - [bob (string)]
       |_Global Keys_
       |00000001 - [1 (int)]
       |_Maintainers_
       |00000001 - [1 (int)]
       |00000005 - [5 (int)]
       |6461766964 - [david (string)]
       |_Template Id_
       |00000001 - [1 (int)]
       |0000000a - [10 (int)]
       |6d6f64756c655f6b6579 - [module_key (string)]
       |00000001 - [1 (int)]
       |00000004 - [4 (int)]
       |6e616d65 - [name (string)]
       |_Package Name_
       |00000010 - [16 (int)]
       |7061636b6167655f6e616d655f6b6579 - [package_name_key (string)]
       |_Key_
       |00000005 - [5 (int)]
       |68656c6c6f - [hello (string)]
       |_By Key_
       |01 - [true]
       |_Interface Id_
       |00000001 - [1 (int)]
       |00000007 - [7 (int)]
       |7061636b616765 - [package (string)]
       |00000001 - [1 (int)]
       |00000010 - [16 (int)]
       |696e746572666163655f6d6f64756c65 - [interface_module (string)]
       |00000001 - [1 (int)]
       |0000000e - [14 (int)]
       |696e746572666163655f6e616d65 - [interface_name (string)]
       |0 - [0 (node_version)]
       |0 - [0 (value_version)]
       |7 - [7 (value_purpose)]
       |_Rollback Node_
       |_Children_
       |00000002 - [2 (int)]
       |0 - [0 (node_version)]
       |0 - [0 (value_version)]
       |7 - [7 (value_purpose)]
       |_LookupByKey Node_
       |_Package Name_
       |0000000e - [14 (int)]
       |7061636b6167652d6e616d652d30 - [package-name-0 (string)]
       |_Template Id_
       |00000007 - [7 (int)]
       |7061636b616765 - [package (string)]
       |00000001 - [1 (int)]
       |00000006 - [6 (int)]
       |6d6f64756c65 - [module (string)]
       |00000001 - [1 (int)]
       |00000004 - [4 (int)]
       |6e616d65 - [name (string)]
       |_Global Key_
       |_Maintainers_
       |00000001 - [1 (int)]
       |00000005 - [5 (int)]
       |6461766964 - [david (string)]
       |_Template Id_
       |00000001 - [1 (int)]
       |0000000a - [10 (int)]
       |6d6f64756c655f6b6579 - [module_key (string)]
       |00000001 - [1 (int)]
       |00000004 - [4 (int)]
       |6e616d65 - [name (string)]
       |_Package Name_
       |00000010 - [16 (int)]
       |7061636b6167655f6e616d655f6b6579 - [package_name_key (string)]
       |_Key_
       |00000005 - [5 (int)]
       |68656c6c6f - [hello (string)]
       |_Result_
       |00000001 - [1 (int)]
       |00000021 - [33 (int)]
       |0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5 - [0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5 (contractId)]
       |0 - [0 (node_version)]
       |0 - [0 (value_version)]
       |7 - [7 (value_purpose)]
       |_LookupByKey Node_
       |_Package Name_
       |0000000e - [14 (int)]
       |7061636b6167652d6e616d652d30 - [package-name-0 (string)]
       |_Template Id_
       |00000007 - [7 (int)]
       |7061636b616765 - [package (string)]
       |00000001 - [1 (int)]
       |00000006 - [6 (int)]
       |6d6f64756c65 - [module (string)]
       |00000001 - [1 (int)]
       |00000004 - [4 (int)]
       |6e616d65 - [name (string)]
       |_Global Key_
       |_Maintainers_
       |00000001 - [1 (int)]
       |00000005 - [5 (int)]
       |6461766964 - [david (string)]
       |_Template Id_
       |00000001 - [1 (int)]
       |0000000a - [10 (int)]
       |6d6f64756c655f6b6579 - [module_key (string)]
       |00000001 - [1 (int)]
       |00000004 - [4 (int)]
       |6e616d65 - [name (string)]
       |_Package Name_
       |00000010 - [16 (int)]
       |7061636b6167655f6e616d655f6b6579 - [package_name_key (string)]
       |_Key_
       |00000003 - [3 (int)]
       |627965 - [bye (string)]
       |_Result_
       |00000001 - [1 (int)]
       |00000021 - [33 (int)]
       |0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b - [0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b (contractId)]
       |""".stripMargin
    }
  }
}
