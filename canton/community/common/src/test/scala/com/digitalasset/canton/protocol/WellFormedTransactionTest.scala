// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.value.Value
import com.digitalasset.canton.protocol.ExampleTransactionFactory.*
import com.digitalasset.canton.protocol.WellFormedTransaction.{State, WithSuffixes, WithoutSuffixes}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPartyId}
import org.scalatest.prop.{TableFor3, TableFor4}
import org.scalatest.wordspec.AnyWordSpec

class WellFormedTransactionTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  val factory: ExampleTransactionFactory = new ExampleTransactionFactory()()

  val lfAbs: LfContractId = suffixedId(0, 0)

  val contractInst = contractInstance()
  val serContractInst = asSerializableRaw(contractInst)

  def createNode(
      cid: LfContractId,
      contractInstance: LfContractInst = ExampleTransactionFactory.contractInstance(),
      signatories: Set[LfPartyId] = Set(signatory),
      key: Option[LfGlobalKeyWithMaintainers] = None,
  ): LfNodeCreate =
    ExampleTransactionFactory.createNode(
      cid,
      signatories = signatories,
      contractInstance = contractInstance,
      key = key,
    )

  def fetchNode(cid: LfContractId): LfNodeFetch =
    ExampleTransactionFactory.fetchNode(
      cid,
      actingParties = Set(submitter),
      signatories = Set(signatory),
    )

  def exerciseNode(cid: LfContractId, child: Int): LfNodeExercises =
    ExampleTransactionFactory.exerciseNode(
      cid,
      children = List(nodeId(child)),
      signatories = Set(signatory),
    )

  private def nid(i: Int): String = """NodeId\(""" + i.toString + """.*\)"""

  val malformedExamples
      : TableFor4[String, (LfVersionedTransaction, TransactionMetadata), State, String] =
    Table(
      ("Description", "Transaction and seeds", "Suffixing", "Expected Error Message"),
      (
        "Orphaned node",
        factory.versionedTransactionWithSeeds(Seq.empty, fetchNode(lfAbs)),
        WithSuffixes,
        "OrphanedNode: 0",
      ),
      (
        "Dangling root node",
        factory.versionedTransactionWithSeeds(Seq(0)),
        WithSuffixes,
        "DanglingNodeId: 0",
      ),
      (
        "Dangling exercise child",
        factory.versionedTransactionWithSeeds(Seq(0), exerciseNode(lfAbs, child = 1)),
        WithSuffixes,
        "DanglingNodeId: 1",
      ),
      (
        "Cycle",
        factory.versionedTransactionWithSeeds(Seq(0), exerciseNode(lfAbs, child = 0)),
        WithSuffixes,
        "AliasedNode: 0",
      ),
      (
        "Two parents",
        factory.versionedTransactionWithSeeds(Seq(0, 0), fetchNode(lfAbs)),
        WithSuffixes,
        "AliasedNode: 0",
      ),
      (
        "Negative node ID",
        transactionFrom(Seq(-1), -1, fetchNode(lfAbs)) -> factory.mkMetadata(),
        WithoutSuffixes,
        "Negative node IDs: -1",
      ),
      (
        "byKey node with no key",
        factory.versionedTransactionWithSeeds(
          Seq(0),
          ExampleTransactionFactory.fetchNode(
            unsuffixedId(0),
            actingParties = Set(submitter),
            signatories = Set(signatory),
            byKey = true,
          ),
        ),
        WithoutSuffixes,
        "byKey nodes without a key: 0",
      ),
      (
        "Missing seed for create",
        (transaction(Seq(0), createNode(unsuffixedId(0))), factory.mkMetadata()),
        WithoutSuffixes,
        "Nodes without seeds: 0",
      ),
      (
        "Missing seed for exercise",
        (
          transaction(
            Seq(0),
            ExampleTransactionFactory.exerciseNode(lfAbs, signatories = Set(signatory)),
          ),
          factory.mkMetadata(),
        ),
        WithoutSuffixes,
        "Nodes without seeds: 0",
      ),
      (
        "Superfluous seed for fetch",
        (
          transaction(Seq(0), fetchNode(lfAbs)),
          factory.mkMetadata(Map(LfNodeId(0) -> ExampleTransactionFactory.lfHash(0))),
        ),
        WithSuffixes,
        "Nodes with superfluous seeds: 0",
      ),
      (
        "Duplicate create",
        factory.versionedTransactionWithSeeds(
          Seq(0, 1),
          createNode(unsuffixedId(0)),
          createNode(unsuffixedId(0)),
        ),
        WithoutSuffixes,
        s"Contract id ${unsuffixedId(0).coid} is created in nodes ${nid(0)} and ${nid(1)}",
      ),
      (
        "Create shadows previously referenced id",
        factory.versionedTransactionWithSeeds(Seq(0, 1), fetchNode(lfAbs), createNode(lfAbs)),
        WithSuffixes,
        s"Contract id 0000000000000000000000000000000000000000000000000000000000000000000000 created in node ${nid(1)} is referenced before in ${nid(0)}",
      ),
      (
        "Unsuffixed discriminator appears with suffix in value",
        factory.versionedTransactionWithSeeds(
          Seq(0),
          createNode(
            unsuffixedId(0),
            contractInstance = contractInstance(capturedIds = Seq(suffixedId(0, 1))),
          ),
        ),
        WithoutSuffixes,
        s"Contract discriminator 0000000000000000000000000000000000000000000000000000000000000000 created in ${nid(0)} is not fresh due to ${nid(0)}",
      ),
      (
        "Unsuffixed discriminator is used earlier with suffix",
        factory.versionedTransactionWithSeeds(
          Seq(0, 1),
          fetchNode(suffixedId(0, 1)),
          createNode(unsuffixedId(0)),
        ),
        WithoutSuffixes,
        s"Contract discriminator 0000000000000000000000000000000000000000000000000000000000000000 created in ${nid(1)} is not fresh due to ${nid(0)}",
      ),
      (
        "Unsuffixed discriminator is used with suffix in later node",
        factory.versionedTransactionWithSeeds(
          Seq(0, 1),
          createNode(unsuffixedId(1)),
          exerciseNode(suffixedId(0, 1), 2),
          fetchNode(suffixedId(1, -1)),
        ),
        WithoutSuffixes,
        s"Contract discriminator 0001000000000000000000000000000000000000000000000000000000000000 created in ${nid(0)} is not fresh due to contract Id 000001000000000000000000000000000000000000000000000000000000000000ffffffff in ${nid(2)}",
      ),
      (
        "Unsuffixed discriminator is referenced with suffix in later node",
        factory.versionedTransactionWithSeeds(
          Seq(0, 1),
          createNode(unsuffixedId(1)),
          ExampleTransactionFactory.exerciseNode(
            suffixedId(0, 0),
            signatories = Set(signatory),
            actingParties = Set(signatory),
            exerciseResult =
              Some(contractInstance(capturedIds = Seq(suffixedId(1, -1))).unversioned.arg),
          ),
        ),
        WithoutSuffixes,
        s"Contract discriminator 0001000000000000000000000000000000000000000000000000000000000000 created in ${nid(0)} is not fresh due to contract Id 000001000000000000000000000000000000000000000000000000000000000000ffffffff in ${nid(1)}",
      ),
      (
        "Missing signatory",
        factory.versionedTransactionWithSeeds(
          Seq(0),
          createNode(unsuffixedId(0)).copy(signatories = Set.empty),
        ),
        WithoutSuffixes,
        "neither signatories nor maintainers present at nodes 0",
      ),
      (
        "Signatory not declared as informee",
        factory.versionedTransactionWithSeeds(
          Seq(0),
          createNode(unsuffixedId(0)).copy(stakeholders = Set.empty),
        ),
        WithoutSuffixes,
        "signatory or maintainer not declared as informee: signatory::default at node 0",
      ),
      (
        "Missing fetch actors",
        factory
          .versionedTransactionWithSeeds(Seq(0), fetchNode(lfAbs).copy(actingParties = Set.empty)),
        WithSuffixes,
        "fetch nodes with unspecified acting parties at nodes 0",
      ),
      (
        "Failure to serialize - depth limit exceeded",
        factory.versionedTransactionWithSeeds(
          Seq(0, 1),
          createNode(unsuffixedId(0), contractInstance = veryDeepContractInstance),
          LfNodeExercises(
            targetCoid = suffixedId(2, -1),
            templateId = templateId,
            packageName = Ref.PackageName.assertFromString("default"),
            interfaceId = None,
            choiceId = LfChoiceName.assertFromString("choice"),
            consuming = false,
            actingParties = Set(ExampleTransactionFactory.submitter),
            chosenValue = ExampleTransactionFactory.veryDeepValue,
            stakeholders = Set(ExampleTransactionFactory.submitter),
            signatories = Set(ExampleTransactionFactory.submitter),
            choiceObservers = Set.empty,
            choiceAuthorizers = None,
            children = ImmArray.empty,
            exerciseResult = None,
            keyOpt = None,
            byKey = false,
            version = ExampleTransactionFactory.transactionVersion,
          ),
        ),
        WithoutSuffixes,
        List(
          """unable to serialize contract instance in node 0: """ +
            s"""Provided Daml-LF value to encode exceeds maximum nesting level of ${Value.MAXIMUM_NESTING}""",
          """unable to serialize chosen value in node 1: """ +
            s"""Provided Daml-LF value to encode exceeds maximum nesting level of ${Value.MAXIMUM_NESTING}""",
        ).sorted.mkString(", "),
      ),
      (
        "Failure to parse party id",
        factory.versionedTransactionWithSeeds(
          Seq(0),
          createNode(unsuffixedId(0), signatories = Set(LfPartyId.assertFromString("bubu"))),
        ),
        WithoutSuffixes,
        """Unable to parse party: Invalid unique identifier `bubu` .*""",
      ),
      (
        "Empty maintainers",
        factory.versionedTransactionWithSeeds(
          Seq(0, 1),
          createNode(
            unsuffixedId(1),
            signatories = Set(signatory),
            key = Some(
              LfGlobalKeyWithMaintainers
                .assertBuild(templateId, contractInst.unversioned.arg, Set.empty)
            ),
          ),
          ExampleTransactionFactory.exerciseNode(
            lfAbs,
            signatories = Set(signatory),
            actingParties = Set(signatory),
            key = Some(
              LfGlobalKeyWithMaintainers.assertBuild(
                templateId,
                contractInst.unversioned.arg,
                Set.empty,
              )
            ),
          ),
        ),
        WithoutSuffixes,
        s"Key of node 0 has no maintainer, Key of node 1 has no maintainer",
      ),
    )

  // Well-formed transactions are mostly covered by ExampleTransactionFactoryTest. So we test only a special cases here.
  val wellformedExamples: TableFor3[String, (LfVersionedTransaction, TransactionMetadata), State] =
    Table(
      ("Description", "Transaction and seeds", "Suffixing"),
      (
        "Suffixed discriminators need not be fresh",
        factory.versionedTransactionWithSeeds(
          Seq(0, 1, 2),
          fetchNode(suffixedId(0, -1)),
          createNode(suffixedId(0, 0)),
          fetchNode(unsuffixedId(0)),
        ),
        WithSuffixes,
      ),
    )

  "A transaction" when {
    malformedExamples.forEvery {
      case (description, (transaction, metadata), state, expectedError) =>
        description must {
          "be reported as malformed" in {
            WellFormedTransaction
              .normalizeAndCheck(transaction, metadata, state)
              .left
              .value should fullyMatch regex expectedError
            an[IllegalArgumentException] must be thrownBy
              WellFormedTransaction.normalizeAndAssert(transaction, metadata, WithoutSuffixes)
          }
        }
    }

    wellformedExamples.forEvery { case (description, (transaction, metadata), state) =>
      description must {
        "be accepted as well-formed" in {
          WellFormedTransaction
            .normalizeAndCheck(transaction, metadata, state)
            .value shouldBe a[WellFormedTransaction[_]]
        }
      }
    }
  }
}
