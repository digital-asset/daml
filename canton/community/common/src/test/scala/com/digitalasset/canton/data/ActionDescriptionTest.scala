// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.daml.lf.transaction.Util
import com.daml.lf.value.Value
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.ActionDescription.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.util.LfTransactionBuilder
import com.digitalasset.canton.util.LfTransactionBuilder.defaultTemplateId
import com.digitalasset.canton.version.RepresentativeProtocolVersion
import org.scalatest.wordspec.AnyWordSpec

class ActionDescriptionTest extends AnyWordSpec with BaseTest {

  private val unsuffixedId: LfContractId = ExampleTransactionFactory.unsuffixedId(10)
  private val suffixedId: LfContractId = ExampleTransactionFactory.suffixedId(0, 0)
  private val seed: LfHash = ExampleTransactionFactory.lfHash(5)
  private val testTxVersion: LfTransactionVersion = ExampleTransactionFactory.transactionVersion
  private val globalKey: LfGlobalKey =
    LfGlobalKey
      .build(
        templateId = LfTransactionBuilder.defaultTemplateId,
        key = Value.ValueInt64(10L),
        shared = Util.sharedKey(testTxVersion),
      )
      .value
  private val choiceName: LfChoiceName = LfChoiceName.assertFromString("choice")

  private val representativePV: RepresentativeProtocolVersion[ActionDescription.type] =
    ActionDescription.protocolVersionRepresentativeFor(testedProtocolVersion)

  "An action description" should {

    "reject creation" when {

      "the choice argument cannot be serialized" in {
        ExerciseActionDescription.create(
          suffixedId,
          templateId = Some(defaultTemplateId),
          choiceName,
          None,
          ExampleTransactionFactory.veryDeepValue,
          Set(ExampleTransactionFactory.submitter),
          byKey = true,
          seed,
          testTxVersion,
          failed = false,
          representativePV,
        ) shouldBe Left(
          InvalidActionDescription(
            "Failed to serialize chosen value: Provided Daml-LF value to encode exceeds maximum nesting level of 100"
          )
        )
      }

      "the key value cannot be serialized" in {
        LookupByKeyActionDescription.create(
          LfGlobalKey
            .build(
              LfTransactionBuilder.defaultTemplateId,
              ExampleTransactionFactory.veryDeepValue,
              Util.sharedKey(LfTransactionBuilder.defaultLanguageVersion),
            )
            .value,
          testTxVersion,
          representativePV,
        ) shouldBe Left(
          InvalidActionDescription(
            "Failed to serialize key: Provided Daml-LF value to encode exceeds maximum nesting level of 100"
          )
        )
      }

      "no seed is given when the node expects a seed" in {
        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory.createNode(suffixedId),
          None,
          testedProtocolVersion,
        ) shouldBe
          Left(InvalidActionDescription("No seed for a Create node given"))

        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory.exerciseNodeWithoutChildren(suffixedId),
          None,
          testedProtocolVersion,
        ) shouldBe
          Left(InvalidActionDescription("No seed for an Exercise node given"))
      }

      "a seed is given when the node does not expect one" in {
        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory.fetchNode(suffixedId),
          Some(seed),
          testedProtocolVersion,
        ) shouldBe
          Left(InvalidActionDescription("No seed should be given for a Fetch node"))

        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory
            .lookupByKeyNode(globalKey, maintainers = Set(ExampleTransactionFactory.observer)),
          Some(seed),
          testedProtocolVersion,
        ) shouldBe Left(InvalidActionDescription("No seed should be given for a LookupByKey node"))
      }

      "actors are not declared for a Fetch node" in {
        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory.fetchNode(suffixedId, actingParties = Set.empty),
          None,
          testedProtocolVersion,
        ) shouldBe Left(InvalidActionDescription("Fetch node without acting parties"))
      }
    }
  }
}
