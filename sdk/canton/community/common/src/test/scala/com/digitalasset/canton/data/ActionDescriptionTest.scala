// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.data.ActionDescription.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.util.LfTransactionBuilder
import com.digitalasset.canton.util.LfTransactionBuilder.{defaultPackageId, defaultTemplateId}
import com.digitalasset.canton.version.RepresentativeProtocolVersion
import com.digitalasset.canton.{BaseTest, LfPackageName, LfPartyId, LfVersioned}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value
import org.scalatest.wordspec.AnyWordSpec

class ActionDescriptionTest extends AnyWordSpec with BaseTest {

  private val suffixedId: LfContractId = ExampleTransactionFactory.suffixedId(0, 0)
  private val seed: LfHash = ExampleTransactionFactory.lfHash(5)
  private val globalKey: LfGlobalKey =
    LfGlobalKey
      .build(
        LfTransactionBuilder.defaultTemplateId,
        Value.ValueInt64(10L),
        LfPackageName.assertFromString("package-name"),
      )
      .value
  private val choiceName: LfChoiceName = LfChoiceName.assertFromString("choice")

  private val representativePV: RepresentativeProtocolVersion[ActionDescription.type] =
    ActionDescription.protocolVersionRepresentativeFor(testedProtocolVersion)

  "An action description" should {

    "accept creation" when {

      "a valid fetch node is presented" in {

        val targetTemplateId =
          Ref.Identifier(defaultPackageId, defaultTemplateId.qualifiedName)

        val actingParties = Set(LfPartyId.assertFromString("acting"))

        val node = ExampleTransactionFactory.fetchNode(
          cid = suffixedId,
          templateId = targetTemplateId,
          actingParties = Set(LfPartyId.assertFromString("acting")),
        )

        val expected = FetchActionDescription(
          inputContractId = suffixedId,
          actors = actingParties,
          byKey = false,
          templateId = targetTemplateId,
          interfaceId = None,
        )(protocolVersionRepresentativeFor(testedProtocolVersion))

        ActionDescription.fromLfActionNode(
          node,
          None,
          Set.empty,
          testedProtocolVersion,
        ) shouldBe
          Right(expected)
      }

    }

    "reject creation" when {
      "the choice argument cannot be serialized" in {
        ExerciseActionDescription.create(
          suffixedId,
          templateId = defaultTemplateId,
          choiceName,
          None,
          Set.empty,
          ExampleTransactionFactory.veryDeepVersionedValue,
          Set(ExampleTransactionFactory.submitter),
          byKey = true,
          seed,
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
          LfVersioned(
            ExampleTransactionFactory.transactionVersion,
            LfGlobalKey
              .build(
                LfTransactionBuilder.defaultTemplateId,
                ExampleTransactionFactory.veryDeepValue,
                ExampleTransactionFactory.packageName,
              )
              .value,
          ),
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
          Set.empty,
          testedProtocolVersion,
        ) shouldBe
          Left(InvalidActionDescription("No seed for a Create node given"))

        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory.exerciseNodeWithoutChildren(suffixedId),
          None,
          Set.empty,
          testedProtocolVersion,
        ) shouldBe
          Left(InvalidActionDescription("No seed for an Exercise node given"))
      }

      "a seed is given when the node does not expect one" in {
        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory.fetchNode(suffixedId),
          Some(seed),
          Set.empty,
          testedProtocolVersion,
        ) shouldBe
          Left(InvalidActionDescription("No seed should be given for a Fetch node"))

        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory
            .lookupByKeyNode(globalKey, maintainers = Set(ExampleTransactionFactory.observer)),
          Some(seed),
          Set.empty,
          testedProtocolVersion,
        ) shouldBe Left(InvalidActionDescription("No seed should be given for a LookupByKey node"))
      }

      "actors are not declared for a Fetch node" in {
        ActionDescription.fromLfActionNode(
          ExampleTransactionFactory.fetchNode(suffixedId, actingParties = Set.empty),
          None,
          Set.empty,
          testedProtocolVersion,
        ) shouldBe Left(InvalidActionDescription("Fetch node without acting parties"))
      }
    }
  }
}
