// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.daml.lf.transaction.Util
import com.daml.lf.value.Value
import com.digitalasset.canton.data.ActionDescription.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.util.LfTransactionBuilder
import com.digitalasset.canton.util.LfTransactionBuilder.defaultTemplateId
import com.digitalasset.canton.version.{ProtocolVersion, RepresentativeProtocolVersion}
import com.digitalasset.canton.{BaseTest, LfInterfaceId}
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
    def tryCreateExerciseActionDescription(
        interfaceId: Option[LfInterfaceId],
        templateId: Option[LfTemplateId],
        protocolVersion: ProtocolVersion,
    ): ExerciseActionDescription =
      createExerciseActionDescription(interfaceId, templateId, protocolVersion).fold(
        err => throw err,
        identity,
      )

    def createExerciseActionDescription(
        interfaceId: Option[LfInterfaceId],
        templateId: Option[LfTemplateId],
        protocolVersion: ProtocolVersion,
    ): Either[InvalidActionDescription, ExerciseActionDescription] =
      ExerciseActionDescription.create(
        suffixedId,
        templateId,
        LfChoiceName.assertFromString("choice"),
        interfaceId,
        Value.ValueUnit,
        Set(ExampleTransactionFactory.submitter),
        byKey = false,
        seed,
        testTxVersion,
        failed = true,
        ActionDescription.protocolVersionRepresentativeFor(protocolVersion),
      )

    val fetchAction = FetchActionDescription(
      unsuffixedId,
      Set(ExampleTransactionFactory.signatory, ExampleTransactionFactory.observer),
      byKey = true,
      testTxVersion,
    )(representativePV)

    "deserialize to the same value (V0)" in {
      val tests = Seq(
        CreateActionDescription(unsuffixedId, seed, testTxVersion)(representativePV),
        tryCreateExerciseActionDescription(
          interfaceId = None,
          templateId = None,
          ProtocolVersion.v3,
        ),
        fetchAction,
        LookupByKeyActionDescription.tryCreate(globalKey, testTxVersion, representativePV),
      )

      forEvery(tests) { actionDescription =>
        ActionDescription.fromProtoV0(actionDescription.toProtoV0) shouldBe Right(actionDescription)
      }
    }

    "deserialize to the same value (V1)" in {
      val tests = Seq(
        CreateActionDescription(unsuffixedId, seed, testTxVersion)(representativePV),
        tryCreateExerciseActionDescription(
          Some(LfTransactionBuilder.defaultInterfaceId),
          templateId = None,
          ProtocolVersion.v4,
        ),
        fetchAction,
        LookupByKeyActionDescription.tryCreate(globalKey, testTxVersion, representativePV),
      )

      forEvery(tests) { actionDescription =>
        ActionDescription.fromProtoV1(actionDescription.toProtoV1) shouldBe Right(actionDescription)
      }
    }

    "deserialize to the same value (V2)" in {
      val tests = Seq(
        CreateActionDescription(unsuffixedId, seed, testTxVersion)(representativePV),
        tryCreateExerciseActionDescription(
          Some(LfTransactionBuilder.defaultInterfaceId),
          Some(LfTransactionBuilder.defaultTemplateId),
          ProtocolVersion.v5,
        ),
        fetchAction,
        LookupByKeyActionDescription.tryCreate(globalKey, testTxVersion, representativePV),
      )

      forEvery(tests) { actionDescription =>
        ActionDescription.fromProtoV2(actionDescription.toProtoV2) shouldBe Right(actionDescription)
      }
    }

    "reject creation" when {
      "interfaceId is set and the protocol version is too old" in {
        def create(
            protocolVersion: ProtocolVersion
        ): Either[InvalidActionDescription, ExerciseActionDescription] =
          createExerciseActionDescription(
            Some(LfTransactionBuilder.defaultInterfaceId),
            templateId = Option.when(protocolVersion >= ProtocolVersion.v5)(defaultTemplateId),
            protocolVersion,
          )

        val v3 = ProtocolVersion.v3
        create(v3) shouldBe Left(
          InvalidActionDescription(
            s"Protocol version is equivalent to $v3 but interface id is supported since protocol version ${ProtocolVersion.v4}"
          )
        )

        create(ProtocolVersion.v4).value shouldBe a[ExerciseActionDescription]
      }

      "templateId should only allowed after ProtocolVersion.v5" in {
        val create = createExerciseActionDescription(interfaceId = None, _, _)

        assert(create(None, ProtocolVersion.v4).isRight)
        assert(create(Some(defaultTemplateId), ProtocolVersion.v4).isLeft)
        assert(create(None, ProtocolVersion.v5).isRight)
        assert(create(Some(defaultTemplateId), ProtocolVersion.v5).isRight)
      }

      "the choice argument cannot be serialized" in {
        ExerciseActionDescription.create(
          suffixedId,
          templateId =
            Option.when(representativePV.representative >= ProtocolVersion.v5)(defaultTemplateId),
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
