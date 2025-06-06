// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.participant.protocol.validation.ExtractUsedAndCreated.{
  CreatedContractPrep,
  InputContractPrep,
  ViewData,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.transaction.{ParticipantAttributes, ParticipantPermission}
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext, LfPartyId}

class ExtractUsedAndCreatedTest extends BaseTestWordSpec with HasExecutionContext {

  val etf: ExampleTransactionFactory = new ExampleTransactionFactory()()

  private val emptyUsedAndCreatedContracts = UsedAndCreatedContracts(
    witnessed = Map.empty[LfContractId, SerializableContract],
    checkActivenessTxInputs = Set.empty[LfContractId],
    consumedInputsOfHostedStakeholders = Map.empty[LfContractId, Set[LfPartyId]],
    used = Map.empty[LfContractId, SerializableContract],
    maybeCreated = Map.empty[LfContractId, Option[SerializableContract]],
    transient = Map.empty[LfContractId, Set[LfPartyId]],
    maybeUnknown = Set.empty[LfContractId],
  )

  private val singleExercise = etf.SingleExercise(etf.deriveNodeSeed(1))
  private val singleCreate = etf.SingleCreate(etf.deriveNodeSeed(1))

  private val informeeParties: Set[LfPartyId] = singleCreate.signatories ++ singleCreate.observers

  private def buildUnderTest(
      hostedParties: Map[LfPartyId, Option[ParticipantAttributes]]
  ): ExtractUsedAndCreated =
    new ExtractUsedAndCreated(
      hostedParties = hostedParties,
      loggerFactory = loggerFactory,
    )

  private val underTest = buildUnderTest(
    hostedParties =
      informeeParties.map(_ -> Some(ParticipantAttributes(ParticipantPermission.Observation))).toMap
  )

  "Build used and created" should {

    val tree = etf.rootTransactionViewTree(singleCreate.view0)
    val transactionViewTrees = NonEmpty(Seq, (tree, Option.empty[Signature]))
    val transactionViews = transactionViewTrees.map { case (viewTree, _) => viewTree.view }

    val actual = underTest.usedAndCreated(transactionViews)

    val expected = UsedAndCreated(
      contracts = emptyUsedAndCreatedContracts.copy(maybeCreated =
        Map(singleCreate.contractId -> singleCreate.created.headOption)
      ),
      hostedWitnesses = informeeParties,
    )

    "match" in {
      actual shouldBe expected
    }
  }

  "Input contract prep" should {

    "Extract input contracts" in {

      val viewData = ViewData(
        singleExercise.view0.viewParticipantData.tryUnwrap,
        singleExercise.view0.viewCommonData.tryUnwrap,
      )

      val actual = underTest.inputContractPrep(Seq(viewData))

      val serializedContract = singleExercise.used.head
      val expected = InputContractPrep(
        used = Map(singleExercise.contractId -> serializedContract),
        divulged = Map.empty,
        consumedOfHostedStakeholders = Map(
          singleExercise.contractId ->
            informeeParties
        ),
        contractIdsOfHostedInformeeStakeholder = Set(singleExercise.contractId),
        contractIdsAllowedToBeUnknown = Set.empty,
      )

      actual shouldBe expected
    }

    "Extract divulged contracts" in {

      val underTestWithNoHostedParties = buildUnderTest(
        hostedParties = informeeParties.map(_ -> None).toMap
      )

      val viewData = ViewData(
        singleExercise.view0.viewParticipantData.tryUnwrap,
        singleExercise.view0.viewCommonData.tryUnwrap,
      )

      val actual = underTestWithNoHostedParties.inputContractPrep(Seq(viewData))

      val serializedContract = singleExercise.used.head

      val expected = InputContractPrep(
        used = Map(singleExercise.contractId -> serializedContract),
        divulged = Map(singleExercise.contractId -> serializedContract),
        consumedOfHostedStakeholders = Map.empty,
        contractIdsOfHostedInformeeStakeholder = Set.empty,
        contractIdsAllowedToBeUnknown = Set.empty,
      )

      actual shouldBe expected
    }

    "Onboarding" should {

      val viewData = ViewData(
        singleExercise.view0.viewParticipantData.tryUnwrap,
        singleExercise.view0.viewCommonData.tryUnwrap,
      )
      val serializedContract = singleExercise.used.head
      val signatories = singleExercise.node.signatories
      val observers = singleExercise.node.stakeholders -- signatories

      "identify potentially unknown contracts" in {
        val underTestOnlyOnboardingHostedParties = buildUnderTest(
          hostedParties = (signatories.map(_ -> None) ++ observers.map(
            _ -> Some(ParticipantAttributes(ParticipantPermission.Confirmation, onboarding = true))
          )).toMap
        )

        val actual = underTestOnlyOnboardingHostedParties.inputContractPrep(Seq(viewData))

        val expected = InputContractPrep(
          used = Map(singleExercise.contractId -> serializedContract),
          divulged = Map.empty,
          consumedOfHostedStakeholders = Map(singleExercise.contractId -> informeeParties),
          contractIdsOfHostedInformeeStakeholder = Set(singleExercise.contractId),
          contractIdsAllowedToBeUnknown = Set(singleExercise.contractId),
        )

        actual shouldBe expected
      }

      "not mark unknown contracts if not all hosted stakeholders onboarding" in {
        val underTestOnlyOnboardingHostedParties = buildUnderTest(
          hostedParties = (signatories.map(
            _ -> Some(ParticipantAttributes(ParticipantPermission.Observation))
          ) ++ observers.map(
            _ -> Some(ParticipantAttributes(ParticipantPermission.Confirmation, onboarding = true))
          )).toMap
        )

        val actual = underTestOnlyOnboardingHostedParties.inputContractPrep(Seq(viewData))

        val expected = InputContractPrep(
          used = Map(singleExercise.contractId -> serializedContract),
          divulged = Map.empty,
          consumedOfHostedStakeholders = Map(singleExercise.contractId -> informeeParties),
          contractIdsOfHostedInformeeStakeholder = Set(singleExercise.contractId),
          contractIdsAllowedToBeUnknown = Set.empty,
        )

        actual shouldBe expected
      }
    }
  }

  "Created contract prep" should {

    "Extract created contracts" in {

      val viewData = ViewData(
        singleCreate.view0.viewParticipantData.tryUnwrap,
        singleCreate.view0.viewCommonData.tryUnwrap,
      )

      val actual = underTest.createdContractPrep(Seq(viewData))

      val expected = CreatedContractPrep(
        createdContractsOfHostedInformees =
          Map(singleCreate.contractId -> singleCreate.created.headOption),
        witnessed = Map.empty,
      )

      actual shouldBe expected
    }

    "Extract witnessed contracts" in {

      val underTestWithNoHostedParties = buildUnderTest(
        hostedParties = informeeParties.map(_ -> None).toMap
      )

      val viewData = ViewData(
        singleCreate.view0.viewParticipantData.tryUnwrap,
        singleCreate.view0.viewCommonData.tryUnwrap,
      )

      val actual = underTestWithNoHostedParties.createdContractPrep(Seq(viewData))

      val expected = CreatedContractPrep(
        createdContractsOfHostedInformees = Map.empty,
        witnessed = Map(singleCreate.contractId -> singleCreate.created.head),
      )

      actual shouldBe expected
    }

  }

  "Transient contract prep" should {

    "Extract transient contract ids" in {

      val viewCreatedConsumed = etf.view(
        node = singleCreate.node,
        viewIndex = 0,
        consumed = singleCreate.created.map(_.contractId).toSet,
        coreInputs = singleCreate.used,
        created = singleCreate.created,
        resolvedKeys = Map.empty,
        seed = singleCreate.nodeSeed,
        isRoot = true,
        packagePreference = Set.empty,
      )

      val viewData = ViewData(
        viewCreatedConsumed.viewParticipantData.tryUnwrap,
        viewCreatedConsumed.viewCommonData.tryUnwrap,
      )

      val actual = underTest.transientContractsPrep(Seq(viewData))

      val expected = Set(singleCreate.contractId)

      actual shouldBe expected
    }
  }
}
