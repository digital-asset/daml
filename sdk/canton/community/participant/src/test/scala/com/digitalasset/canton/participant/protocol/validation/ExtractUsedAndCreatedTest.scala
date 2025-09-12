// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.participant.protocol.validation.ExtractUsedAndCreated.{
  CreatedContractPrep,
  InputContractPrep,
  ViewData,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.transaction.{ParticipantAttributes, ParticipantPermission}
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext, LfPartyId}

class ExtractUsedAndCreatedTest extends BaseTestWordSpec with HasExecutionContext {

  val etf: ExampleTransactionFactory = new ExampleTransactionFactory()(
    // TODO(#23971) Make this work with Contract ID V2
    cantonContractIdVersion = AuthenticatedContractIdVersionV11
  )

  private val singleExercise = etf.SingleExercise(etf.deriveNodeSeed(1))
  private val singleCreate = etf.SingleCreate(etf.deriveNodeSeed(1))

  private val informeeParties: Set[LfPartyId] = singleCreate.signatories ++ singleCreate.observers

  private def buildUnderTest(
      hostedParties: Map[LfPartyId, Option[ParticipantAttributes]]
  ): ExtractUsedAndCreated =
    new ExtractUsedAndCreated(hostedParties, loggerFactory)

  s"ExtractUsedAndCreated for version $AuthenticatedContractIdVersionV11" when {
    val relevantExamples = etf.standardHappyCases.filter(_.rootViews.nonEmpty)

    forEvery(relevantExamples) { example =>
      s"checking $example" must {

        val dataViews = ExtractUsedAndCreated.viewDataFromRootViews(example.rootViews)
        val parties = ExtractUsedAndCreated.extractPartyIds(dataViews)
        val hostedParties =
          parties.map(_ -> Some(ParticipantAttributes(ParticipantPermission.Observation))).toMap
        val sut = buildUnderTest(hostedParties)

        "yield the correct result" in {
          val expected = example.usedAndCreated
          val usedAndCreated = sut.usedAndCreated(dataViews)
          usedAndCreated.contracts shouldBe expected
          usedAndCreated.hostedWitnesses shouldBe hostedParties.keySet
        }
      }
    }
  }

  "Input contract prep" should {

    "Extract divulged contracts" in {

      val underTestWithNoHostedParties = buildUnderTest(
        hostedParties = informeeParties.map(_ -> None).toMap
      )

      val viewData = ViewData.tryFromView(singleExercise.view0)
      val actual = underTestWithNoHostedParties.inputContractPrep(Seq(viewData))

      val serializedContract = singleExercise.used.head

      val expected = InputContractPrep(
        used = Map(singleExercise.absolutizedContractId -> serializedContract),
        divulged = Map(singleExercise.absolutizedContractId -> serializedContract),
        consumedOfHostedStakeholders = Map.empty,
        contractIdsOfHostedInformeeStakeholder = Set.empty,
        contractIdsAllowedToBeUnknown = Set.empty,
      )

      actual shouldBe expected
    }

    "Onboarding" should {

      val viewData = ViewData.tryFromView(singleExercise.view0)
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
          used = Map(singleExercise.absolutizedContractId -> serializedContract),
          divulged = Map.empty,
          consumedOfHostedStakeholders =
            Map(singleExercise.absolutizedContractId -> informeeParties),
          contractIdsOfHostedInformeeStakeholder = Set(singleExercise.absolutizedContractId),
          contractIdsAllowedToBeUnknown = Set(singleExercise.absolutizedContractId),
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
          used = Map(singleExercise.absolutizedContractId -> serializedContract),
          divulged = Map.empty,
          consumedOfHostedStakeholders =
            Map(singleExercise.absolutizedContractId -> informeeParties),
          contractIdsOfHostedInformeeStakeholder = Set(singleExercise.absolutizedContractId),
          contractIdsAllowedToBeUnknown = Set.empty,
        )

        actual shouldBe expected
      }
    }
  }

  "Created contract prep" should {

    "Extract witnessed contracts" in {

      val underTestWithNoHostedParties = buildUnderTest(
        hostedParties = informeeParties.map(_ -> None).toMap
      )

      val viewData = ViewData.tryFromView(singleCreate.view0)
      val actual = underTestWithNoHostedParties.createdContractPrep(Seq(viewData))

      val expected = CreatedContractPrep(
        createdContractsOfHostedInformees = Map.empty,
        witnessed = Map(singleCreate.absolutizedContractId -> singleCreate.created.head),
      )

      actual shouldBe expected
    }

  }
}
