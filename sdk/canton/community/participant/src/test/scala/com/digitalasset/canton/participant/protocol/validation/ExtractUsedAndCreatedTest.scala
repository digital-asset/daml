// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.data.TransactionView
import com.digitalasset.canton.participant.protocol.LedgerEffectAbsolutizer
import com.digitalasset.canton.participant.protocol.LedgerEffectAbsolutizer.ViewAbsoluteLedgerEffect
import com.digitalasset.canton.participant.protocol.validation.ExtractUsedAndCreated.{
  CreatedContractPrep,
  InputContractPrep,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.ContractIdAbsolutizer.{
  ContractIdAbsolutizationDataV1,
  ContractIdAbsolutizationDataV2,
}
import com.digitalasset.canton.topology.transaction.{ParticipantAttributes, ParticipantPermission}
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext, LfPartyId}

class ExtractUsedAndCreatedTest extends BaseTestWordSpec with HasExecutionContext {

  private def buildUnderTest(
      hostedParties: Map[LfPartyId, Option[ParticipantAttributes]]
  ): ExtractUsedAndCreated =
    new ExtractUsedAndCreated(hostedParties, loggerFactory)

  private def viewEffectsFromRootViews(
      effectAbsolutizer: LedgerEffectAbsolutizer,
      rootViews: Seq[TransactionView],
  ): Seq[ViewAbsoluteLedgerEffect] = rootViews.flatMap(viewEffectsInPreOrder(effectAbsolutizer, _))

  private def viewEffectsInPreOrder(
      effectAbsolutizer: LedgerEffectAbsolutizer,
      view: TransactionView,
  ): Seq[ViewAbsoluteLedgerEffect] = {
    view.subviews.assertAllUnblinded(hash =>
      s"View ${view.viewHash} contains an unexpected blinded subview $hash"
    )
    tryEffectsFromView(effectAbsolutizer, view) +: view.subviews.unblindedElements.flatMap(
      viewEffectsInPreOrder(effectAbsolutizer, _)
    )
  }

  private def tryEffectsFromView(
      effectAbsolutizer: LedgerEffectAbsolutizer,
      v: TransactionView,
  ): ViewAbsoluteLedgerEffect = {
    val vpd = v.viewParticipantData.tryUnwrap
    val informees = v.viewCommonData.tryUnwrap.viewConfirmationParameters.informees
    effectAbsolutizer
      .absoluteViewEffects(vpd, informees)
      .valueOrFail(s"absolutizing view effects for view ${v.viewHash}")
  }

  forAll(CantonContractIdVersion.all) { cantonContractIdVersion =>
    s"For version $cantonContractIdVersion" should {

      val etf: ExampleTransactionFactory = new ExampleTransactionFactory()(
        cantonContractIdVersion = cantonContractIdVersion
      )

      val singleExercise = etf.SingleExercise(etf.deriveNodeSeed(1))
      val singleCreate = etf.SingleCreate(etf.deriveNodeSeed(1))

      val informeeParties: Set[LfPartyId] = singleCreate.signatories ++ singleCreate.observers

      def effectAbsolutizer(example: ExampleTransaction) = {
        val absolutizationData = etf.cantonContractIdVersion match {
          case _: CantonContractIdV1Version => ContractIdAbsolutizationDataV1
          case _: CantonContractIdV2Version =>
            ContractIdAbsolutizationDataV2(example.updateId, etf.ledgerTime)
        }
        val contractAbsolutizer = new ContractIdAbsolutizer(etf.cryptoOps, absolutizationData)
        val effectAbsolutizer = new LedgerEffectAbsolutizer(contractAbsolutizer)
        effectAbsolutizer
      }

      "ExtractUsedAndCreated" when {
        val relevantExamples = etf.standardHappyCases.filter(_.rootViews.nonEmpty)

        forEvery(relevantExamples) { example =>
          s"checking $example" must {

            "yield the correct result" in {
              val dataViews =
                viewEffectsFromRootViews(effectAbsolutizer(example), example.rootViews)
              val parties = ExtractUsedAndCreated.extractPartyIds(dataViews)
              val hostedParties =
                parties
                  .map(_ -> Some(ParticipantAttributes(ParticipantPermission.Observation)))
                  .toMap
              val sut = buildUnderTest(hostedParties)

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

          val absolutizer = effectAbsolutizer(singleExercise)
          val viewEffects = tryEffectsFromView(absolutizer, singleExercise.view0)
          val actual = underTestWithNoHostedParties.inputContractPrep(Seq(viewEffects))

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

          lazy val absolutizer = effectAbsolutizer(singleExercise)
          lazy val viewEffects = tryEffectsFromView(absolutizer, singleExercise.view0)
          lazy val serializedContract = singleExercise.used.head
          lazy val signatories = singleExercise.node.signatories
          lazy val observers = singleExercise.node.stakeholders -- signatories

          "identify potentially unknown contracts" in {
            val underTestOnlyOnboardingHostedParties = buildUnderTest(
              hostedParties = (signatories.map(_ -> None) ++ observers.map(
                _ -> Some(
                  ParticipantAttributes(ParticipantPermission.Confirmation, onboarding = true)
                )
              )).toMap
            )

            val actual = underTestOnlyOnboardingHostedParties.inputContractPrep(Seq(viewEffects))

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
                _ -> Some(
                  ParticipantAttributes(ParticipantPermission.Confirmation, onboarding = true)
                )
              )).toMap
            )

            val actual = underTestOnlyOnboardingHostedParties.inputContractPrep(Seq(viewEffects))

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

          val absolutizer = effectAbsolutizer(singleCreate)
          val viewEffects = tryEffectsFromView(absolutizer, singleCreate.view0)
          val actual = underTestWithNoHostedParties.createdContractPrep(Seq(viewEffects))

          val expected = CreatedContractPrep(
            createdContractsOfHostedInformees = Map.empty,
            witnessed = singleCreate.createdAbsolute,
          )

          actual shouldBe expected
        }

      }
    }
  }
}
