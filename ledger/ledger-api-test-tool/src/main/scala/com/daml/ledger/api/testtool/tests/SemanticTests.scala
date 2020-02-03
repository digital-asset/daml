// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.ledger.api.testtool.infrastructure.Synchronize.synchronize
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import com.digitalasset.ledger.api.v1.value.{Record, RecordField, Value}
import com.digitalasset.ledger.client.binding.Primitive
import com.digitalasset.ledger.test.SemanticTests.Delegation._
import com.digitalasset.ledger.test.SemanticTests.FetchIou._
import com.digitalasset.ledger.test.SemanticTests.FetchPaintAgree._
import com.digitalasset.ledger.test.SemanticTests.FetchPaintOffer._
import com.digitalasset.ledger.test.SemanticTests.Iou._
import com.digitalasset.ledger.test.SemanticTests.PaintCounterOffer._
import com.digitalasset.ledger.test.SemanticTests.PaintOffer._
import com.digitalasset.ledger.test.SemanticTests.SharedContract._
import com.digitalasset.ledger.test.SemanticTests._
import io.grpc.Status
import scalaz.Tag

import scala.concurrent.Future

final class SemanticTests(session: LedgerSession) extends LedgerTestSuite(session) {
  private[this] val onePound = Amount(BigDecimal(1), "GBP")
  private[this] val twoPounds = Amount(BigDecimal(2), "GBP")

  /*
   * Consistency
   *
   * A transaction is internally consistent for a contract
   * `c` if the following hold for all its subactions `act`
   * on the contract `c`:
   *
   * 1. `act` does not happen before any Create c action (correct by construction in DAML)
   * 2. `act` does not happen after the contract has been consumend (i.e. no double spending)
   */

  test(
    "SemanticDoubleSpend",
    "Cannot double spend across transactions",
    allocate(TwoParties, TwoParties),
  ) {
    case Participants(
        Participant(alpha, payer, owner),
        Participant(_, newOwner, leftWithNothing),
        ) =>
      for {
        iou <- alpha.create(payer, Iou(payer, owner, onePound))
        _ <- alpha.exercise(owner, iou.exerciseTransfer(_, newOwner))
        failure <- alpha.exercise(owner, iou.exerciseTransfer(_, leftWithNothing)).failed
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "couldn't find contract")
      }
  }

  test(
    "SemanticDoubleSpendSameTx",
    "Cannot double spend within a transaction",
    allocate(TwoParties, TwoParties),
  ) {
    case Participants(Participant(alpha, payer, owner), Participant(_, newOwner1, newOwner2)) =>
      for {
        iou <- alpha.create(payer, Iou(payer, owner, onePound))
        doubleSpend <- alpha.submitAndWaitRequest(
          owner,
          iou.exerciseTransfer(owner, newOwner1).command,
          iou.exerciseTransfer(owner, newOwner2).command,
        )
        failure <- alpha.submitAndWait(doubleSpend).failed
      } yield {
        assertGrpcError(
          failure,
          Status.Code.INVALID_ARGUMENT,
          "Update failed due to fetch of an inactive contract",
        )
      }
  }

  test(
    "SemanticDoubleSpendShared",
    "Different parties cannot spend the same contract",
    allocate(TwoParties, SingleParty),
  ) {
    case Participants(Participant(alpha, payer, owner1), Participant(beta, owner2)) =>
      for {
        shared <- alpha.create(payer, SharedContract(payer, owner1, owner2))
        _ <- alpha.exercise(owner1, shared.exerciseSharedContract_Consume1)
        _ <- synchronize(alpha, beta)
        failure <- beta.exercise(owner2, shared.exerciseSharedContract_Consume2).failed
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "couldn't find contract")
      }
  }

  /*
   * Authorization
   *
   * A commit is well-authorized if every subaction `act` of the commit is
   * authorized by at least all of the required authorizers to `act`, where:
   *
   * 1. the required authorizers of a Create action on a contract `c` are the signatories of `c`
   * 2. the required authorizers of an Exercise or a Fetch action are its actors
   */

  test(
    "SemanticPaintOffer",
    "Conduct the paint offer workflow successfully",
    allocate(TwoParties, SingleParty),
  ) {
    case Participants(Participant(alpha, bank, houseOwner), Participant(beta, painter)) =>
      for {
        iou <- alpha.create(bank, Iou(bank, houseOwner, onePound))
        offer <- beta.create(painter, PaintOffer(painter, houseOwner, bank, onePound))
        tree <- eventually { alpha.exercise(houseOwner, offer.exercisePaintOffer_Accept(_, iou)) }
      } yield {
        val agreement = assertSingleton(
          "SemanticPaintOffer",
          createdEvents(tree).filter(_.getTemplateId == Tag.unwrap(PaintAgree.id)),
        )
        assertEquals(
          "Paint agreement parameters",
          agreement.getCreateArguments,
          Record(
            recordId = Some(Tag.unwrap(PaintAgree.id)),
            fields = Seq(
              RecordField("painter", Some(Value(Value.Sum.Party(Tag.unwrap(painter))))),
              RecordField("houseOwner", Some(Value(Value.Sum.Party(Tag.unwrap(houseOwner))))),
            ),
          ),
        )
      }
  }

  test(
    "SemanticPaintCounterOffer",
    "Conduct the paint counter-offer worflow successfully",
    allocate(TwoParties, SingleParty),
  ) {
    case Participants(Participant(alpha, bank, houseOwner), Participant(beta, painter)) =>
      for {
        iou <- alpha.create(bank, Iou(bank, houseOwner, onePound))
        offer <- beta.create(painter, PaintOffer(painter, houseOwner, bank, twoPounds))
        counter <- eventually {
          alpha.exerciseAndGetContract[PaintCounterOffer](
            houseOwner,
            offer.exercisePaintOffer_Counter(_, iou),
          )
        }
        tree <- eventually { beta.exercise(painter, counter.exercisePaintCounterOffer_Accept) }
      } yield {
        val agreement = assertSingleton(
          "SemanticPaintCounterOffer",
          createdEvents(tree).filter(_.getTemplateId == Tag.unwrap(PaintAgree.id)),
        )
        assertEquals(
          "Paint agreement parameters",
          agreement.getCreateArguments,
          Record(
            recordId = Some(Tag.unwrap(PaintAgree.id)),
            fields = Seq(
              RecordField("painter", Some(Value(Value.Sum.Party(Tag.unwrap(painter))))),
              RecordField("houseOwner", Some(Value(Value.Sum.Party(Tag.unwrap(houseOwner))))),
            ),
          ),
        )
      }
  }

  test(
    "SemanticPartialSignatories",
    "A signatory should not be able to create a contract on behalf of two parties",
    allocate(SingleParty, SingleParty),
  ) {
    case Participants(Participant(alpha, houseOwner), Participant(_, painter)) =>
      for {
        failure <- alpha.create(houseOwner, PaintAgree(painter, houseOwner)).failed
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "requires authorizers")
      }
  }

  test(
    "SemanticAcceptOnBehalf",
    "It should not be possible to exercise a choice without the consent of the controller",
    allocate(TwoParties, SingleParty),
  ) {
    case Participants(Participant(alpha, bank, houseOwner), Participant(beta, painter)) =>
      for {
        iou <- beta.create(painter, Iou(painter, houseOwner, onePound))
        offer <- beta.create(painter, PaintOffer(painter, houseOwner, bank, onePound))
        failure <- beta.exercise(painter, offer.exercisePaintOffer_Accept(_, iou)).failed
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "requires authorizers")
      }
  }

  /*
   * Privacy
   *
   * Visibility of contracts we fetch. Since the Ledger API has
   * no fetch operation built-in, we use a template with a choice
   * that causes the fetch.
   */

  test(
    "SemanticPrivacyProjections",
    "Test visibility via contract fetches for the paint-offer flow",
    allocate(TwoParties, SingleParty),
    timeoutScale = 2.0,
  ) {
    case Participants(Participant(alpha, bank, houseOwner), Participant(beta, painter)) =>
      for {
        iou <- alpha.create(bank, Iou(bank, houseOwner, onePound))
        _ <- synchronize(alpha, beta)

        // The IOU should be visible only to the payer and the owner
        _ <- fetchIou(alpha, bank, iou)
        _ <- fetchIou(alpha, houseOwner, iou)
        iouFetchFailure <- fetchIou(beta, painter, iou).failed

        offer <- beta.create(painter, PaintOffer(painter, houseOwner, bank, onePound))
        _ <- synchronize(alpha, beta)

        // The house owner and the painter can see the offer but the bank can't
        _ <- fetchPaintOffer(alpha, houseOwner, offer)
        _ <- fetchPaintOffer(beta, painter, offer)
        paintOfferFetchFailure <- fetchPaintOffer(alpha, bank, offer).failed

        tree <- alpha.exercise(houseOwner, offer.exercisePaintOffer_Accept(_, iou))
        (newIouEvent +: _, agreementEvent +: _) = createdEvents(tree).partition(
          _.getTemplateId == Tag.unwrap(Iou.id),
        )
        newIou = Primitive.ContractId[Iou](newIouEvent.contractId)
        agreement = Primitive.ContractId[PaintAgree](agreementEvent.contractId)
        _ <- synchronize(alpha, beta)

        // The Bank can see the new IOU, but it cannot see the PaintAgree contract
        _ <- fetchIou(alpha, bank, newIou)
        paintAgreeFetchFailure <- fetchPaintAgree(alpha, bank, agreement).failed

        // The house owner and the painter can see the contract
        _ <- fetchPaintAgree(beta, painter, agreement)
        _ <- fetchPaintAgree(alpha, houseOwner, agreement)

        // The painter sees its new IOU but the house owner cannot see it
        _ <- fetchIou(beta, painter, newIou)
        secondIouFetchFailure <- fetchIou(alpha, houseOwner, newIou).failed

      } yield {
        assertGrpcError(iouFetchFailure, Status.Code.INVALID_ARGUMENT, "couldn't find contract")
        assertGrpcError(
          paintOfferFetchFailure,
          Status.Code.INVALID_ARGUMENT,
          "couldn't find contract",
        )
        assertGrpcError(
          paintAgreeFetchFailure,
          Status.Code.INVALID_ARGUMENT,
          "couldn't find contract",
        )
        assertGrpcError(
          secondIouFetchFailure,
          Status.Code.INVALID_ARGUMENT,
          "requires one of the stakeholders",
        )
      }
  }

  private def fetchIou(
      ledger: ParticipantTestContext,
      party: Primitive.Party,
      iou: Primitive.ContractId[Iou],
  ): Future[Unit] =
    for {
      fetch <- ledger.create(party, FetchIou(party, iou))
      _ <- ledger.exercise(party, fetch.exerciseFetchIou_Fetch)
    } yield ()

  private def fetchPaintOffer(
      ledger: ParticipantTestContext,
      party: Primitive.Party,
      paintOffer: Primitive.ContractId[PaintOffer],
  ): Future[Unit] =
    for {
      fetch <- ledger.create(party, FetchPaintOffer(party, paintOffer))
      _ <- ledger.exercise(party, fetch.exerciseFetchPaintOffer_Fetch)
    } yield ()

  private def fetchPaintAgree(
      ledger: ParticipantTestContext,
      party: Primitive.Party,
      agreement: Primitive.ContractId[PaintAgree],
  ): Future[Unit] =
    for {
      fetch <- ledger.create(party, FetchPaintAgree(party, agreement))
      _ <- ledger.exercise(party, fetch.exerciseFetchPaintAgree_Fetch)
    } yield ()

  /*
   * Divulgence
   */

  test("SemanticDivulgence", "Respect divulgence rules", allocate(TwoParties, SingleParty)) {
    case Participants(Participant(alpha, issuer, owner), Participant(beta, delegate)) =>
      for {
        token <- alpha.create(issuer, Token(issuer, owner, 1))
        delegation <- alpha.create(owner, Delegation(owner, delegate))

        // The owner tries to divulge with a non-consuming choice, which actually doesn't work
        noDivulgeToken <- alpha.create(owner, Delegation(owner, delegate))
        _ <- alpha.exercise(owner, noDivulgeToken.exerciseDelegation_Wrong_Divulge_Token(_, token))
        _ <- synchronize(alpha, beta)
        failure <- beta
          .exercise(delegate, delegation.exerciseDelegation_Token_Consume(_, token))
          .failed

        // Successful divulgence and delegation
        divulgeToken <- alpha.create(owner, Delegation(owner, delegate))
        _ <- alpha.exercise(owner, divulgeToken.exerciseDelegation_Divulge_Token(_, token))
        _ <- eventually {
          beta.exercise(delegate, delegation.exerciseDelegation_Token_Consume(_, token))
        }
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "couldn't find contract")
      }
  }
}
