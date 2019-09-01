// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import ai.x.diff.conversions._
import com.daml.ledger.api.testtool.infrastructure.{
  LedgerSession,
  LedgerTest,
  LedgerTestContext,
  LedgerTestSuite
}
import com.digitalasset.ledger.client.binding.Primitive
import com.digitalasset.ledger.api.v1.value.{Optional, Record, RecordField, Value}
import com.digitalasset.ledger.test.DA.Types.{Tuple2 => DamlTuple2}
import com.digitalasset.ledger.test.SemanticTests.AccountInvitation._
import com.digitalasset.ledger.test.SemanticTests.AccountFetchByKey._
import com.digitalasset.ledger.test.SemanticTests.AccountLookupByKey._
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

  private[this] val doubleSpendAcrossTwoTransactions =
    LedgerTest("SemanticDoubleSpend", "Cannot double spend across transactions") { ledger =>
      for {
        Vector(payer, owner, newOwner, leftWithNothing) <- ledger.allocateParties(4)
        iou <- ledger.create(payer, Iou(payer, owner, onePound))
        _ <- ledger.exercise(owner, iou.exerciseTransfer(_, newOwner))
        failure <- ledger.exercise(owner, iou.exerciseTransfer(_, leftWithNothing)).failed
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "couldn't find contract")
      }
    }

  private[this] val doubleSpendInTransaction =
    LedgerTest("SemanticDoubleSpendSameTx", "Cannot double spend within a transaction") { ledger =>
      for {
        Vector(payer, owner, newOwner1, newOwner2) <- ledger.allocateParties(4)
        iou <- ledger.create(payer, Iou(payer, owner, onePound))
        doubleSpend <- ledger.submitAndWaitRequest(
          owner,
          iou.exerciseTransfer(owner, newOwner1).command,
          iou.exerciseTransfer(owner, newOwner2).command)
        failure <- ledger.submitAndWait(doubleSpend).failed
      } yield {
        assertGrpcError(
          failure,
          Status.Code.INVALID_ARGUMENT,
          "Update failed due to fetch of an inactive contract")
      }
    }

  private[this] val doubleSpendSharedContract =
    LedgerTest("SemanticDoubleSpendShared", "Different parties cannot spend the same contract") {
      ledger =>
        for {
          Vector(payer, owner1, owner2) <- ledger.allocateParties(3)
          shared <- ledger.create(payer, SharedContract(payer, owner1, owner2))
          _ <- ledger.exercise(owner1, shared.exerciseSharedContract_Consume1)
          failure <- ledger.exercise(owner2, shared.exerciseSharedContract_Consume2).failed
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

  private[this] val successfulPaintOffer =
    LedgerTest("SemanticPaintOffer", "Conduct the paint offer worflow successfully") { ledger =>
      for {
        Vector(bank, houseOwner, painter) <- ledger.allocateParties(3)
        iou <- ledger.create(bank, Iou(bank, houseOwner, onePound))
        offer <- ledger.create(painter, PaintOffer(painter, houseOwner, bank, onePound))
        tree <- ledger.exercise(houseOwner, offer.exercisePaintOffer_Accept(_, iou))
      } yield {
        val agreement = assertSingleton(
          "SemanticPaintOffer",
          createdEvents(tree).filter(_.getTemplateId == Tag.unwrap(PaintAgree.id)))
        assertEquals(
          "Paint agreement parameters",
          agreement.getCreateArguments,
          Record(
            recordId = Some(Tag.unwrap(PaintAgree.id)),
            fields = Seq(
              RecordField("painter", Some(Value(Value.Sum.Party(Tag.unwrap(painter))))),
              RecordField("houseOwner", Some(Value(Value.Sum.Party(Tag.unwrap(houseOwner)))))
            )
          )
        )
      }
    }

  private[this] val successfulPaintCounterOffer =
    LedgerTest("SemanticPaintCounterOffer", "Conduct the paint counter-offer worflow successfully") {
      ledger =>
        for {
          Vector(bank, houseOwner, painter) <- ledger.allocateParties(3)
          iou <- ledger.create(bank, Iou(bank, houseOwner, onePound))
          offer <- ledger.create(painter, PaintOffer(painter, houseOwner, bank, twoPounds))
          counter <- ledger.exerciseAndGetContract[PaintCounterOffer](
            houseOwner,
            offer.exercisePaintOffer_Counter(_, iou))
          tree <- ledger.exercise(painter, counter.exercisePaintCounterOffer_Accept)
        } yield {
          val agreement = assertSingleton(
            "SemanticPaintCounterOffer",
            createdEvents(tree).filter(_.getTemplateId == Tag.unwrap(PaintAgree.id)))
          assertEquals(
            "Paint agreement parameters",
            agreement.getCreateArguments,
            Record(
              recordId = Some(Tag.unwrap(PaintAgree.id)),
              fields = Seq(
                RecordField("painter", Some(Value(Value.Sum.Party(Tag.unwrap(painter))))),
                RecordField("houseOwner", Some(Value(Value.Sum.Party(Tag.unwrap(houseOwner)))))
              )
            )
          )
        }
    }

  private[this] val partialSignatories =
    LedgerTest(
      "SemanticPartialSignatories",
      "A signatory should not be able to create a contract on behalf of two parties") { ledger =>
      for {
        Vector(houseOwner, painter) <- ledger.allocateParties(2)
        failure <- ledger.create(houseOwner, PaintAgree(painter, houseOwner)).failed
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "requires authorizers")
      }
    }

  private[this] val acceptOnBehalf =
    LedgerTest(
      "SemanticAcceptOnBehalf",
      "It should not be possible to exercise a choice without the consent of the controller") {
      ledger =>
        for {
          Vector(bank, houseOwner, painter) <- ledger.allocateParties(3)
          iou <- ledger.create(painter, Iou(painter, houseOwner, onePound))
          offer <- ledger.create(painter, PaintOffer(painter, houseOwner, bank, onePound))
          failure <- ledger.exercise(painter, offer.exercisePaintOffer_Accept(_, iou)).failed
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

  private[this] val privacyProjections =
    LedgerTest(
      "SemanticPrivacyProjections",
      "Test visibility via contract fetches for the paint-offer flow") { ledger =>
      for {
        Vector(bank, houseOwner, painter) <- ledger.allocateParties(3)
        iou <- ledger.create(bank, Iou(bank, houseOwner, onePound))

        // The IOU should be visible only to the payer and the owner
        _ <- fetchIou(ledger, bank, iou)
        _ <- fetchIou(ledger, houseOwner, iou)
        _ <- fetchIou(ledger, painter, iou).failed

        offer <- ledger.create(painter, PaintOffer(painter, houseOwner, bank, onePound))

        // The house owner and the painter can see the offer but the bank can't
        _ <- fetchPaintOffer(ledger, houseOwner, offer)
        _ <- fetchPaintOffer(ledger, painter, offer)
        _ <- fetchPaintOffer(ledger, bank, offer).failed

        tree <- ledger.exercise(houseOwner, offer.exercisePaintOffer_Accept(_, iou))
        (newIouEvent +: _, agreementEvent +: _) = createdEvents(tree).partition(
          _.getTemplateId == Tag.unwrap(Iou.id))
        newIou = Primitive.ContractId[Iou](newIouEvent.contractId)
        agreement = Primitive.ContractId[PaintAgree](agreementEvent.contractId)

        // The Bank can see the new IOU, but it cannot see the PaintAgree contract
        _ <- fetchIou(ledger, bank, newIou)
        _ <- fetchPaintAgree(ledger, bank, agreement).failed

        // The house owner and the painter can see the contract
        _ <- fetchPaintAgree(ledger, painter, agreement)
        _ <- fetchPaintAgree(ledger, houseOwner, agreement)

        // The painter sees its new IOU but the house owner cannot see it
        _ <- fetchIou(ledger, painter, newIou)
        _ <- fetchIou(ledger, houseOwner, newIou).failed

      } yield {
        // Nothing to do, all checks have been done in the for-comprehension
      }
    }

  private def fetchIou(
      ledger: LedgerTestContext,
      party: Primitive.Party,
      iou: Primitive.ContractId[Iou]): Future[Unit] =
    for {
      fetch <- ledger.create(party, FetchIou(party, iou))
      _ <- ledger.exercise(party, fetch.exerciseFetchIou_Fetch)
    } yield ()

  private def fetchPaintOffer(
      ledger: LedgerTestContext,
      party: Primitive.Party,
      paintOffer: Primitive.ContractId[PaintOffer]
  ): Future[Unit] =
    for {
      fetch <- ledger.create(party, FetchPaintOffer(party, paintOffer))
      _ <- ledger.exercise(party, fetch.exerciseFetchPaintOffer_Fetch)
    } yield ()

  private def fetchPaintAgree(
      ledger: LedgerTestContext,
      party: Primitive.Party,
      agreement: Primitive.ContractId[PaintAgree]
  ): Future[Unit] =
    for {
      fetch <- ledger.create(party, FetchPaintAgree(party, agreement))
      _ <- ledger.exercise(party, fetch.exerciseFetchPaintAgree_Fetch)
    } yield ()

  /*
   * Divulgence
   */

  private[this] val divulgence =
    LedgerTest("SemanticDivulgence", "Respect divulgence rules") { ledger =>
      for {
        Vector(issuer, owner, delegate) <- ledger.allocateParties(3)
        token <- ledger.create(issuer, Token(issuer, owner, 1))
        delegation <- ledger.create(owner, Delegation(owner, delegate))

        // The owner tries to divulge with a non-consuming choice, which actually doesn't work
        noDivulgeToken <- ledger.create(owner, Delegation(owner, delegate))
        _ <- ledger.exercise(owner, noDivulgeToken.exerciseDelegation_Wrong_Divulge_Token(_, token))
        _ <- ledger.exercise(delegate, delegation.exerciseDelegation_Token_Consume(_, token)).failed

        // Successful divulgence and delegation
        divulgeToken <- ledger.create(owner, Delegation(owner, delegate))
        _ <- ledger.exercise(owner, divulgeToken.exerciseDelegation_Divulge_Token(_, token))
        _ <- ledger.exercise(delegate, delegation.exerciseDelegation_Token_Consume(_, token))
      } yield {}
    }

  /*
   * Contract keys
   */

  private[this] val contractKeys =
    LedgerTest("SemanticContractKeys", "Perform correctly operations based on contract keys") {
      ledger =>
        for {
          Vector(bank, accountHolder) <- ledger.allocateParties(2)
          accountTemplate = Account(bank, accountHolder, DamlTuple2("CH", 123))
          // Replicating the logic with which we compute the contract key, which in DAML is the following:
          // key (bank, accountNumber._1 <> show (this.accountNumber._2)) : (Party, Text)
          accountKey = DamlTuple2(bank, "CH123") //
          invitation <- ledger.create(bank, AccountInvitation(accountTemplate))
          account <- ledger
            .exerciseAndGetContract[Account](accountHolder, invitation.exerciseAccept)
          toLookup <- ledger.create(bank, AccountLookupByKey(bank, accountKey))
          lookup <- ledger.exercise(bank, toLookup.exerciseAccountLookupByKey_Execute)
          toFetch <- ledger.create(bank, AccountFetchByKey(bank, accountKey))
          fetch <- ledger.exercise(bank, toFetch.exerciseAccountFetchByKey_Execute)
        } yield {

          val lookupEvent = assertSingleton("Lookup", exercisedEvents(lookup))
          assertEquals(
            "The contract that has been looked up is wrong",
            lookupEvent.getExerciseResult,
            Value(
              Value.Sum.Optional(Optional(Some(Value(Value.Sum.ContractId(Tag.unwrap(account)))))))
          )

          val fetchEvent = assertSingleton("Fetch", exercisedEvents(fetch))
          val fetchedFields = fetchEvent.getExerciseResult.getRecord.fields

          val id = assertSingleton("Fetched contract identifier", fetchedFields.collect {
            case RecordField("_1", Some(value)) => value.getContractId
          })
          assertEquals("Fetched contract identifier", id, Tag.unwrap(account))

          val contract = assertSingleton("Fetched contract", fetchedFields.collect {
            case RecordField("_2", Some(value)) => value.getRecord
          })
          assertEquals("The fetched contract is wrong", contract, accountTemplate.arguments)
        }
    }

  override val tests: Vector[LedgerTest] = Vector(
    doubleSpendAcrossTwoTransactions,
    doubleSpendInTransaction,
    doubleSpendSharedContract,
    successfulPaintOffer,
    successfulPaintCounterOffer,
    partialSignatories,
    acceptOnBehalf,
    privacyProjections,
    divulgence,
    contractKeys
  )
}
