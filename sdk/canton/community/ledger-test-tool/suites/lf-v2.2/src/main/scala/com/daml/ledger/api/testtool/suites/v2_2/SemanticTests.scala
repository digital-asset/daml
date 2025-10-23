// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.*
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, Party, TestConstraints}
import com.daml.ledger.api.v2.commands.DisclosedContract
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  TemplateFilter,
  TransactionFormat,
}
import com.daml.ledger.api.v2.value.{Record, RecordField, Value}
import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.ledger.test.java.semantic.semantictests.{PaintOffer, *}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.error.groups.{CommandExecutionErrors, ConsistencyErrors}

import java.math.BigDecimal
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.util.Success

final class SemanticTests extends LedgerTestSuite {
  import CompanionImplicits.*
  implicit val delegationCompanion
      : ContractCompanion.WithoutKey[Delegation.Contract, Delegation.ContractId, Delegation] =
    Delegation.COMPANION
  implicit val sharedContractCompanion: ContractCompanion.WithoutKey[
    SharedContract.Contract,
    SharedContract.ContractId,
    SharedContract,
  ] = SharedContract.COMPANION
  implicit val paintOfferCompanion
      : ContractCompanion.WithoutKey[PaintOffer.Contract, PaintOffer.ContractId, PaintOffer] =
    PaintOffer.COMPANION

  private[this] val onePound = new Amount(BigDecimal.valueOf(1), "GBP")
  private[this] val twoPounds = new Amount(BigDecimal.valueOf(2), "GBP")

  /*
   * Consistency
   *
   * A transaction is internally consistent for a contract
   * `c` if the following hold for all its subactions `act`
   * on the contract `c`:
   *
   * 1. `act` does not happen before any Create c action (correct by construction in Daml)
   * 2. `act` does not happen after the contract has been consumend (i.e. no double spending)
   */

  test(
    "SemanticDoubleSpendBasic",
    "Cannot double spend across transactions",
    allocate(TwoParties, TwoParties),
  )(implicit ec => {
    case Participants(
          Participant(alpha, Seq(payer, owner)),
          Participant(_, Seq(newOwner, leftWithNothing)),
        ) =>
      for {
        iou <- alpha.create(payer, new Iou(payer, owner, onePound))
        _ <- alpha.exercise(owner, iou.exerciseTransfer(newOwner))
        failure <- alpha
          .exercise(owner, iou.exerciseTransfer(leftWithNothing))
          .mustFail("consuming a contract twice")
      } yield {
        assertGrpcError(
          failure,
          ConsistencyErrors.ContractNotFound,
          Some("Contract could not be found"),
          checkDefiniteAnswerMetadata = true,
        )
      }
  })

  test(
    "SemanticConcurrentDoubleSpend",
    "Cannot concurrently double spend across transactions",
    allocate(TwoParties, SingleParty),
  )(implicit ec => {
    case p @ Participants(Participant(alpha, Seq(payer, owner1)), Participant(beta, Seq(owner2))) =>
      // This test create a contract and then concurrently archives it several times
      // through two different participants
      val creates = 2 // Number of contracts to create
      val archives = 10 // Number of concurrent archives per contract
      // Each created contract is archived in parallel,
      // Next contract is created only when previous one is archived
      (1 to creates)
        .foldLeft(Future(())) {
          (f, c) =>
            f.flatMap(_ =>
              for {
                shared <- alpha.create(payer, new SharedContract(payer, owner1, owner2))
                _ <- p.synchronize
                results <- Future.traverse(1 to archives) {
                  case i if i % 2 == 0 =>
                    alpha
                      .exercise(owner1, shared.exerciseSharedContract_Consume1())
                      .transform(Success(_))
                  case _ =>
                    beta
                      .exercise(owner2, shared.exerciseSharedContract_Consume2())
                      .transform(Success(_))
                }
              } yield {
                assertLength(
                  s"Contract $c successful archives",
                  1,
                  results.filter(_.isSuccess),
                ).discard
                assertLength(
                  s"Contract $c failed archives",
                  archives - 1,
                  results.filter(_.isFailure),
                ).discard
              }
            )
        }
  })

  test(
    "SemanticDoubleSpendSameTx",
    "Cannot double spend within a transaction",
    allocate(TwoParties, TwoParties),
  )(implicit ec => {
    case Participants(
          Participant(alpha, Seq(payer, owner)),
          Participant(_, Seq(newOwner1, newOwner2)),
        ) =>
      for {
        iou <- alpha.create(payer, new Iou(payer, owner, onePound))
        doubleSpend = alpha.submitAndWaitRequest(
          owner,
          (iou.exerciseTransfer(newOwner1).commands.asScala ++ iou
            .exerciseTransfer(newOwner2)
            .commands
            .asScala).asJava,
        )
        failure <- alpha.submitAndWait(doubleSpend).mustFail("consuming a contract twice")
      } yield {
        assertGrpcError(
          failure,
          CommandExecutionErrors.Interpreter.ContractNotActive,
          Some("Update failed due to fetch of an inactive contract"),
          checkDefiniteAnswerMetadata = true,
        )
      }
  })

  test(
    "SemanticDoubleSpendShared",
    "Different parties cannot spend the same contract",
    allocate(TwoParties, SingleParty),
  )(implicit ec => {
    case p @ Participants(Participant(alpha, Seq(payer, owner1)), Participant(beta, Seq(owner2))) =>
      for {
        shared <- alpha.create(payer, new SharedContract(payer, owner1, owner2))
        _ <- alpha.exercise(owner1, shared.exerciseSharedContract_Consume1())
        _ <- p.synchronize
        failure <- beta
          .exercise(owner2, shared.exerciseSharedContract_Consume2())
          .mustFail("consuming a contract twice")
      } yield {
        assertGrpcError(
          failure,
          ConsistencyErrors.ContractNotFound,
          Some("Contract could not be found"),
          checkDefiniteAnswerMetadata = true,
        )
      }
  })

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
    limitation = TestConstraints.GrpcOnly(
      "RecordId information is not propagated by transcode/SchemaProcessor"
    ),
  )(implicit ec => {
    case Participants(Participant(alpha, Seq(bank, houseOwner)), Participant(beta, Seq(painter))) =>
      for {
        iou <- alpha.create(bank, new Iou(bank, houseOwner, onePound))
        offer <- beta.create(painter, new PaintOffer(painter, houseOwner, bank, onePound))
        tree <- eventually("Exercise paint offer") {
          alpha.exercise(houseOwner, offer.exercisePaintOffer_Accept(iou))
        }
      } yield {
        val agreement = assertSingleton(
          "SemanticPaintOffer",
          createdEvents(tree).filter(_.getTemplateId == PaintAgree.TEMPLATE_ID_WITH_PACKAGE_ID.toV1),
        )
        assertEquals(
          "Paint agreement parameters",
          agreement.getCreateArguments,
          Record(
            recordId = Some(PaintAgree.TEMPLATE_ID_WITH_PACKAGE_ID.toV1),
            fields = Seq(
              RecordField("painter", Some(Value(Value.Sum.Party(painter)))),
              RecordField("houseOwner", Some(Value(Value.Sum.Party(houseOwner)))),
            ),
          ),
        )
      }
  })

  test(
    "SemanticPaintCounterOffer",
    "Conduct the paint counter-offer workflow successfully",
    allocate(TwoParties, SingleParty),
    limitation = TestConstraints.GrpcOnly(
      "RecordId information is not propagated by transcode/SchemaProcessor"
    ),
  )(implicit ec => {
    case Participants(Participant(alpha, Seq(bank, houseOwner)), Participant(beta, Seq(painter))) =>
      for {
        iou <- alpha.create(bank, new Iou(bank, houseOwner, onePound))
        txReq <- alpha.getTransactionsRequest(
          TransactionFormat(
            eventFormat = Some(
              EventFormat(
                filtersByParty = Map(
                  houseOwner.getValue -> Filters(
                    Seq(
                      CumulativeFilter(
                        IdentifierFilter.TemplateFilter(
                          TemplateFilter(
                            Some(Iou.TEMPLATE_ID.toV1),
                            includeCreatedEventBlob = true,
                          )
                        )
                      )
                    )
                  )
                ),
                filtersForAnyParty = None,
                verbose = false,
              )
            ),
            transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
          )
        )
        iouTxsForHouseOwner <- alpha.transactions(txReq)
        iouCreatedEvent = createdEvents(iouTxsForHouseOwner.head).head
        iouDisclosedContract = DisclosedContract(
          templateId = iouCreatedEvent.templateId,
          contractId = iouCreatedEvent.contractId,
          createdEventBlob = iouCreatedEvent.createdEventBlob,
          synchronizerId = "",
        )
        offer <- beta.create(painter, new PaintOffer(painter, houseOwner, bank, twoPounds))
        counter <- eventually("exerciseAndGetContract") {
          alpha.exerciseAndGetContract[PaintCounterOffer.ContractId, PaintCounterOffer](
            houseOwner,
            offer.exercisePaintOffer_Counter(iou),
          )(PaintCounterOffer.COMPANION)
        }
        tx <- eventually("exercisePaintCounterOffer") {
          val request = beta
            .submitAndWaitForTransactionRequest(
              painter,
              counter.exercisePaintCounterOffer_Accept().commands,
            )
            .update(_.commands.disclosedContracts := Seq(iouDisclosedContract))
          beta.submitAndWaitForTransaction(request).map(_.getTransaction)
        }
      } yield {
        val agreement = assertSingleton(
          "SemanticPaintCounterOffer",
          createdEvents(tx).filter(
            _.getTemplateId == PaintAgree.TEMPLATE_ID_WITH_PACKAGE_ID.toV1
          ),
        )
        assertEquals(
          "Paint agreement parameters",
          agreement.getCreateArguments,
          Record(
            recordId = Some(PaintAgree.TEMPLATE_ID_WITH_PACKAGE_ID.toV1),
            fields = Seq(
              RecordField("painter", Some(Value(Value.Sum.Party(painter)))),
              RecordField("houseOwner", Some(Value(Value.Sum.Party(houseOwner)))),
            ),
          ),
        )
      }
  })

  test(
    "SemanticPartialSignatories",
    "A signatory should not be able to create a contract on behalf of two parties",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha, Seq(houseOwner)), Participant(_, Seq(painter))) =>
      for {
        failure <- alpha
          .create(houseOwner, new PaintAgree(painter, houseOwner))(PaintAgree.COMPANION)
          .mustFail("creating a contract on behalf of two parties")
      } yield {
        assertGrpcError(
          failure,
          CommandExecutionErrors.Interpreter.AuthorizationError,
          Some("requires authorizers"),
          checkDefiniteAnswerMetadata = true,
        )
      }
  })

  test(
    "SemanticAcceptOnBehalf",
    "It should not be possible to exercise a choice without the consent of the controller",
    allocate(TwoParties, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha, Seq(bank, houseOwner)), Participant(beta, Seq(painter))) =>
      for {
        iou <- beta.create(painter, new Iou(painter, houseOwner, onePound))
        offer <- beta.create(painter, new PaintOffer(painter, houseOwner, bank, onePound))
        failure <- beta
          .exercise(painter, offer.exercisePaintOffer_Accept(iou))
          .mustFail("exercising a choice without consent")
      } yield {
        assertGrpcError(
          failure,
          CommandExecutionErrors.Interpreter.AuthorizationError,
          Some("requires authorizers"),
          checkDefiniteAnswerMetadata = true,
        )
      }
  })

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
  )(implicit ec => {
    case p @ Participants(
          Participant(alpha, Seq(bank, houseOwner)),
          Participant(beta, Seq(painter)),
        ) =>
      for {
        iou <- alpha.create(bank, new Iou(bank, houseOwner, onePound))
        _ <- p.synchronize

        // The IOU should be visible only to the payer and the owner
        _ <- fetchIou(alpha, bank, iou)
        _ <- fetchIou(alpha, houseOwner, iou)
        iouFetchFailure <- fetchIou(beta, painter, iou)
          .mustFail("fetching the IOU with the wrong party")

        offer <- beta.create(painter, new PaintOffer(painter, houseOwner, bank, onePound))
        _ <- p.synchronize

        // The house owner and the painter can see the offer but the bank can't
        _ <- fetchPaintOffer(alpha, houseOwner, offer)
        _ <- fetchPaintOffer(beta, painter, offer)
        paintOfferFetchFailure <- fetchPaintOffer(alpha, bank, offer)
          .mustFail("fetching the offer with the wrong party")

        tree <- alpha.exercise(houseOwner, offer.exercisePaintOffer_Accept(iou))
        (newIouEvents, agreementEvents) = createdEvents(tree).partition(
          _.getTemplateId == Iou.TEMPLATE_ID_WITH_PACKAGE_ID.toV1
        )
        newIouEvent <- Future(newIouEvents.head)
        agreementEvent <- Future(agreementEvents.head)
        newIou = new Iou.ContractId(newIouEvent.contractId)
        agreement = new PaintAgree.ContractId(agreementEvent.contractId)
        _ <- p.synchronize

        // The Bank can see the new IOU, but it cannot see the PaintAgree contract
        _ <- fetchIou(alpha, bank, newIou)
        paintAgreeFetchFailure <- fetchPaintAgree(alpha, bank, agreement)
          .mustFail("fetching the agreement with the wrong party")

        // The house owner and the painter can see the contract
        _ <- fetchPaintAgree(beta, painter, agreement)
        _ <- fetchPaintAgree(alpha, houseOwner, agreement)

        // The painter sees its new IOU but the house owner cannot see it
        _ <- fetchIou(beta, painter, newIou)
        withoutExplicitDisclosureFailure <- fetchIou(alpha, houseOwner, newIou)
          .mustFail("fetching the new IOU without explicit disclosure")
        txReq <- alpha.getTransactionsRequest(
          TransactionFormat(
            eventFormat = Some(
              EventFormat(
                filtersByParty = Map(
                  bank.getValue -> Filters(
                    Seq(
                      CumulativeFilter(
                        IdentifierFilter.TemplateFilter(
                          TemplateFilter(
                            Some(Iou.TEMPLATE_ID.toV1),
                            includeCreatedEventBlob = true,
                          )
                        )
                      )
                    )
                  )
                ),
                filtersForAnyParty = None,
                verbose = false,
              )
            ),
            transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
          )
        )
        iouTxsForBank <- alpha.transactions(txReq)
        iouCreatedEvent = iouTxsForBank.flatMap(createdEvents)(1)
        iouDisclosedContract = DisclosedContract(
          templateId = iouCreatedEvent.templateId,
          contractId = iouCreatedEvent.contractId,
          createdEventBlob = iouCreatedEvent.createdEventBlob,
          synchronizerId = "",
        )
        wrongPartyFailure <- fetchIouWithExplicitDisclosure(
          alpha,
          houseOwner,
          newIou,
          iouDisclosedContract,
        )
          .mustFail("fetching the new IOU with the wrong party")
      } yield {
        assertEquals(
          "the second created contract should be the new IOU",
          iouCreatedEvent.contractId,
          newIou.contractId,
        )
        assertGrpcError(
          iouFetchFailure,
          ConsistencyErrors.ContractNotFound,
          Some("Contract could not be found"),
          checkDefiniteAnswerMetadata = true,
        )
        assertGrpcError(
          paintOfferFetchFailure,
          ConsistencyErrors.ContractNotFound,
          Some("Contract could not be found"),
          checkDefiniteAnswerMetadata = true,
        )
        assertGrpcError(
          paintAgreeFetchFailure,
          ConsistencyErrors.ContractNotFound,
          Some("Contract could not be found"),
          checkDefiniteAnswerMetadata = true,
        )
        assertGrpcError(
          withoutExplicitDisclosureFailure,
          ConsistencyErrors.ContractNotFound,
          Some("Contract could not be found"),
          checkDefiniteAnswerMetadata = true,
        )
        assertGrpcError(
          wrongPartyFailure,
          CommandExecutionErrors.Interpreter.AuthorizationError,
          Some("requires one of the stakeholders"),
          checkDefiniteAnswerMetadata = true,
        )
      }
  })

  private def fetchIou(
      ledger: ParticipantTestContext,
      party: Party,
      iou: Iou.ContractId,
  )(implicit ec: ExecutionContext): Future[Unit] =
    for {
      fetch <- ledger.create(party, new FetchIou(party, iou))(FetchIou.COMPANION)
      _ <- ledger.exercise(party, fetch.exerciseFetchIou_Fetch())
    } yield ()

  private def fetchIouWithExplicitDisclosure(
      ledger: ParticipantTestContext,
      party: Party,
      iou: Iou.ContractId,
      iouDisclosedContract: DisclosedContract,
  )(implicit ec: ExecutionContext): Future[Unit] =
    for {
      fetch <- ledger.create(party, new FetchIou(party, iou))(FetchIou.COMPANION)
      request = ledger
        .submitAndWaitRequest(
          party,
          fetch.exerciseFetchIou_Fetch().commands(),
        )
        .update(_.commands.disclosedContracts := Seq(iouDisclosedContract))
      _ <- ledger.submitAndWait(request).map(_ => ())
    } yield ()

  private def fetchPaintOffer(
      ledger: ParticipantTestContext,
      party: Party,
      paintOffer: PaintOffer.ContractId,
  )(implicit ec: ExecutionContext): Future[Unit] =
    for {
      fetch <- ledger.create(party, new FetchPaintOffer(party, paintOffer))(
        FetchPaintOffer.COMPANION
      )
      _ <- ledger.exercise(party, fetch.exerciseFetchPaintOffer_Fetch())
    } yield ()

  private def fetchPaintAgree(
      ledger: ParticipantTestContext,
      party: Party,
      agreement: PaintAgree.ContractId,
  )(implicit ec: ExecutionContext): Future[Unit] =
    for {
      fetch <- ledger.create(party, new FetchPaintAgree(party, agreement))(
        FetchPaintAgree.COMPANION
      )
      _ <- ledger.exercise(party, fetch.exerciseFetchPaintAgree_Fetch())
    } yield ()

  /*
   * Divulgence
   */

  test("SemanticDivulgence", "Respect divulgence rules", allocate(TwoParties, SingleParty))(
    implicit ec => {
      case p @ Participants(
            Participant(alpha, Seq(issuer, owner)),
            Participant(beta, Seq(delegate)),
          ) =>
        for {
          token <- alpha.create(issuer, new Token(issuer, owner, 1))(Token.COMPANION)
          delegation <- alpha.create[Delegation.ContractId, Delegation](
            owner,
            new Delegation(owner, delegate),
          )(Delegation.COMPANION)

          // The owner tries to divulge with a non-consuming choice, which actually doesn't work
          noDivulgeToken <- alpha.create(owner, new Delegation(owner, delegate))(
            Delegation.COMPANION
          )
          _ <- alpha
            .exercise(owner, noDivulgeToken.exerciseDelegation_Wrong_Divulge_Token(token))
          _ <- p.synchronize
          failureNonConsuming <- beta
            .exercise(delegate, delegation.exerciseDelegation_Token_Consume(token))
            .mustFail("divulging with a non-consuming choice")

          // The owner tries to divulge with a consuming choice, which actually doesn't work neither
          divulgeToken <- alpha.create(owner, new Delegation(owner, delegate))(Delegation.COMPANION)
          _ <- alpha.exercise(owner, divulgeToken.exerciseDelegation_Divulge_Token(token))
          failureConsuming <- beta
            .exercise(delegate, delegation.exerciseDelegation_Token_Consume(token))
            .mustFail("divulging with a consuming choice")
        } yield {
          assertGrpcError(
            failureNonConsuming,
            ConsistencyErrors.ContractNotFound,
            Some("Contract could not be found"),
            checkDefiniteAnswerMetadata = true,
          )
          assertGrpcError(
            failureConsuming,
            ConsistencyErrors.ContractNotFound,
            Some("Contract could not be found"),
            checkDefiniteAnswerMetadata = true,
          )
        }
    }
  )
}
