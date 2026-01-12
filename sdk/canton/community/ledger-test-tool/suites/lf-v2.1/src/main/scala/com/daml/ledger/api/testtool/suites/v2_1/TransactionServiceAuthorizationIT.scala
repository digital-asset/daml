// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.*
import com.daml.ledger.api.v2.value.{Record, RecordField}
import com.daml.ledger.test.java.model.test.{
  Agreement,
  AgreementFactory,
  BranchingSignatories,
  TriProposal,
}
import com.digitalasset.base.error.{ErrorCategory, ErrorCode}
import com.digitalasset.canton.ledger.api.TransactionShape.AcsDelta
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors

import scala.jdk.CollectionConverters.*

class TransactionServiceAuthorizationIT extends LedgerTestSuite {
  import CompanionImplicits.*

  test(
    "TXRequireAuthorization",
    "Require only authorization of chosen branching signatory",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, Seq(alice)), Participant(_, Seq(bob))) =>
    import ClearIdsImplicits.*
    val template = new BranchingSignatories(true, alice, bob)
    for {
      _ <- alpha.create(alice, template)(BranchingSignatories.COMPANION)
      transactions <- alpha.transactions(AcsDelta, alice)
    } yield {
      assert(
        Record.fromJavaProto(
          template.toValue.toProtoRecord
        ) == transactions.headOption.value.events.headOption.value.getCreated.getCreateArguments.clearValueIds
      )
    }
  })

  test(
    "TXMultiActorChoiceOkBasic",
    "Accept exercising a well-authorized multi-actor choice",
    allocate(TwoParties, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha, Seq(operator, receiver)), Participant(beta, Seq(giver))) =>
      for {
        agreementFactory <- beta.create(giver, new AgreementFactory(receiver, giver))
        agreement <- eventually("exerciseAgreementFactoryAccept") {
          alpha.exerciseAndGetContract[Agreement.ContractId, Agreement](
            receiver,
            agreementFactory.exerciseAgreementFactoryAccept(),
          )(Agreement.COMPANION)
        }
        triProposalTemplate = new TriProposal(operator, receiver, giver)
        triProposal <- alpha.create(operator, triProposalTemplate)
        tree <- eventually("exerciseAcceptTriProposal") {
          beta.exercise(giver, agreement.exerciseAcceptTriProposal(triProposal))
        }
      } yield {
        val contract = assertSingleton("AcceptTriProposal", createdEvents(tree))
        assertEquals(
          "AcceptTriProposal",
          contract.getCreateArguments.fields,
          triProposalTemplate.toValue.getFields.asScala.map(rf =>
            RecordField.fromJavaProto(rf.toProto)
          ),
        )
      }
  })

  test(
    "TXMultiActorChoiceOkCoincidingControllers",
    "Accept exercising a well-authorized multi-actor choice with coinciding controllers",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha, Seq(operator)), Participant(beta, Seq(giver))) =>
      for {
        agreementFactory <- beta.create(giver, new AgreementFactory(giver, giver))
        agreement <-
          beta.exerciseAndGetContract[Agreement.ContractId, Agreement](
            giver,
            agreementFactory.exerciseAgreementFactoryAccept(),
          )(
            Agreement.COMPANION
          )
        triProposalTemplate = new TriProposal(operator, giver, giver)
        triProposal <- alpha.create(operator, triProposalTemplate)
        tree <- eventually("exerciseAcceptTriProposal") {
          beta.exercise(giver, agreement.exerciseAcceptTriProposal(triProposal))
        }
      } yield {
        val contract = assertSingleton("AcceptTriProposalCoinciding", createdEvents(tree))
        assertEquals(
          "AcceptTriProposalCoinciding",
          contract.getCreateArguments.fields,
          triProposalTemplate.toValue.getFields.asScala.map(rf =>
            RecordField.fromJavaProto(rf.toProto)
          ),
        )
      }
  })

  test(
    "TXRejectMultiActorMissingAuth",
    "Reject exercising a multi-actor choice with missing authorizers",
    allocate(TwoParties, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha, Seq(operator, receiver)), Participant(beta, Seq(giver))) =>
      for {
        triProposal <- alpha.create(operator, new TriProposal(operator, receiver, giver))
        _ <- eventually("exerciseTriProposalAccept") {
          for {
            failure <- beta
              .exercise(giver, triProposal.exerciseTriProposalAccept())
              .mustFail("exercising with missing authorizers")
          } yield {
            assertGrpcError(
              failure,
              CommandExecutionErrors.Interpreter.AuthorizationError,
              Some("requires authorizers"),
              checkDefiniteAnswerMetadata = true,
            )
          }
        }
      } yield {
        // Check performed in the `eventually` block
      }
  })

  // This is the current, most conservative semantics of multi-actor choice authorization.
  // It is likely that this will change in the future. Should we delete this test, we should
  // also remove the 'UnrestrictedAcceptTriProposal' choice from the 'Agreement' template.
  test(
    "TXRejectMultiActorExcessiveAuth",
    "Reject exercising a multi-actor choice with too many authorizers",
    allocate(TwoParties, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha, Seq(operator, receiver)), Participant(beta, Seq(giver))) =>
      for {
        agreementFactory <- beta.create(giver, new AgreementFactory(receiver, giver))
        // TODO(#16361) eventually is a temporary workaround. It should take into account
        // that the contract needs to hit the target node before a choice
        // is executed on it.
        agreement <- eventually("exerciseAgreementFactoryAccept") {
          alpha.exerciseAndGetContract[Agreement.ContractId, Agreement](
            receiver,
            agreementFactory.exerciseAgreementFactoryAccept(),
          )(Agreement.COMPANION)
        }
        triProposalTemplate = new TriProposal(operator, giver, giver)
        triProposal <- alpha.create(operator, triProposalTemplate)
        _ <- eventually("exerciseAcceptTriProposal") {
          for {
            failure <- beta
              .exercise(giver, agreement.exerciseAcceptTriProposal(triProposal))
              .mustFail("exercising with failing assertion")
          } yield {
            assertGrpcError(
              failure,
              new ErrorCode(
                CommandExecutionErrors.Interpreter.FailureStatus.id,
                ErrorCategory.InvalidGivenCurrentSystemStateOther,
              )(
                CommandExecutionErrors.Interpreter.FailureStatus.parent
              ) {},
              Some("Assertion failed"),
              checkDefiniteAnswerMetadata = true,
            )
          }
        }
      } yield {
        // Check performed in the `eventually` block
      }
  })
}
