// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.test.model.Test.Agreement._
import com.daml.ledger.test.model.Test.AgreementFactory._
import com.daml.ledger.test.model.Test.TriProposal._
import com.daml.ledger.test.model.Test._
import com.daml.platform.error.definitions.LedgerApiErrors

class TransactionServiceAuthorizationIT extends LedgerTestSuite {
  test(
    "TXRequireAuthorization",
    "Require only authorization of chosen branching signatory",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(_, bob)) =>
    val template = BranchingSignatories(whichSign = true, signTrue = alice, signFalse = bob)
    for {
      _ <- alpha.create(alice, template)
      transactions <- alpha.flatTransactions(alice)
    } yield {
      assert(template.arguments == transactions.head.events.head.getCreated.getCreateArguments)
    }
  })

  test(
    "TXMultiActorChoiceOkBasic",
    "Accept exercising a well-authorized multi-actor choice",
    allocate(TwoParties, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha, operator, receiver), Participant(beta, giver)) =>
      for {
        agreementFactory <- beta.create(giver, AgreementFactory(receiver, giver))
        agreement <- eventually("exerciseAgreementFactoryAccept") {
          alpha.exerciseAndGetContract(receiver, agreementFactory.exerciseAgreementFactoryAccept())
        }
        triProposalTemplate = TriProposal(operator, receiver, giver)
        triProposal <- alpha.create(operator, triProposalTemplate)
        tree <- eventually("exerciseAcceptTriProposal") {
          beta.exercise(giver, agreement.exerciseAcceptTriProposal(triProposal))
        }
      } yield {
        val contract = assertSingleton("AcceptTriProposal", createdEvents(tree))
        assertEquals(
          "AcceptTriProposal",
          contract.getCreateArguments.fields,
          triProposalTemplate.arguments.fields,
        )
      }
  })

  test(
    "TXMultiActorChoiceOkCoincidingControllers",
    "Accept exercising a well-authorized multi-actor choice with coinciding controllers",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, operator), Participant(beta, giver)) =>
    for {
      agreementFactory <- beta.create(giver, AgreementFactory(giver, giver))
      agreement <-
        beta.exerciseAndGetContract(giver, agreementFactory.exerciseAgreementFactoryAccept())
      triProposalTemplate = TriProposal(operator, giver, giver)
      triProposal <- alpha.create(operator, triProposalTemplate)
      tree <- eventually("exerciseAcceptTriProposal") {
        beta.exercise(giver, agreement.exerciseAcceptTriProposal(triProposal))
      }
    } yield {
      val contract = assertSingleton("AcceptTriProposalCoinciding", createdEvents(tree))
      assertEquals(
        "AcceptTriProposalCoinciding",
        contract.getCreateArguments.fields,
        triProposalTemplate.arguments.fields,
      )
    }
  })

  test(
    "TXRejectMultiActorMissingAuth",
    "Reject exercising a multi-actor choice with missing authorizers",
    allocate(TwoParties, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha, operator, receiver), Participant(beta, giver)) =>
      for {
        triProposal <- alpha.create(operator, TriProposal(operator, receiver, giver))
        _ <- eventually("exerciseTriProposalAccept") {
          for {
            failure <- beta
              .exercise(giver, triProposal.exerciseTriProposalAccept())
              .mustFail("exercising with missing authorizers")
          } yield {
            assertGrpcError(
              failure,
              LedgerApiErrors.CommandExecution.Interpreter.AuthorizationError,
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
    case Participants(Participant(alpha, operator, receiver), Participant(beta, giver)) =>
      for {
        agreementFactory <- beta.create(giver, AgreementFactory(receiver, giver))
        // TODO eventually is a temporary workaround. It should take into account
        // TODO that the contract needs to hit the target node before a choice
        // TODO is executed on it.
        agreement <- eventually("exerciseAgreementFactoryAccept") {
          alpha.exerciseAndGetContract(receiver, agreementFactory.exerciseAgreementFactoryAccept())
        }
        triProposalTemplate = TriProposal(operator, giver, giver)
        triProposal <- alpha.create(operator, triProposalTemplate)
        _ <- eventually("exerciseAcceptTriProposal") {
          for {
            failure <- beta
              .exercise(giver, agreement.exerciseAcceptTriProposal(triProposal))
              .mustFail("exercising with failing assertion")
          } yield {
            assertGrpcError(
              failure,
              LedgerApiErrors.CommandExecution.Interpreter.GenericInterpretationError,
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
