// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.javaapi.data.codegen.ContractId
import com.daml.ledger.test.java.model.test.{Dummy, DummyWithParam}
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.protocol.TestUpdateId

class UpdateServiceQueryIT extends LedgerTestSuite {
  import CompanionImplicits.*

  test(
    "TXTransactionByIdLedgerEffectsBasic",
    "Expose a visible transaction tree by identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      dummy <- ledger.create(party, new Dummy(party))
      tree <- ledger.exercise(party, dummy.exerciseDummyChoice1())
      byId <- ledger.transactionById(tree.updateId, Seq(party), LedgerEffects)
    } yield {
      assertEquals("The transaction fetched by identifier does not match", tree, byId)
      assertAcsDelta(
        tree.events,
        acsDelta = true,
        "The acs_delta field in exercised events should be set",
      )
    }
  })

  test(
    "TXLedgerEffectsInvisibleUpdateByIdOtherParty",
    "Do not expose an invisible transaction by identifier for an other party (LedgerEffects)",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case p @ Participants(Participant(alpha, Seq(party)), Participant(beta, Seq(intruder))) =>
      for {
        dummy <- alpha.create(party, new Dummy(party))
        transaction <- alpha.exercise(party, dummy.exerciseDummyChoice1())
        _ <- p.synchronize
        failure <- beta
          .transactionById(transaction.updateId, Seq(intruder), LedgerEffects)
          .mustFail("subscribing to an invisible transaction")
      } yield {
        assertGrpcError(
          failure,
          RequestValidationErrors.NotFound.Update,
          Some("Update not found, or not visible."),
        )
      }
  })

  test(
    "TXLedgerEffectsInvisibleUpdateByIdOtherTemplate",
    "Do not expose an invisible transaction by identifier for an other template (LedgerEffects)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, Seq(party))) =>
    for {
      dummy <- alpha.create(party, new Dummy(party))
      transaction <- alpha.exercise(party, dummy.exerciseDummyChoice1())
      failure <- alpha
        .transactionById(
          updateId = transaction.updateId,
          parties = Seq(party),
          templateIds = Seq(DummyWithParam.TEMPLATE_ID),
          transactionShape = LedgerEffects,
        )
        .mustFail("subscribing to an invisible transaction")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.NotFound.Update,
        Some("Update not found, or not visible."),
      )
    }
  })

  test(
    "TXLedgerEffectsUpdateByIdNotFound",
    "Return NOT_FOUND when looking up a non-existent transaction by identifier (LedgerEffects)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      failure <- ledger
        .transactionById(
          TestUpdateId("a" * 60).toHexString,
          Seq(party),
          LedgerEffects,
        )
        .mustFail("looking up an non-existent transaction")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.NotFound.Update,
        Some("Update not found, or not visible."),
      )
    }
  })

  test(
    "TXAcsDeltaUpdateByIdBasic",
    "Expose a visible transaction by identifier (AcsDelta)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      dummy <- ledger.create(party, new Dummy(party))
      transaction <- ledger.exercise(party, dummy.exerciseDummyChoice1(), AcsDelta)
      byId <- ledger.transactionById(transaction.updateId, Seq(party), AcsDelta)
    } yield {
      assertEquals("The transaction fetched by identifier does not match", transaction, byId)
    }
  })

  test(
    "TXAcsDeltaUpdateByIdCreateArgumentsNonEmpty",
    "Include contract arguments in fetching a transaction by identifier (AcsDelta)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      response <- ledger.submitAndWaitForTransaction(
        ledger.submitAndWaitForTransactionRequest(party, new Dummy(party).create().commands)
      )
      tx = response.getTransaction
      byId <- ledger.transactionById(tx.updateId, Seq(party), AcsDelta)
      // archive the created contract to not pollute the ledger
      contractId = Dummy.COMPANION.toContractId(
        new ContractId(tx.events.head.getCreated.contractId)
      )
      _ <- ledger.exercise(party, contractId.exerciseArchive())
    } yield {
      checkArgumentsNonEmpty(byId.events.head.getCreated)
    }
  })

  test(
    "TXAcsDeltaInvisibleUpdateByIdOtherParty",
    "Do not expose an invisible transaction by identifier for an other party (AcsDelta)",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party, intruder))) =>
    for {
      dummy <- ledger.create(party, new Dummy(party))
      tree <- ledger.exercise(party, dummy.exerciseDummyChoice1())
      failure <- ledger
        .transactionById(tree.updateId, Seq(intruder), AcsDelta)
        .mustFail("looking up an invisible flat transaction")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.NotFound.Update,
        Some("Update not found, or not visible."),
      )
    }
  })

  test(
    "TXAcsDeltaInvisibleUpdateByIdOtherTemplate",
    "Do not expose an invisible transaction by identifier for an other template (AcsDelta)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, Seq(party))) =>
    for {
      dummy <- alpha.create(party, new Dummy(party))
      transaction <- alpha.exercise(party, dummy.exerciseDummyChoice1())
      failure <- alpha
        .transactionById(
          updateId = transaction.updateId,
          parties = Seq(party),
          templateIds = Seq(DummyWithParam.TEMPLATE_ID),
          transactionShape = AcsDelta,
        )
        .mustFail("subscribing to an invisible transaction")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.NotFound.Update,
        Some("Update not found, or not visible."),
      )
    }
  })

  test(
    "TXAcsDeltaUpdateByIdNotFound",
    "Return NOT_FOUND when looking up a non-existent transaction by identifier (AcsDelta)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      failure <- ledger
        .transactionById(
          TestUpdateId("a" * 60).toHexString,
          Seq(party),
          AcsDelta,
        )
        .mustFail("looking up a non-existent transaction")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.NotFound.Update,
        Some("Update not found, or not visible."),
      )
    }
  })

  test(
    "TXLedgerEffectsUpdateByOffsetBasic",
    "Expose a visible transaction by offset (Ledger Effects)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      dummy <- ledger.create(party, new Dummy(party))
      tx <- ledger.exercise(party, dummy.exerciseDummyChoice1())
      byOffset <- ledger.transactionByOffset(tx.offset, Seq(party), LedgerEffects)
    } yield {
      assertEquals("The transaction fetched by offset does not match", tx, byOffset)
    }
  })

  test(
    "TXLegerEffectsInvisibleUpdateByOffsetOtherParty",
    "Do not expose an invisible transaction by offset for an other party (LedgerEffects)",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case p @ Participants(Participant(alpha, Seq(party)), Participant(beta, Seq(intruder))) =>
      for {
        dummy <- alpha.create(party, new Dummy(party))
        tree <- alpha.exercise(party, dummy.exerciseDummyChoice1())
        _ <- p.synchronize
        failure <- beta
          .transactionByOffset(tree.offset, Seq(intruder), LedgerEffects)
          .mustFail("looking up an invisible transaction")
      } yield {
        assertGrpcError(
          failure,
          RequestValidationErrors.NotFound.Update,
          Some("Update not found, or not visible."),
        )
      }
  })

  test(
    "TXLegerEffectsInvisibleUpdateByOffsetOtherTemplate",
    "Do not expose an invisible transaction by offset for an other template (LedgerEffects)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, Seq(party))) =>
    for {
      dummy <- alpha.create(party, new Dummy(party))
      ledgerEffectsTx <- alpha.exercise(party, dummy.exerciseDummyChoice1())
      failure <- alpha
        .transactionByOffset(
          offset = ledgerEffectsTx.offset,
          parties = Seq(party),
          transactionShape = LedgerEffects,
          templateIds = Seq(DummyWithParam.TEMPLATE_ID),
        )
        .mustFail("looking up an invisible transaction")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.NotFound.Update,
        Some("Update not found, or not visible."),
      )
    }
  })

  test(
    "TXLegerEffectsUpdateByOffsetNotFound",
    "Return NOT_FOUND when looking up a non-existent transaction by offset (LedgerEffects)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      failure <- ledger
        .transactionByOffset(21 * 60, Seq(party), LedgerEffects)
        .mustFail("looking up a non-existent transaction tree")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.NotFound.Update,
        Some("Update not found, or not visible."),
      )
    }
  })

  test(
    "TXAcsDeltaUpdateByOffsetBasic",
    "Expose a visible transaction by offset (AcsDelta)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      dummy <- ledger.create(party, new Dummy(party))
      transaction <- ledger.exercise(party, dummy.exerciseDummyChoice1(), AcsDelta)
      event = transaction.events.head.event
      offset = event.archived.map(_.offset).get
      byOffset <- ledger.transactionByOffset(offset, Seq(party), AcsDelta)
    } yield {
      assertEquals("The transaction fetched by offset does not match", transaction, byOffset)
    }
  })

  test(
    "TXAcsDeltaUpdateByOffsetCreateArgumentsNonEmpty",
    "Include contract arguments in fetching a transaction by offset (AcsDelta)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      response <- ledger.submitAndWaitForTransaction(
        ledger.submitAndWaitForTransactionRequest(party, new Dummy(party).create().commands)
      )
      tx = response.getTransaction
      byOffset <- ledger.transactionByOffset(tx.offset, Seq(party), AcsDelta)
      // archive the created contract to not pollute the ledger
      contractId = Dummy.COMPANION.toContractId(
        new ContractId(tx.events.head.getCreated.contractId)
      )
      _ <- ledger.exercise(party, contractId.exerciseArchive())
    } yield {
      checkArgumentsNonEmpty(byOffset.events.head.getCreated)
    }
  })

  test(
    "TXAcsDeltaInvisibleUpdateByOffsetOtherParty",
    "Do not expose an invisible transaction by event for an other party identifier (AcsDelta)",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party, intruder))) =>
    for {
      dummy <- ledger.create(party, new Dummy(party))
      tree <- ledger.exercise(party, dummy.exerciseDummyChoice1())
      failure <- ledger
        .transactionByOffset(tree.offset, Seq(intruder), AcsDelta)
        .mustFail("looking up an invisible transaction")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.NotFound.Update,
        Some("Update not found, or not visible."),
      )
    }
  })

  test(
    "TXAcsDeltaInvisibleUpdateByOffsetOtherTemplate",
    "Do not expose an invisible transaction by offset for an other template (AcsDelta)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, Seq(party))) =>
    for {
      dummy <- alpha.create(party, new Dummy(party))
      tree <- alpha.exercise(party, dummy.exerciseDummyChoice1())
      failure <- alpha
        .transactionByOffset(
          offset = tree.offset,
          parties = Seq(party),
          transactionShape = AcsDelta,
          templateIds = Seq(DummyWithParam.TEMPLATE_ID),
        )
        .mustFail("looking up an invisible transaction")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.NotFound.Update,
        Some("Update not found, or not visible."),
      )
    }
  })

  test(
    "TXAcsDeltaUpdateByOffsetNotFound",
    "Return NOT_FOUND when looking up a non-existent transaction by offset (AcsDelta)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      failure <- ledger
        .transactionByOffset(21 * 60, Seq(party), AcsDelta)
        .mustFail("looking up a non-existent flat transaction")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.NotFound.Update,
        Some("Update not found, or not visible."),
      )
    }
  })

  test(
    "TXUpdateByIdTransientContract",
    "GetUpdateById returns NOT_FOUND for AcsDelta on a transient contract",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(owner))) =>
    for {
      response <- ledger.submitAndWait(
        ledger.submitAndWaitRequest(owner, new Dummy(owner).createAnd().exerciseArchive().commands)
      )
      failure <- ledger
        .transactionById(
          updateId = response.updateId,
          parties = Seq(owner),
          transactionShape = AcsDelta,
        )
        .mustFail("acs delta lookup")
      ledgerEffectsResult <- ledger.transactionById(
        updateId = response.updateId,
        parties = Seq(owner),
        transactionShape = LedgerEffects,
      )
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.NotFound.Update,
        Some("Update not found, or not visible."),
      )
      assertEquals(
        "ledgerEffectsResult should not be empty",
        ledgerEffectsResult.events.nonEmpty,
        true,
      )
    }
  })

  test(
    "TXUpdateByOffsetTransientContract",
    "GetUpdateByOffset returns NOT_FOUND on a transient contract",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(owner))) =>
    for {
      response <- ledger.submitAndWaitForTransaction(
        ledger.submitAndWaitForTransactionRequest(
          party = owner,
          commands = new Dummy(owner).createAnd().exerciseArchive().commands,
          transactionShape = LedgerEffects,
        )
      )
      offset = response.transaction.get.offset
      failure <- ledger
        .transactionByOffset(offset, Seq(owner), AcsDelta)
        .mustFail("acs delta lookup")
      ledgerEffectsResult <- ledger.transactionByOffset(offset, Seq(owner), LedgerEffects)
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.NotFound.Update,
        Some("Update not found, or not visible."),
      )
      assertEquals(
        "ledgerEffectsResult should not be empty",
        ledgerEffectsResult.events.nonEmpty,
        true,
      )
    }
  })

  test(
    "TXUpdateByIdNonConsumingChoice",
    "GetUpdateById returns NOT_FOUND when command contains only a non-consuming choice",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(owner))) =>
    for {
      contractId: Dummy.ContractId <- ledger.create(owner, new Dummy(owner))
      response <- ledger.submitAndWait(
        ledger.submitAndWaitRequest(owner, contractId.exerciseDummyNonConsuming().commands)
      )
      failure <-
        ledger
          .transactionById(
            updateId = response.updateId,
            parties = Seq(owner),
            transactionShape = AcsDelta,
          )
          .mustFail("looking up an non-existent transaction")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.NotFound.Update,
        Some("Update not found, or not visible."),
      )
    }
  })

  test(
    "TXUpdateByOffsetNonConsumingChoice",
    "GetUpdateByOffset returns NOT_FOUND when command contains only a non-consuming choice",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(owner))) =>
    for {
      contractId: Dummy.ContractId <- ledger.create(owner, new Dummy(owner))
      response <- ledger.submitAndWaitForTransaction(
        ledger.submitAndWaitForTransactionRequest(
          owner,
          contractId.exerciseDummyNonConsuming().commands,
        )
      )
      offset = response.transaction.get.offset
      failure <- ledger
        .transactionByOffset(offset, Seq(owner), AcsDelta)
        .mustFail("looking up an non-existent transaction")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.NotFound.Update,
        Some("Update not found, or not visible."),
      )
    }
  })

  test(
    "TopologyTXByOffsetBasic",
    "Expose a visible topology transaction by offset",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, _)) =>
    for {
      begin <- ledger.currentEnd()
      party <- ledger.allocateParty()
      // For synchronization
      topologyTx <- ledger.participantAuthorizationTransaction(
        partyIdSubstring = "TopologyTXByOffsetBasic",
        begin = Some(begin),
      )
      byOffset <- ledger.topologyTransactionByOffset(topologyTx.offset, Seq(party))
      byId <- ledger.topologyTransactionById(topologyTx.updateId, Seq(party))
    } yield {
      assertEquals(
        "The topology transactions fetched by identifier and by offset do not match",
        byOffset,
        byId,
      )
    }
  })

  test(
    "TopologyTXPartyWildcard",
    "Expose a visible topology transaction without specifying parties",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, _)) =>
    for {
      begin <- ledger.currentEnd()
      _ <- ledger.allocateParty()
      // For synchronization
      topologyTx <- ledger.participantAuthorizationTransaction(
        partyIdSubstring = "TopologyTXPartyWildcard",
        begin = Some(begin),
      )
      byOffset <- ledger.topologyTransactionByOffset(topologyTx.offset, Seq())
      byId <- ledger.topologyTransactionById(topologyTx.updateId, Seq())
    } yield {
      assertEquals(
        "The topology transactions fetched by identifier and by offset do not match",
        byOffset,
        byId,
      )
    }
  })

  test(
    "TopologyTXInvisibleByIdOtherParty",
    "Do not expose an invisible topology transaction by identifier for an other party",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(intruder))) =>
    for {
      begin <- ledger.currentEnd()
      _ <- ledger.allocateParty()
      // For synchronization
      topologyTx <- ledger.participantAuthorizationTransaction(
        partyIdSubstring = "TopologyTXInvisibleByIdOtherParty",
        begin = Some(begin),
      )
      failure <- ledger
        .topologyTransactionById(topologyTx.updateId, Seq(intruder))
        .mustFail("subscribing to an invisible topology transaction")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.NotFound.Update,
        Some("Update not found, or not visible."),
      )
    }
  })

  test(
    "TopologyTXInvisibleById",
    "Return NOT_FOUND when looking up a non-existent topology transaction",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, _)) =>
    for {
      failure <- ledger
        .topologyTransactionById(
          TestUpdateId("a" * 60).toHexString,
          Seq.empty,
        )
        .mustFail("looking up an non-existent transaction")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.NotFound.Update,
        Some("Update not found, or not visible."),
      )
    }
  })

  test(
    "TopologyTXInvisibleByOffsetOtherParty",
    "Do not expose an invisible topology transaction by offset for an other party",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(intruder))) =>
    for {
      begin <- ledger.currentEnd()
      party <- ledger.allocateParty()
      // For synchronization
      topologyTx <- ledger.participantAuthorizationTransaction(
        partyIdSubstring = "TopologyTXInvisibleByOffsetOtherParty",
        begin = Some(begin),
      )
      failure <- ledger
        .topologyTransactionByOffset(topologyTx.offset, Seq(intruder))
        .mustFail("subscribing to an invisible topology transaction")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.NotFound.Update,
        Some("Update not found, or not visible."),
      )
    }
  })

  test(
    "TopologyTXInvisibleByOffset",
    "Return NOT_FOUND when looking up a non-existent topology transaction",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, _)) =>
    for {
      failure <- ledger
        .topologyTransactionByOffset(4200 * 60, Seq.empty)
        .mustFail("looking up an non-existent transaction")
    } yield {
      assertGrpcError(
        failure,
        RequestValidationErrors.NotFound.Update,
        Some("Update not found, or not visible."),
      )
    }
  })

  def checkArgumentsNonEmpty(event: CreatedEvent): Unit =
    assertEquals(
      s"Create event $event createArguments must NOT be empty",
      event.createArguments.isEmpty,
      false,
    )

}
