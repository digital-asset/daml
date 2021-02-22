// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.util.concurrent.CompletableFuture

import com.daml.ledger.api.domain.PartyDetails
import com.daml.ledger.participant.state.index.v2.{
  IndexPartyManagementService,
  IndexSubmissionService,
}
import com.daml.ledger.participant.state.v1.{Party, SubmissionId, SubmissionResult, WriteService}
import com.daml.lf.data.Ref
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import org.mockito.captor.ArgCaptor
import org.mockito.{ArgumentMatchersSugar, Mockito, MockitoSugar}
import org.scalatest.{BeforeAndAfter, OneInstancePerTest}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class ApiSubmissionServiceSpec
    extends AsyncFlatSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar
    with OneInstancePerTest
    with BeforeAndAfter {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val builder = TransactionBuilder()
  private val knownParties = (1 to 100).map(idx => s"party-$idx").toArray
  private val knownPartiesSet = knownParties.toSet
  private val missingParties = (101 to 200).map(idx => s"party-$idx").toArray
  private val allInformeesInTransaction = knownParties ++ missingParties
  for {
    i <- 0 until 100
  } {
    // Ensure 100 % overlap by having 4 informees per each of the 100 nodes
    val informeesOfNode = allInformeesInTransaction.slice(i * 4, (i + 1) * 4)
    val (signatories, observers) = informeesOfNode.splitAt(2)
    builder.add(
      builder.create(
        s"#contractId$i",
        "template:test:test",
        Value.ValueNil,
        signatories.toSeq,
        observers.toSeq,
        Option.empty,
      )
    )
  }
  private val transaction = builder.buildSubmitted()

  private val partyManagementService = mock[IndexPartyManagementService]
  private val writeService = mock[WriteService]
  private val service =
    submissionService(writeService, partyManagementService, implicitPartyAllocation = true)

  before {
    when(writeService.allocateParty(any[Option[Party]], any[Option[Party]], any[SubmissionId]))
      .thenReturn(CompletableFuture.completedFuture(SubmissionResult.Acknowledged))
  }

  behavior of "allocateMissingInformees"

  it should "allocate missing informees" in {
    val argCaptor = ArgCaptor[Seq[Party]]

    when(partyManagementService.getParties(argCaptor.capture)(any[LoggingContext])).thenAnswer(
      Future.successful(
        argCaptor.value
          .filter(knownPartiesSet)
          .map(PartyDetails(_, Option.empty, isLocal = true))
          .toList
      )
    )

    for {
      results <- service.allocateMissingInformees(transaction)
    } yield {
      results.find(_ != SubmissionResult.Acknowledged) shouldBe None
      results.size shouldBe 100
      Mockito.mockingDetails(partyManagementService).getInvocations.size() shouldBe 1
      Mockito.mockingDetails(writeService).getInvocations.size() shouldBe 100
      missingParties.foreach(party =>
        verify(writeService).allocateParty(
          eqTo(Some(Ref.Party.assertFromString(party))),
          eqTo(Some(Ref.Party.assertFromString(party))),
          any[SubmissionId],
        )
      )
      verifyNoMoreInteractions(writeService)
      succeed
    }
  }

  it should "not allocate if all parties are already known" in {
    val argCaptor = ArgCaptor[Seq[Party]]
    when(partyManagementService.getParties(argCaptor.capture)(any[LoggingContext])).thenAnswer(
      Future.successful(argCaptor.value.map(PartyDetails(_, Option.empty, isLocal = true)).toList)
    )

    for {
      result <- service.allocateMissingInformees(transaction)
    } yield {
      verifyZeroInteractions(writeService)
      result shouldBe Seq.empty[SubmissionResult]
    }
  }

  it should "not allocate missing informees if implicit party allocation is disabled" in {
    val service = submissionService(null, null, implicitPartyAllocation = false)
    for {
      result <- service.allocateMissingInformees(transaction)
    } yield {
      result shouldBe Seq.empty[SubmissionResult]
    }
  }

  it should "forward SubmissionResult if it failed" in {
    val party = "party-1"
    val typedParty = Ref.Party.assertFromString(party)
    val submissionFailure = SubmissionResult.InternalError(s"failed to allocate $party")
    when(writeService.allocateParty(eqTo(Some(typedParty)), eqTo(Some(party)), any[SubmissionId]))
      .thenReturn(CompletableFuture.completedFuture(submissionFailure))
    when(partyManagementService.getParties(eqTo(Seq(typedParty)))(any[LoggingContext])).thenAnswer(
      Future(List.empty[PartyDetails])
    )
    val builder = TransactionBuilder()
    builder.add(
      builder.create(
        s"#contractId1",
        "template:test:test",
        Value.ValueNil,
        Seq(party),
        Seq.empty,
        Option.empty,
      )
    )
    val transaction = builder.buildSubmitted()

    for {
      result <- service.allocateMissingInformees(transaction)
    } yield {
      result shouldBe Seq(submissionFailure)
    }
  }

  def submissionService(
      writeService: WriteService,
      partyManagementService: IndexPartyManagementService,
      implicitPartyAllocation: Boolean,
  ) = new ApiSubmissionService(
    writeService = writeService,
    submissionService = mock[IndexSubmissionService],
    partyManagementService = partyManagementService,
    timeProvider = null,
    timeProviderType = null,
    ledgerConfigProvider = null,
    seedService = null,
    commandExecutor = null,
    configuration = ApiSubmissionService.Configuration(implicitPartyAllocation),
    metrics = null,
  )
}
