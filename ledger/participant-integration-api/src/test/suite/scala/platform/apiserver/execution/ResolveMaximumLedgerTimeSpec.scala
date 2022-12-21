// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.execution

import com.daml.ledger.participant.state.index.v2.{MaximumLedgerTime, MaximumLedgerTimeService}
import com.daml.lf.command.{EngineEnrichedContractMetadata, ProcessedDisclosedContract}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.Identifier
import com.daml.lf.data.{ImmArray, Time}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext
import org.mockito.captor.{ArgCaptor, Captor}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class ResolveMaximumLedgerTimeSpec
    extends AnyFlatSpec
    with Matchers
    with MockitoSugar
    with ScalaFutures
    with ArgumentMatchersSugar {

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  behavior of classOf[ResolveMaximumLedgerTime].getSimpleName

  it should "resolve maximum ledger time using disclosed contracts with fallback to contract store lookup" in new TestScope {
    private val disclosedContracts = ImmArray(
      buildDisclosedContract(cId_1, t1),
      buildDisclosedContract(cId_2, t2),
    )

    resolveMaximumLedgerTime(
      disclosedContracts,
      Set(cId_2, cId_3, cId_4),
    ).futureValue shouldBe MaximumLedgerTime.Max(t4)
  }

  it should "resolve maximum ledger time when all contracts are provided as explicitly disclosed" in new TestScope {
    private val disclosedContracts = ImmArray(
      buildDisclosedContract(cId_1, t1),
      buildDisclosedContract(cId_2, t2),
    )

    resolveMaximumLedgerTime(
      disclosedContracts,
      Set(cId_1, cId_2),
    ).futureValue shouldBe MaximumLedgerTime.Max(t2)
  }

  it should "resolve maximum ledger time when no disclosed contracts are provided" in new TestScope {
    resolveMaximumLedgerTime(
      ImmArray.empty,
      Set(cId_1, cId_2),
    ).futureValue shouldBe MaximumLedgerTime.Max(t2)
  }

  it should "forward contract store lookup result on archived contracts" in new TestScope {
    resolveMaximumLedgerTime(
      ImmArray.empty,
      archived.contracts + cId_1,
    ).futureValue shouldBe archived
  }

  private def buildDisclosedContract(cId: ContractId, createdAt: Time.Timestamp) =
    ProcessedDisclosedContract(
      templateId = Identifier.assertFromString("some:pkg:identifier"),
      contractId = cId,
      argument = Value.ValueNil,
      metadata = EngineEnrichedContractMetadata(
        createdAt = createdAt,
        driverMetadata = ImmArray.empty,
        signatories = Set.empty,
        stakeholders = Set.empty,
        maybeKeyWithMaintainers = None,
        agreementText = "",
      ),
    )

  private def contractId(id: Int): ContractId =
    ContractId.V1(Hash.hashPrivateKey(id.toString))

  private trait TestScope {
    val t1: Time.Timestamp = Time.Timestamp.assertFromLong(1L)
    val t2: Time.Timestamp = Time.Timestamp.assertFromLong(2L)
    val t3: Time.Timestamp = Time.Timestamp.assertFromLong(3L)
    val t4: Time.Timestamp = Time.Timestamp.assertFromLong(4L)

    val cId_1: ContractId = contractId(1)
    val cId_2: ContractId = contractId(2)
    val cId_3: ContractId = contractId(3)
    val cId_4: ContractId = contractId(4)
    val cId_5: ContractId = contractId(5)

    val archived: MaximumLedgerTime.Archived = MaximumLedgerTime.Archived(Set(cId_5))

    def mapping: Map[ContractId, Time.Timestamp] = Map(
      cId_1 -> t1,
      cId_2 -> t2,
      cId_3 -> t3,
      cId_4 -> t4,
    )

    val maximumLedgerTimeServiceMock: MaximumLedgerTimeService = mock[MaximumLedgerTimeService]
    val lookedUpCidsCaptor: Captor[Set[ContractId]] = ArgCaptor[Set[ContractId]]

    when(
      maximumLedgerTimeServiceMock.lookupMaximumLedgerTimeAfterInterpretation(
        lookedUpCidsCaptor.capture
      )(
        eqTo(loggingContext)
      )
    ).delegate.thenAnswer(new Answer[Future[MaximumLedgerTime]]() {
      override def answer(invocation: InvocationOnMock): Future[MaximumLedgerTime] = {
        val lookedUpCids = lookedUpCidsCaptor.value

        if (lookedUpCids.isEmpty) Future.successful(MaximumLedgerTime.NotAvailable)
        else if (archived.contracts.diff(lookedUpCids).isEmpty) Future.successful(archived)
        else Future.successful(MaximumLedgerTime.Max(lookedUpCids.map(mapping).max))
      }
    })

    val resolveMaximumLedgerTime = new ResolveMaximumLedgerTime(maximumLedgerTimeServiceMock)
  }
}
