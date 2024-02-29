// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.index

import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value.{ContractId, VersionedContractInstance}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ledger.participant.state.index.v2.{
  ContractState,
  ContractStore,
  MaximumLedgerTime,
  MaximumLedgerTimeService,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.concurrent.Future

class ContractStoreBasedMaximumLedgerTimeServiceSpec
    extends AsyncFlatSpec
    with Matchers
    with BaseTest {

  import ContractState.*
  import com.digitalasset.canton.ledger.participant.state.index.v2.MaximumLedgerTime.*

  private implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

  private val timestamp1 = timestampFromInstant(Instant.now())
  private val timestamp2 = timestamp1.addMicros(5000)
  private val timestamp3 = timestamp2.addMicros(5000)
  private val timestamp4 = timestamp3.addMicros(5000)

  private val contractId1 = hashCid("1")
  private val contractId2 = hashCid("2")
  private val contractId3 = hashCid("3")
  private val contractId4 = hashCid("4")

  behavior of "lookupMaximumLedgerTimeAfterInterpretation"

  it should "find the maximum ledger time on the happy path" in {
    testeeWithFixture(
      contractId1 -> active(timestamp1),
      contractId2 -> active(timestamp2),
      contractId3 -> active(timestamp3),
      contractId4 -> active(timestamp4),
    ).lookupMaximumLedgerTimeAfterInterpretation(
      Set(
        contractId1,
        contractId2,
        contractId3,
        contractId4,
      )
    ).map(
      _ shouldBe Max(timestamp4)
    )
  }

  it should "find the maximum ledger time if all contracts are active with same ledger time" in {
    testeeWithFixture(
      contractId1 -> active(timestamp1),
      contractId2 -> active(timestamp1),
      contractId3 -> active(timestamp1),
      contractId4 -> active(timestamp1),
    ).lookupMaximumLedgerTimeAfterInterpretation(
      Set(
        contractId1,
        contractId2,
        contractId3,
        contractId4,
      )
    ).map(
      _ shouldBe Max(timestamp1)
    )
  }

  it should "find no maximum ledger time if ids is empty" in {
    testeeWithFixture(
    ).lookupMaximumLedgerTimeAfterInterpretation(
      Set(
      )
    ).map(
      _ shouldBe NotAvailable
    )
  }

  it should "find the maximum ledger time if there for only one active contract" in {
    testeeWithFixture(
      contractId1 -> active(timestamp1)
    ).lookupMaximumLedgerTimeAfterInterpretation(
      Set(
        contractId1
      )
    ).map(
      _ shouldBe Max(timestamp1)
    )
  }

  it should "find no maximum ledger time if there are some contracts which cannot be found" in {
    testeeWithFixture(
      contractId1 -> active(timestamp1),
      contractId2 -> NotFound,
      contractId3 -> active(timestamp3),
      contractId4 -> NotFound,
    ).lookupMaximumLedgerTimeAfterInterpretation(
      Set(
        contractId1,
        contractId2,
        contractId3,
        contractId4,
      )
    ).map(
      _ shouldBe MaximumLedgerTime.Archived(Set(contractId2))
    )
  }

  it should "find no maximum ledger time if none of the contracts can be found" in {
    testeeWithFixture(
      contractId1 -> NotFound,
      contractId2 -> NotFound,
      contractId3 -> NotFound,
      contractId4 -> NotFound,
    ).lookupMaximumLedgerTimeAfterInterpretation(
      Set(
        contractId1,
        contractId2,
        contractId3,
        contractId4,
      )
    ).map(
      _ shouldBe MaximumLedgerTime.Archived(Set(contractId1))
    )
  }

  it should "find no maximum ledger time if for the one contract cannot be found" in {
    testeeWithFixture(
      contractId1 -> NotFound
    ).lookupMaximumLedgerTimeAfterInterpretation(
      Set(
        contractId1
      )
    ).map(
      _ shouldBe MaximumLedgerTime.Archived(Set(contractId1))
    )
  }

  it should "return the archived contract, if one of the contracts is archived" in {
    testeeWithFixture(
      contractId1 -> active(timestamp1),
      contractId2 -> active(timestamp2),
      contractId3 -> ContractState.Archived,
      contractId4 -> active(timestamp4),
    ).lookupMaximumLedgerTimeAfterInterpretation(
      Set(
        contractId1,
        contractId2,
        contractId3,
        contractId4,
      )
    ).map(
      _ shouldBe MaximumLedgerTime.Archived(Set(contractId3))
    )
  }

  it should "return one of the archived contracts, if two of the contracts are archived" in {
    testeeWithFixture(
      contractId1 -> ContractState.Archived,
      contractId2 -> active(timestamp2),
      contractId3 -> ContractState.Archived,
      contractId4 -> active(timestamp4),
    ).lookupMaximumLedgerTimeAfterInterpretation(
      Set(
        contractId1,
        contractId2,
        contractId3,
        contractId4,
      )
    ).map { result =>
      inside(result) { case MaximumLedgerTime.Archived(archivedResults) =>
        archivedResults.size shouldBe 1
        val archivedResult = archivedResults.head
        Set(contractId1, contractId3) should contain(archivedResult)
      }
    }
  }

  it should "return one of the archived contracts, if all of the contracts are archived" in {
    testeeWithFixture(
      contractId1 -> ContractState.Archived,
      contractId2 -> ContractState.Archived,
      contractId3 -> ContractState.Archived,
      contractId4 -> ContractState.Archived,
    ).lookupMaximumLedgerTimeAfterInterpretation(
      Set(
        contractId1,
        contractId2,
        contractId3,
        contractId4,
      )
    ).map { result =>
      inside(result) { case MaximumLedgerTime.Archived(archivedResults) =>
        archivedResults.size shouldBe 1
        val archivedResult = archivedResults.head
        Set(contractId1, contractId2, contractId3, contractId4) should contain(archivedResult)
      }
    }
  }

  it should "return one of the archived contracts, if some of the contracts are archived, and some cannot be found" in {
    testeeWithFixture(
      contractId1 -> ContractState.Archived,
      contractId2 -> active(timestamp2),
      contractId3 -> ContractState.Archived,
      contractId4 -> NotFound,
    ).lookupMaximumLedgerTimeAfterInterpretation(
      Set(
        contractId1,
        contractId2,
        contractId3,
        contractId4,
      )
    ).map { result =>
      inside(result) { case MaximumLedgerTime.Archived(archivedResults) =>
        archivedResults.size shouldBe 1
        val archivedResult = archivedResults.head
        Set(contractId1, contractId3) should contain(archivedResult)
      }
    }
  }

  private def hashCid(key: String): ContractId = ContractId.V1(Hash.hashPrivateKey(key))

  private def timestampFromInstant(i: Instant): Timestamp = Timestamp.assertFromInstant(i)

  private def active(ledgerEffectiveTime: Timestamp): ContractState =
    Active(
      null,
      ledgerEffectiveTime,
      Set.empty,
      Set.empty,
      None,
      None,
      None,
    ) // we do not care about the payload here

  private def testeeWithFixture(fixture: (ContractId, ContractState)*): MaximumLedgerTimeService = {
    val fixtureMap = fixture.toMap
    new ContractStoreBasedMaximumLedgerTimeService(
      new ContractStore {
        override def lookupActiveContract(readers: Set[Party], contractId: ContractId)(implicit
            loggingContext: LoggingContextWithTrace
        ): Future[Option[VersionedContractInstance]] =
          throw new UnsupportedOperationException

        override def lookupContractKey(readers: Set[Party], key: GlobalKey)(implicit
            loggingContext: LoggingContextWithTrace
        ): Future[Option[ContractId]] =
          throw new UnsupportedOperationException

        override def lookupContractState(contractId: ContractId)(implicit
            loggingContext: LoggingContextWithTrace
        ): Future[ContractState] =
          Future.successful(fixtureMap(contractId))
      },
      loggerFactory,
    )
  }
}
