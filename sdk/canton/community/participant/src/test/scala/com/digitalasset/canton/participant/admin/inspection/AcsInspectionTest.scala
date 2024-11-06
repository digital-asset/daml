// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.inspection

import cats.Eval
import cats.data.OptionT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.{DomainIndex, RequestIndex}
import com.digitalasset.canton.participant.admin.inspection.AcsInspectionTest.{
  FakeDomainId,
  readAllVisibleActiveContracts,
}
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.store.{
  AcsInspection,
  AcsInspectionError,
  ActiveContractStore,
  ContractStore,
  RequestJournalStore,
  StoredContract,
  SyncDomainPersistentState,
}
import com.digitalasset.canton.protocol.ContractIdSyntax.orderingLfContractId
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.protocol.{
  ContractMetadata,
  LfContractId,
  LfLanguageVersion,
  SerializableContract,
  SerializableRawContractInstance,
}
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{
  BaseTest,
  LfPartyId,
  LfValue,
  LfVersioned,
  ReassignmentCounter,
  RequestCounter,
}
import com.digitalasset.daml.lf.data.Ref
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future}

final class AcsInspectionTest
    extends AsyncWordSpec
    with Matchers
    with EitherValues
    with MockitoSugar
    with ArgumentMatchersSugar {

  "AcsInspection.forEachVisibleActiveContract" when {

    val emptyState = AcsInspectionTest.mockSyncDomainPersistentState(contracts = Map.empty)

    "the snapshot is empty" should {
      "return an empty result" in {
        for (contracts <- readAllVisibleActiveContracts(emptyState, Set.empty))
          yield {
            contracts.value shouldBe empty
          }
      }
    }

    def contract(c: Char) = LfContractId.assertFromString(s"${"0" * 67}$c")
    def party(s: String) = LfPartyId.assertFromString(s)

    val consistent = AcsInspectionTest.mockSyncDomainPersistentState(contracts =
      Map(
        contract('0') -> Set(party("a"), party("b")),
        contract('1') -> Set(party("a")),
        contract('2') -> Set(party("b")),
      )
    )

    "the snapshot contains relevant data" should {
      "return all data when filtering for all parties" in {
        for (
          contracts <- readAllVisibleActiveContracts(
            consistent,
            Set(party("a"), party("b")),
          )
        )
          yield {
            contracts.value.map(_.contractId) should contain.allOf(
              contract('0'),
              contract('1'),
              contract('2'),
            )
          }
      }
      "return the correct subset of data when filtering by party" in {
        for (
          contracts <- readAllVisibleActiveContracts(
            consistent,
            Set(party("a")),
          )
        )
          yield {
            contracts.value.map(_.contractId) should contain.allOf(
              contract('0'),
              contract('1'),
            )
          }
      }
    }

    val inconsistent = AcsInspectionTest.mockSyncDomainPersistentState(
      contracts = Map(
        contract('0') -> Set(party("a"), party("b")),
        contract('1') -> Set(party("a")),
      ),
      missingContracts = Set(contract('2')),
    )

    "the state is inconsistent" should {
      "return an error if the inconsistency is visible in the final result" in {
        for (
          contracts <- readAllVisibleActiveContracts(
            inconsistent,
            Set(party("b")),
          )
        )
          yield {
            contracts.left.value shouldBe AcsInspectionError.InconsistentSnapshot(
              FakeDomainId,
              contract('2'),
            )
          }
      }
    }
  }

}

object AcsInspectionTest extends MockitoSugar with ArgumentMatchersSugar with BaseTest {

  private val FakeDomainId = DomainId.tryFromString(s"acme::${"0" * 68}")

  private val MaxDomainIndex: DomainIndex =
    DomainIndex.of(
      RequestIndex(
        counter = RequestCounter(0),
        sequencerCounter = None,
        timestamp = CantonTimestamp.MaxValue,
      )
    )

  private val MockedSerializableRawContractInstance =
    SerializableRawContractInstance
      .create(
        LfVersioned(
          LfLanguageVersion.v2_dev,
          LfValue.ContractInstance(
            packageName = Ref.PackageName.assertFromString("pkg-name"),
            template = Ref.Identifier.assertFromString("pkg:Mod:Template"),
            arg = LfValue.ValueNil,
          ),
        )
      )
      .left
      .map(e => new RuntimeException(e.errorMessage))
      .toTry
      .get

  private def mockContract(
      contractId: LfContractId,
      stakeholders: Set[LfPartyId],
  ): StoredContract = {
    val metadata = ContractMetadata.tryCreate(stakeholders, stakeholders, None)
    val serializableContract = SerializableContract(
      contractId,
      MockedSerializableRawContractInstance,
      metadata,
      LedgerCreateTime(CantonTimestamp.Epoch),
      None,
    )
    StoredContract(serializableContract, RequestCounter.MaxValue, isDivulged = true)
  }

  private def mockSyncDomainPersistentState(
      contracts: Map[LfContractId, Set[LfPartyId]],
      missingContracts: Set[LfContractId] = Set.empty,
  )(implicit ec: ExecutionContext): SyncDomainPersistentState = {
    implicit def mockedTraceContext: TraceContext = any[TraceContext]

    val allContractIds = contracts.keys ++ missingContracts

    val snapshot = allContractIds.map(_ -> (CantonTimestamp.Epoch, ReassignmentCounter.Genesis))

    val acs = mock[ActiveContractStore]
    when(acs.snapshot(any[CantonTimestamp])(mockedTraceContext))
      .thenAnswer(Future.successful(SortedMap.from(snapshot)))

    val cs = mock[ContractStore]
    when(cs.lookupManyExistingUncached(any[Seq[LfContractId]])(mockedTraceContext))
      .thenAnswer { (contractIds: Seq[LfContractId]) =>
        OptionT
          .fromOption[Future](NonEmpty.from(contractIds.filter(missingContracts)))
          .map(_.head)
          .toLeft {
            contracts.view.collect {
              case (id, stakeholders) if contractIds.contains(id) => mockContract(id, stakeholders)
            }.toList
          }
      }

    val rjs = mock[RequestJournalStore]

    val state = mock[SyncDomainPersistentState]
    val acsInspection = new AcsInspection(FakeDomainId, acs, cs, Eval.now(mockLedgerApiStore))

    when(state.contractStore).thenAnswer(cs)
    when(state.activeContractStore).thenAnswer(acs)
    when(state.requestJournalStore).thenAnswer(rjs)
    when(state.indexedDomain).thenAnswer(IndexedDomain.tryCreate(FakeDomainId, 1))
    when(state.acsInspection).thenAnswer(acsInspection)

    state
  }

  private val mockLedgerApiStore: LedgerApiStore = {
    val mockStore = mock[LedgerApiStore]
    when(mockStore.domainIndex(same(FakeDomainId))(any[TraceContext]))
      .thenAnswer(Future.successful(MaxDomainIndex))
    mockStore
  }

  private def readAllVisibleActiveContracts(
      state: SyncDomainPersistentState,
      parties: Set[LfPartyId],
  )(implicit
      ec: ExecutionContext
  ): Future[Either[AcsInspectionError, Vector[SerializableContract]]] =
    TraceContext.withNewTraceContext { implicit tc =>
      val builder = Vector.newBuilder[SerializableContract]
      state.acsInspection
        .forEachVisibleActiveContract(
          FakeDomainId,
          parties,
          timestamp = None,
        ) { case (contract, _) =>
          builder += contract
          Either.unit
        }(tc, ec)
        .failOnShutdown
        .map(_ => builder.result())
        .value
    }

}
