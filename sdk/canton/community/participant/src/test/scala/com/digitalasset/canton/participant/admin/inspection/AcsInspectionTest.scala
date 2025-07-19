// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.inspection

import cats.Eval
import cats.data.OptionT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.admin.inspection.AcsInspectionTest.{
  fakeSynchronizerId,
  readAllVisibleActiveContracts,
}
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.ContractIdSyntax.orderingLfContractId
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, LfPartyId, LfTimestamp, LfValue, ReassignmentCounter}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.CreationTime
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

    val emptyState = AcsInspectionTest.mockSyncPersistentState(contracts = Map.empty)

    "the snapshot is empty" should {
      "return an empty result" in {
        for (contracts <- readAllVisibleActiveContracts(emptyState, Set.empty))
          yield {
            contracts.value shouldBe empty
          }
      }
    }

    def contract(c: Char) = ExampleContractFactory.buildContractId(c.toInt)
    def party(s: String) = LfPartyId.assertFromString(s)

    val consistent = AcsInspectionTest.mockSyncPersistentState(contracts =
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

    val inconsistent = AcsInspectionTest.mockSyncPersistentState(
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
              fakeSynchronizerId,
              contract('2'),
            )
          }
      }
    }
  }

}

object AcsInspectionTest extends MockitoSugar with ArgumentMatchersSugar with BaseTest {

  private val fakeSynchronizerId = SynchronizerId.tryFromString(s"acme::${"0" * 68}")

  private val MaxSynchronizerIndex: SynchronizerIndex =
    SynchronizerIndex.of(CantonTimestamp.MaxValue)

  private def mockContract(
      contractId: LfContractId,
      stakeholders: Set[LfPartyId],
  ): ContractInstance =
    ExampleContractFactory.build(
      signatories = stakeholders.take(1),
      stakeholders = stakeholders,
      createdAt = CreationTime.CreatedAt(LfTimestamp.Epoch),
      version = LfLanguageVersion.v2_dev,
      packageName = Ref.PackageName.assertFromString("pkg-name"),
      templateId = Ref.Identifier.assertFromString("pkg:Mod:Template"),
      argument = LfValue.ValueNil,
      overrideContractId = Some(contractId),
    )

  private def mockSyncPersistentState(
      contracts: Map[LfContractId, Set[LfPartyId]],
      missingContracts: Set[LfContractId] = Set.empty,
  )(implicit ec: ExecutionContext): SyncPersistentState = {
    implicit def mockedTraceContext: TraceContext = any[TraceContext]

    val allContractIds = contracts.keys ++ missingContracts

    val snapshot =
      allContractIds.map(
        _ -> (TimeOfChange(CantonTimestamp.Epoch), ReassignmentCounter.Genesis)
      )

    val acs = mock[ActiveContractStore]
    when(acs.snapshot(any[TimeOfChange])(mockedTraceContext))
      .thenAnswer(FutureUnlessShutdown.pure(SortedMap.from(snapshot)))

    val cs = mock[ContractStore]
    when(cs.lookupManyExistingUncached(any[Seq[LfContractId]])(mockedTraceContext))
      .thenAnswer { (contractIds: Seq[LfContractId]) =>
        OptionT
          .fromOption[FutureUnlessShutdown](NonEmpty.from(contractIds.filter(missingContracts)))
          .map(_.head)
          .toLeft {
            contracts.view.collect {
              case (id, stakeholders) if contractIds.contains(id) => mockContract(id, stakeholders)
            }.toList
          }
      }

    val logicalState = mock[LogicalSyncPersistentState]
    when(logicalState.activeContractStore).thenAnswer(acs)
    when(logicalState.synchronizerIdx).thenAnswer(
      IndexedSynchronizer.tryCreate(fakeSynchronizerId, 1)
    )

    val acsInspection = new AcsInspection(fakeSynchronizerId, acs, cs, Eval.now(mockLedgerApiStore))
    when(logicalState.acsInspection).thenAnswer(acsInspection)

    val physicalState = mock[PhysicalSyncPersistentState]
    val rjs = mock[RequestJournalStore]
    when(physicalState.requestJournalStore).thenAnswer(rjs)

    new SyncPersistentState(logicalState, physicalState, loggerFactory)
  }

  private val mockLedgerApiStore: LedgerApiStore = {
    val mockStore = mock[LedgerApiStore]
    when(
      mockStore
        .cleanSynchronizerIndex(same(fakeSynchronizerId))(any[TraceContext], any[ExecutionContext])
    )
      .thenAnswer(FutureUnlessShutdown.pure(Some(MaxSynchronizerIndex)))
    mockStore
  }

  private def readAllVisibleActiveContracts(
      state: SyncPersistentState,
      parties: Set[LfPartyId],
  )(implicit
      ec: ExecutionContext
  ): Future[Either[AcsInspectionError, Vector[ContractInstance]]] =
    TraceContext.withNewTraceContext("read_visible_active_contracts") { implicit tc =>
      val builder = Vector.newBuilder[ContractInstance]
      state.acsInspection
        .forEachVisibleActiveContract(
          fakeSynchronizerId,
          parties,
          timeOfSnapshotO = None,
        ) { case (contract, _) =>
          builder += contract
          Either.unit
        }(tc, ec)
        .failOnShutdown
        .map(_ => builder.result())
        .value
    }

}
