// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.inspection

import cats.data.OptionT
import com.digitalasset.daml.lf.data.Ref
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.admin.inspection.AcsInspectionTest.{
  FakeDomainId,
  readAllVisibleActiveContracts,
}
import com.digitalasset.canton.participant.store.{
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
  LfTransactionVersion,
  SerializableContract,
  SerializableRawContractInstance,
}
import com.digitalasset.canton.store.CursorPrehead
import com.digitalasset.canton.store.CursorPrehead.RequestCounterCursorPrehead
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{
  LfPartyId,
  LfValue,
  LfVersioned,
  RequestCounter,
  RequestCounterDiscriminator,
  TransferCounter,
}
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
        for (contracts <- readAllVisibleActiveContracts(emptyState, Set.empty)) yield {
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
        for (contracts <- readAllVisibleActiveContracts(consistent, Set(party("a"), party("b"))))
          yield {
            contracts.value.map(_.contractId) should contain.allOf(
              contract('0'),
              contract('1'),
              contract('2'),
            )
          }
      }
      "return the correct subset of data when filtering by party" in {
        for (contracts <- readAllVisibleActiveContracts(consistent, Set(party("a"))))
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
        for (contracts <- readAllVisibleActiveContracts(inconsistent, Set(party("b"))))
          yield {
            contracts.left.value shouldBe Error.InconsistentSnapshot(FakeDomainId, contract('2'))
          }
      }
    }
  }

}

object AcsInspectionTest extends MockitoSugar with ArgumentMatchersSugar {

  private val FakeDomainId = DomainId.tryFromString(s"acme::${"0" * 68}")

  private val MaxCursorPrehead: RequestCounterCursorPrehead =
    CursorPrehead[RequestCounterDiscriminator](RequestCounter(0), CantonTimestamp.MaxValue)

  private val MockedSerializableRawContractInstance =
    SerializableRawContractInstance
      .create(
        LfVersioned(
          LfTransactionVersion.VDev,
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
    StoredContract(serializableContract, RequestCounter.MaxValue, None)
  }

  private def mockSyncDomainPersistentState(
      contracts: Map[LfContractId, Set[LfPartyId]],
      missingContracts: Set[LfContractId] = Set.empty,
  )(implicit ec: ExecutionContext): SyncDomainPersistentState = {
    implicit def mockedTraceContext: TraceContext = any[TraceContext]

    val allContractIds = contracts.keys ++ missingContracts

    val snapshot = allContractIds.map(_ -> (CantonTimestamp.Epoch, TransferCounter.Genesis))

    val acs = mock[ActiveContractStore]
    when(acs.snapshot(any[CantonTimestamp]))
      .thenAnswer(Future.successful(SortedMap.from(snapshot)))

    val cs = mock[ContractStore]
    when(cs.lookupManyExistingUncached(any[Seq[LfContractId]]))
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
    when(rjs.preheadClean)
      .thenAnswer(Future.successful(Option(AcsInspectionTest.MaxCursorPrehead)))

    val state = mock[SyncDomainPersistentState]

    when(state.contractStore).thenAnswer(cs)
    when(state.activeContractStore).thenAnswer(acs)
    when(state.requestJournalStore).thenAnswer(rjs)

    state
  }

  private def readAllVisibleActiveContracts(
      state: SyncDomainPersistentState,
      parties: Set[LfPartyId],
  )(implicit ec: ExecutionContext): Future[Either[Error, Vector[SerializableContract]]] =
    TraceContext.withNewTraceContext { implicit tc =>
      val builder = Vector.newBuilder[SerializableContract]
      AcsInspection
        .forEachVisibleActiveContract(
          FakeDomainId,
          state,
          parties,
          timestamp = None,
        ) { case (contract, _) =>
          builder += contract
          Right(())
        }
        .map(_ => builder.result())
        .value
    }

}
