// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.daml.lf.transaction.GlobalKey
import com.digitalasset.daml.lf.value.Value.{ValueText, ValueUnit}
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsContracts
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  import StorageBackendTestValues.*

  behavior of "StorageBackend (contracts)"

  it should "correctly find key states" in {
    val key1 = GlobalKey.assertBuild(
      Identifier.assertFromString("A:B:C"),
      ValueUnit,
      someTemplateId.pkg.name,
    )
    val key2 = GlobalKey.assertBuild(
      Identifier.assertFromString("A:B:C"),
      ValueText("value"),
      someTemplateId.pkg.name,
    )
    val internalContractId = 123L
    val internalContractId2 = 223L
    val internalContractId3 = 323L
    val internalContractId4 = 423L
    val signatory = Ref.Party.assertFromString("signatory")

    val dtos: Vector[DbDto] = Vector(
      dtosCreate(
        event_offset = 1L,
        event_sequential_id = 1L,
        internal_contract_id = internalContractId4,
        create_key_hash = Some(key2.hash.toHexString),
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosCreate(
        event_offset = 2L,
        event_sequential_id = 2L,
        internal_contract_id = internalContractId,
        create_key_hash = Some(key1.hash.toHexString),
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosCreate(
        event_offset = 3L,
        event_sequential_id = 3L,
        internal_contract_id = internalContractId2,
        create_key_hash = Some(key1.hash.toHexString),
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosConsumingExercise(
        event_offset = 4L,
        event_sequential_id = 4L,
        internal_contract_id = Some(internalContractId2),
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosCreate(
        event_offset = 5L,
        event_sequential_id = 5L,
        internal_contract_id = internalContractId3,
        create_key_hash = Some(key1.hash.toHexString),
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(5), 5L)
    )
    val keyStates2 = executeSql(
      backend.contract.keyStates(
        List(
          key1,
          key2,
        ),
        2L,
      )
    )
    val keyStateKey1_2 = executeSql(
      backend.contract.keyState(key1, 2L)
    )
    val keyStateKey2_2 = executeSql(
      backend.contract.keyState(key2, 2L)
    )
    val keyStates3 = executeSql(
      backend.contract.keyStates(
        List(
          key1,
          key2,
        ),
        3L,
      )
    )
    val keyStateKey1_3 = executeSql(
      backend.contract.keyState(key1, 3L)
    )
    val keyStateKey2_3 = executeSql(
      backend.contract.keyState(key2, 3L)
    )
    val keyStates4 = executeSql(
      backend.contract.keyStates(
        List(
          key1,
          key2,
        ),
        4L,
      )
    )
    val keyStateKey1_4 = executeSql(
      backend.contract.keyState(key1, 4L)
    )
    val keyStateKey2_4 = executeSql(
      backend.contract.keyState(key2, 4L)
    )
    val keyStates5 = executeSql(
      backend.contract.keyStates(
        List(
          key1,
          key2,
        ),
        5L,
      )
    )
    val keyStateKey1_5 = executeSql(
      backend.contract.keyState(key1, 5L)
    )
    val keyStateKey2_5 = executeSql(
      backend.contract.keyState(key2, 5L)
    )

    keyStates2 shouldBe Map(
      key1 -> internalContractId,
      key2 -> internalContractId4,
    )
    keyStateKey1_2 shouldBe Some(internalContractId)
    keyStateKey2_2 shouldBe Some(internalContractId4)
    keyStates3 shouldBe Map(
      key1 -> internalContractId2,
      key2 -> internalContractId4,
    )
    keyStateKey1_3 shouldBe Some(internalContractId2)
    keyStateKey2_3 shouldBe Some(internalContractId4)
    keyStates4 shouldBe Map(
      key2 -> internalContractId4
    )
    keyStateKey1_4 shouldBe None
    keyStateKey2_4 shouldBe Some(internalContractId4)
    keyStates5 shouldBe Map(
      key1 -> internalContractId3,
      key2 -> internalContractId4,
    )
    keyStateKey1_5 shouldBe Some(internalContractId3)
    keyStateKey2_5 shouldBe Some(internalContractId4)
  }

  it should "correctly find active contracts" in {
    val internalContractId = 123L
    val internalContractId2 = 223L
    val internalContractId3 = 323L
    val signatory = Ref.Party.assertFromString("signatory")

    val dtos: Vector[DbDto] = Vector(
      dtosCreate(
        event_offset = 1L,
        event_sequential_id = 1L,
        internal_contract_id = internalContractId,
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosAssign(
        event_offset = 2L,
        event_sequential_id = 2L,
        internal_contract_id = internalContractId2,
        synchronizer_id = someSynchronizerId2,
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosAssign(
        event_offset = 3L,
        event_sequential_id = 3L,
        internal_contract_id = internalContractId3,
        synchronizer_id = someSynchronizerId2,
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosAssign(
        event_offset = 4L,
        event_sequential_id = 4L,
        internal_contract_id = internalContractId3,
        synchronizer_id = someSynchronizerId,
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(3), 3L)
    )
    val activeContracts2 = executeSql(
      backend.contract.activeContracts(
        List(
          internalContractId,
          internalContractId2,
          internalContractId3,
        ),
        2L,
      )
    )
    val activeContracts3 = executeSql(
      backend.contract.activeContracts(
        List(
          internalContractId,
          internalContractId2,
          internalContractId3,
        ),
        3L,
      )
    )
    val activeIds = executeSql(
      backend.event.updateStreamingQueries.fetchActiveIds(
        stakeholderO = Some(signatory),
        templateIdO = None,
        activeAtEventSeqId = 1000,
      )(_)(
        PaginatingAsyncStream.IdFilterInput(
          startExclusive = 0L,
          endInclusive = 1000L,
        )
      )
    )
    val lastActivations = executeSql(
      backend.contract.lastActivations(
        List(
          someSynchronizerId -> internalContractId,
          someSynchronizerId -> internalContractId3,
          someSynchronizerId2 -> internalContractId2,
          someSynchronizerId2 -> internalContractId3,
        )
      )
    )

    activeContracts2 shouldBe Map(
      internalContractId -> true,
      internalContractId2 -> true,
    )
    activeContracts3 shouldBe Map(
      internalContractId -> true,
      internalContractId2 -> true,
      internalContractId3 -> true,
    )
    activeIds shouldBe Vector(1L, 2L, 3L, 4L)
    lastActivations shouldBe Map(
      (someSynchronizerId, internalContractId) -> 1L,
      (someSynchronizerId2, internalContractId2) -> 2L,
      (someSynchronizerId2, internalContractId3) -> 3L,
    )
  }

  it should "correctly find deactivated contracts" in {
    val internalContractId = 123L
    val internalContractId2 = 223L
    val internalContractId3 = 323L
    val signatory = Ref.Party.assertFromString("signatory")

    val dtos: Vector[DbDto] = Vector(
      dtosCreate(
        event_offset = 1L,
        event_sequential_id = 1L,
        internal_contract_id = internalContractId,
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosAssign(
        event_offset = 2L,
        event_sequential_id = 2L,
        internal_contract_id = internalContractId2,
        synchronizer_id = someSynchronizerId2,
      )(
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosUnassign(
        event_offset = 3L,
        event_sequential_id = 3L,
        internal_contract_id = Some(internalContractId),
        deactivated_event_sequential_id = Some(1L),
        synchronizer_id = someSynchronizerId,
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
      dtosConsumingExercise(
        event_offset = 4L,
        event_sequential_id = 4L,
        internal_contract_id = Some(internalContractId2),
        deactivated_event_sequential_id = Some(2L),
        synchronizer_id = someSynchronizerId2,
        stakeholders = Set(signatory),
        template_id = someTemplateId,
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(
      updateLedgerEnd(offset(4), 4L)
    )
    val activeContracts2 = executeSql(
      backend.contract.activeContracts(
        List(
          internalContractId,
          internalContractId2,
        ),
        2L,
      )
    )
    val activeContracts4 = executeSql(
      backend.contract.activeContracts(
        List(
          internalContractId,
          internalContractId2,
          internalContractId3,
        ),
        4L,
      )
    )
    val activeIds2 = executeSql(
      backend.event.updateStreamingQueries.fetchActiveIds(
        stakeholderO = Some(signatory),
        templateIdO = None,
        activeAtEventSeqId = 2L,
      )(_)(
        PaginatingAsyncStream.IdFilterInput(
          startExclusive = 0L,
          endInclusive = 2L,
        )
      )
    )
    val activeIds4 = executeSql(
      backend.event.updateStreamingQueries.fetchActiveIds(
        stakeholderO = Some(signatory),
        templateIdO = None,
        activeAtEventSeqId = 4,
      )(_)(
        PaginatingAsyncStream.IdFilterInput(
          startExclusive = 0L,
          endInclusive = 4L,
        )
      )
    )
    val lastActivations = executeSql(
      backend.contract.lastActivations(
        List(
          someSynchronizerId -> internalContractId,
          someSynchronizerId2 -> internalContractId2,
        )
      )
    )

    activeContracts2 shouldBe Map(
      internalContractId -> true,
      internalContractId2 -> true,
    )
    activeContracts4 shouldBe Map(
      internalContractId -> true, // although deactivated, this logic only cares about archivals
      internalContractId2 -> false,
    )
    activeIds2 shouldBe Vector(1L, 2L)
    activeIds4 shouldBe Vector.empty
    // lastActivation does not care about deactivations
    lastActivations shouldBe Map(
      (someSynchronizerId, internalContractId) -> 1L,
      (someSynchronizerId2, internalContractId2) -> 2L,
    )
  }

  it should "be able to query with 1000 contract ids" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(
      updateLedgerEnd(offset(3), 6L)
    )
    val activeContracts = executeSql(
      backend.contract.activeContracts(
        1.to(1000).map(_.toLong),
        2,
      )
    )
    activeContracts shouldBe empty
  }
}
