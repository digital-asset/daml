// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_17

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  Participant,
  Participants,
  SingleParty,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.Assertions.{assertGrpcError, futureAssertions}
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.suites.v1_8.CompanionImplicits
import com.daml.ledger.api.v1.commands
import com.daml.ledger.javaapi.data.PrefetchContractKey
import com.daml.ledger.test.java.model.da.types.Tuple2
import com.daml.ledger.test.java.model.test.{TextKey, TextKeyOperations, WithKey}

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

object PrefetchContractKeysIT {
  implicit final class JavaBindingSupportExtension(val prefetch: PrefetchContractKey)
      extends AnyVal {
    def toProtoInner: commands.PrefetchContractKey = {
      commands.PrefetchContractKey.fromJavaProto(prefetch.toProto)
    }
  }
}

class PrefetchContractKeysIT extends LedgerTestSuite {
  import CompanionImplicits._
  import PrefetchContractKeysIT._

  test(
    "CSprefetchContractKeysBasic",
    "Explicit contract key prefetches are accepted",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val prefetch = WithKey.byKey(party).toPrefetchKey().toProtoInner
    val request = ledger
      .submitAndWaitRequest(party, new WithKey(party).create.commands)
      .update(_.commands.prefetchContractKeys := Seq(prefetch))
    for {
      _ <- ledger.submitAndWait(request)
      active <- ledger.activeContracts(party)
    } yield {
      assert(active.size == 1)
      val dummyTemplateId = active.flatMap(_.templateId.toList).head
      assert(dummyTemplateId == WithKey.TEMPLATE_ID_WITH_PACKAGE_ID.toV1)
    }
  })

  test(
    "CSprefetchContractKeysWronglyTyped",
    "Contract key prefetches with wrongly typed keys are rejected",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val prefetch = WithKey
      .byKey(party)
      .toPrefetchKey()
      .toProtoInner
      .update(_.templateId := TextKey.TEMPLATE_ID.toV1)
    val request = ledger
      .submitAndWaitRequest(party, new WithKey(party).create.commands)
      .update(_.commands.prefetchContractKeys := Seq(prefetch))
    for {
      failure <- ledger.submitAndWait(request).mustFail("wrongly typed key in prefetch list")
    } yield assertGrpcError(
      failure,
      LedgerApiErrors.CommandExecution.Preprocessing.PreprocessingFailed,
      Some("mismatching type"),
    )
  })

  test(
    "CSprefetchContractKeysMany",
    "Prefetch many contract keys",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val numPrefetches = 1000
    val prefetches =
      (1 to numPrefetches).map(i =>
        TextKey.byKey(new Tuple2(party, s"key$i")).toPrefetchKey().toProtoInner
      )
    val existingKeyIndex = 10
    for {
      textKeyContract <- ledger.create(party, new TextKey(party, "key10", Seq.empty.asJava))
      textKeyOps <- ledger.create(party, new TextKeyOperations(party))
      exerciseCommands = (1 to numPrefetches)
        .flatMap(i =>
          textKeyOps
            .exerciseTKOLookup(
              new Tuple2(party, s"key$i"),
              Option.when(i == existingKeyIndex)(textKeyContract).toJava,
            )
            .commands
            .asScala
        )
        .asJava
      request = ledger
        .submitAndWaitRequest(party, exerciseCommands)
        .update(_.commands.prefetchContractKeys := prefetches)
      _ <- ledger.submitAndWait(request)
    } yield ()
  })
}
