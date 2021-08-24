// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.ledger.offset.Offset
import org.scalatest.Inside
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsCompletions
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AsyncFlatSpec =>

  behavior of "StorageBackend (completions)"

  import StorageBackendTestValues._

  it should "correctly find completions by offset range" in {
    val party = someParty
    val applicationId = someApplicationId

    val dtos = Vector(
      dtoConfiguration(offset(1)),
      dtoCompletion(offset(2), submitter = party),
      dtoCompletion(offset(3), submitter = party),
      dtoCompletion(offset(4), submitter = party),
    )

    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(dtos, _))
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(offset(4), 3L)))
      completions0to3 <- executeSql(
        backend.commandCompletions(Offset.beforeBegin, offset(3), applicationId, Set(party))
      )
      completions1to3 <- executeSql(
        backend.commandCompletions(offset(1), offset(3), applicationId, Set(party))
      )
      completions2to3 <- executeSql(
        backend.commandCompletions(offset(2), offset(3), applicationId, Set(party))
      )
      completions1to9 <- executeSql(
        backend.commandCompletions(offset(1), offset(9), applicationId, Set(party))
      )
    } yield {
      completions0to3 should have length 2
      completions1to3 should have length 2
      completions2to3 should have length 1
      completions1to9 should have length 3
    }
  }

}
