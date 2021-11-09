// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import org.scalatest.Inside
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsStringInterning
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AsyncFlatSpec =>

  behavior of "StorageBackend (StringInterning)"

  it should "store and load string-interning entries" in {
    val dtos = Vector(
      DbDto.StringInterningDto(2, "a"),
      DbDto.StringInterningDto(3, "b"),
      DbDto.StringInterningDto(4, "c"),
      DbDto.StringInterningDto(5, "d"),
    )

    for {
      interningIdsBeforeBegin <- executeSql(
        backend.stringInterning.loadStringInterningEntries(0, 5)
      )
      _ <- executeSql(ingest(dtos, _))
      interningIdsFull <- executeSql(backend.stringInterning.loadStringInterningEntries(0, 5))
      interningIdsOverFetch <- executeSql(
        backend.stringInterning.loadStringInterningEntries(0, 10)
      )
      interningIdsEmpty <- executeSql(
        backend.stringInterning.loadStringInterningEntries(5, 10)
      )
      interningIdsSubset <- executeSql(
        backend.stringInterning.loadStringInterningEntries(3, 10)
      )
    } yield {
      val expectedFullList = List(
        2 -> "a",
        3 -> "b",
        4 -> "c",
        5 -> "d",
      )
      interningIdsBeforeBegin shouldBe Nil
      interningIdsFull shouldBe expectedFullList
      interningIdsOverFetch shouldBe expectedFullList
      interningIdsEmpty shouldBe Nil
      interningIdsSubset shouldBe expectedFullList.drop(2)
    }
  }
}
