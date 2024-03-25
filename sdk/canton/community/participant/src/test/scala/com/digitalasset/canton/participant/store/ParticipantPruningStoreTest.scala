// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CantonRequireTypes.String36
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.store.ParticipantPruningStore.ParticipantPruningStatus
import org.scalatest.wordspec.AsyncWordSpec

import scala.language.implicitConversions

trait ParticipantPruningStoreTest extends AsyncWordSpec with BaseTest {

  protected def mk(): ParticipantPruningStore

  protected val name: String36 = String36.tryCreate("ParticipantPruningStoreTest")

  private implicit def toGlobalOffset(i: Long): GlobalOffset = GlobalOffset.tryFromLong(i)

  "be not pruning in the beginning" in {
    val store = mk()
    for {
      status <- store.pruningStatus()
    } yield status shouldBe ParticipantPruningStatus(None, None)
  }

  "store the correct states in a single pruning cycle" in {
    val store = mk()
    for {
      status0 <- store.pruningStatus()
      _ <- store.markPruningStarted(42L)
      status1 <- store.pruningStatus()
      _ <- store.markPruningDone(42L)
      status2 <- store.pruningStatus()
    } yield {
      status0 shouldBe ParticipantPruningStatus(None, None)
      status0 should not be Symbol("inProgress")

      status1 shouldBe ParticipantPruningStatus(Some(42L), None)
      status1 shouldBe Symbol("inProgress")

      status2 shouldBe ParticipantPruningStatus(Some(42L), Some(42L))
      status2 should not be Symbol("inProgress")
    }
  }

  "store the correct states in two concurrent pruning cycles" in {
    val store = mk()
    for {
      _ <- store.markPruningStarted(5L)
      _ <- store.markPruningStarted(10L)
      status0 <- store.pruningStatus()
      _ <- store.markPruningDone(10L)
      status1 <- store.pruningStatus()
      _ <- store.markPruningDone(5L)
      status2 <- store.pruningStatus()
    } yield {
      status0 shouldBe ParticipantPruningStatus(Some(10L), None)
      status1 shouldBe ParticipantPruningStatus(Some(10L), Some(10L))
      status2 shouldBe ParticipantPruningStatus(Some(10L), Some(10L))
    }
  }

  "ignore outdated status updates" in {
    val store = mk()
    for {
      _ <- store.markPruningStarted(10L)
      _ <- store.markPruningStarted(8L)
      _ <- store.markPruningDone(11L)
      _ <- store.markPruningDone(9L)
      status <- store.pruningStatus()
    } yield status shouldBe ParticipantPruningStatus(Some(10L), Some(11L))
  }

  "store the maximum values" in {
    val store = mk()
    val maxOffset = Long.MaxValue
    for {
      _ <- store.markPruningStarted(maxOffset)
      _ <- store.markPruningDone(maxOffset)
      status <- store.pruningStatus()
    } yield status shouldBe ParticipantPruningStatus(Some(maxOffset), Some(maxOffset))
  }
}
