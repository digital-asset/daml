// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.digitalasset.daml.lf.data.Ref
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.Semaphore
import scala.concurrent.Future

class PendingPartyAllocationsSpec
    extends AsyncFlatSpec
    with Matchers
    with BaseTest
    with HasExecutionContext {

  private val className = classOf[PendingPartyAllocations].getSimpleName

  private val ken = Some(Ref.UserId.assertFromString("ken"))
  behavior of s"$className.withUser"

  it should "not keep a tally when user not provided" in {
    val ppa = new PendingPartyAllocations
    for {
      outstanding <- ppa.withUser(None)(Future.successful)
    } yield {
      outstanding shouldBe 0
    }
  }

  it should "give 1 as the number of operations running when sequential" in {
    val ppa = new PendingPartyAllocations
    for {
      first <- ppa.withUser(ken)(Future.successful)
      second <- ppa.withUser(ken)(Future.successful)
    } yield {
      first shouldBe 1
      second shouldBe 1
    }
  }

  it should "keep tally when one of the operations throws" in {
    val ppa = new PendingPartyAllocations
    for {
      first <- ppa.withUser(ken)(Future.successful)
      _ <- ppa
        .withUser(ken)(_ => Future.failed(new RuntimeException("deliberate throw")))
        .recover(_ => 1)
      second <- ppa.withUser(ken)(Future.successful)
    } yield {
      first shouldBe 1
      second shouldBe 1
    }
  }

  it should "keep tally when concurrent operations" in {
    val semaphore = new Semaphore(0)
    val elements = 3
    def waitAndReturn(count: Int) =
      Future {
        if (count < elements)
          semaphore.acquire()
        else
          semaphore.release(3)
        count
      }
    val ppa = new PendingPartyAllocations
    val expected = (1 to elements).toList
    val futures = expected.map(_ => ppa.withUser(ken)(waitAndReturn))
    for {
      result <- Future.sequence(futures)
    } yield {
      result shouldBe expected
    }
  }

}
