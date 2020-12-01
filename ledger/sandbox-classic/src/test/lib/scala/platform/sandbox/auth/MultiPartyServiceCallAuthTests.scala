// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import java.util.UUID

import scala.concurrent.Future

/**
  * Trait for services that use multiple actAs and readAs parties.
  * These tests only test for variations in the authorized parties.
  * They do not test for variations in expiration time, ledger ID, or participant ID.
  * It is expected that [[ReadWriteServiceCallAuthTests]] are run on the same service.
  */
trait MultiPartyServiceCallAuthTests extends ServiceCallAuthTests {

  private val actorsCount: Int = 3
  private val readersCount: Int = 3

  protected val actAs: List[String] = List.fill(actorsCount)(UUID.randomUUID.toString)
  protected val readAs: List[String] = List.fill(readersCount)(UUID.randomUUID.toString)

  private val randomActAs: List[String] = List.fill(actorsCount)(UUID.randomUUID.toString)
  private val randomReadAs: List[String] = List.fill(readersCount)(UUID.randomUUID.toString)

  private def serviceCallFor(actAs: List[String], readAs: List[String]): Future[Any] = {
    val token = Option(toHeader(multiPartyToken(actAs, readAs)))
    serviceCallWithToken(token)
  }

  it should "allow calls authorized to exactly the required parties" in {
    expectSuccess(serviceCallFor(actAs, readAs))
  }
  it should "allow calls authorized to a superset of the required parties" in {
    expectSuccess(serviceCallFor(randomActAs ++ actAs, randomReadAs ++ readAs))
  }
  it should "allow calls with all parties authorized in read-write mode" in {
    expectPermissionDenied(serviceCallFor(actAs ++ readAs, List.empty))
  }

  it should "deny calls authorized to no parties" in {
    expectPermissionDenied(serviceCallFor(List.empty, List.empty))
  }
  it should "deny calls authorized to random parties" in {
    expectPermissionDenied(serviceCallFor(randomActAs, randomReadAs))
  }
  it should "deny calls with all parties authorized in read-only mode" in {
    expectPermissionDenied(serviceCallFor(List.empty, actAs ++ readAs))
  }
  it should "deny calls with one missing actor authorization" in {
    expectPermissionDenied(serviceCallFor(actAs.take(actorsCount - 1), readAs))
  }
  it should "deny calls with one missing reader authorization" in {
    expectPermissionDenied(serviceCallFor(actAs, readAs.take(readersCount - 1)))
  }
  it should "deny calls authorized to swapped actAs/readAs parties" in {
    expectPermissionDenied(serviceCallFor(readAs, actAs))
  }
}
