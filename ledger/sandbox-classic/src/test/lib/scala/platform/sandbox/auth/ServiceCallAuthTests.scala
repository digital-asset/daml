// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import java.time.Duration
import java.util.UUID

import com.daml.grpc.{GrpcException, GrpcStatus}
import com.daml.ledger.api.auth.client.LedgerCallCredentials
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.platform.sandbox.services.SandboxFixtureWithAuth
import io.grpc.Status
import io.grpc.stub.AbstractStub
import org.scalatest.{Assertion, AsyncFlatSpec, Matchers}

import scala.concurrent.Future
import scala.util.control.NonFatal

trait ServiceCallAuthTests
    extends AsyncFlatSpec
    with SandboxFixtureWithAuth
    with SuiteResourceManagementAroundAll
    with Matchers {

  def serviceCallName: String

  def serviceCallWithToken(token: Option[String]): Future[Any]

  protected def stub[A <: AbstractStub[A]](stub: A, token: Option[String]): A =
    token.fold(stub)(LedgerCallCredentials.authenticatingStub(stub, _))

  protected def expectSuccess(f: Future[Any]): Future[Assertion] = f.map((_: Any) => succeed)

  protected def expectPermissionDenied(f: Future[Any]): Future[Assertion] =
    expectFailure(f, Status.Code.PERMISSION_DENIED)

  protected def expectUnauthenticated(f: Future[Any]): Future[Assertion] =
    expectFailure(f, Status.Code.UNAUTHENTICATED)

  protected def expectFailure(f: Future[Any], code: Status.Code): Future[Assertion] =
    f.failed.collect {
      case GrpcException(GrpcStatus(`code`, _), _) => succeed
      case NonFatal(e) => fail(e)
    }

  protected def txFilterFor(party: String) = Some(TransactionFilter(Map(party -> Filters())))

  protected def ledgerBegin: LedgerOffset =
    LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))

  behavior of serviceCallName

  it should "deny unauthenticated calls" in {
    expectUnauthenticated(serviceCallWithToken(None))
  }

  protected val canActAsRandomParty =
    Option(toHeader(readWriteToken(UUID.randomUUID.toString)))
  protected val canActAsRandomPartyExpired =
    Option(toHeader(expiringIn(Duration.ofDays(-1), readWriteToken(UUID.randomUUID.toString))))
  protected val canActAsRandomPartyExpiresTomorrow =
    Option(toHeader(expiringIn(Duration.ofDays(1), readWriteToken(UUID.randomUUID.toString))))

  protected val canReadAsRandomParty =
    Option(toHeader(readOnlyToken(UUID.randomUUID.toString)))
  protected val canReadAsRandomPartyExpired =
    Option(toHeader(expiringIn(Duration.ofDays(-1), readOnlyToken(UUID.randomUUID.toString))))
  protected val canReadAsRandomPartyExpiresTomorrow =
    Option(toHeader(expiringIn(Duration.ofDays(1), readOnlyToken(UUID.randomUUID.toString))))

  protected val canReadAsAdmin =
    Option(toHeader(adminToken))
  protected val canReadAsAdminExpired =
    Option(toHeader(expiringIn(Duration.ofDays(-1), adminToken)))
  protected val canReadAsAdminExpiresTomorrow = Option(
    toHeader(expiringIn(Duration.ofDays(1), adminToken)))

  // Note: lazy val, because the ledger ID is only known after the sandbox start
  protected lazy val canReadAsRandomPartyActualLedgerId =
    Option(toHeader(forLedgerId(unwrappedLedgerId, readOnlyToken(UUID.randomUUID.toString))))
  protected val canReadAsRandomPartyRandomLedgerId =
    Option(toHeader(forLedgerId(UUID.randomUUID.toString, readOnlyToken(UUID.randomUUID.toString))))
  protected val canReadAsRandomPartyActualParticipantId =
    Option(
      toHeader(forParticipantId("sandbox-participant", readOnlyToken(UUID.randomUUID.toString))))
  protected val canReadAsRandomPartyRandomParticipantId =
    Option(
      toHeader(forParticipantId(UUID.randomUUID.toString, readOnlyToken(UUID.randomUUID.toString))))

  // Note: lazy val, because the ledger ID is only known after the sandbox start
  protected lazy val canReadAsAdminActualLedgerId =
    Option(toHeader(forLedgerId(unwrappedLedgerId, adminToken)))
  protected val canReadAsAdminRandomLedgerId =
    Option(toHeader(forLedgerId(UUID.randomUUID.toString, adminToken)))
  protected val canReadAsAdminActualParticipantId =
    Option(toHeader(forParticipantId("sandbox-participant", adminToken)))
  protected val canReadAsAdminRandomParticipantId =
    Option(toHeader(forParticipantId(UUID.randomUUID.toString, adminToken)))
}
