// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.grpc.{GrpcException, GrpcStatus}
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{
  EventFormat,
  Filters,
  ParticipantAuthorizationTopologyFormat,
  TopologyFormat,
  TransactionFormat,
  UpdateFormat,
}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.CantonFixture
import io.grpc.Status
import org.scalatest.Assertion

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

trait ServiceCallAuthTests
    extends CantonFixture
    with SecurityTags
    with SandboxRequiringAuthorization {

  protected def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any]

  protected def expectSuccess(f: Future[Any])(implicit ec: ExecutionContext): Assertion =
    f.map((_: Any) => succeed).futureValue

  protected def expectPermissionDenied(f: Future[Any])(implicit
      ec: ExecutionContext
  ): Assertion =
    expectFailure(f, Status.Code.PERMISSION_DENIED)

  protected def expectUnauthenticated(f: Future[Any])(implicit
      ec: ExecutionContext
  ): Assertion =
    expectFailure(f, Status.Code.UNAUTHENTICATED)

  protected def expectInvalidArgument(f: Future[Any])(implicit
      ec: ExecutionContext
  ): Assertion =
    expectFailure(f, Status.Code.INVALID_ARGUMENT)

  protected def expectUnknownResource(f: Future[Any])(implicit
      ec: ExecutionContext
  ): Assertion =
    expectFailure(f, Status.Code.NOT_FOUND)

  protected def expectFailure(f: Future[Any], code: Status.Code)(implicit
      ec: ExecutionContext
  ): Assertion =
    f.failed.collect {
      case GrpcException(GrpcStatus(`code`, _), _) =>
        succeed
      case NonFatal(e) =>
        fail(e)
    }.futureValue

  protected def eventFormat(party: String): EventFormat =
    EventFormat(
      filtersByParty = Map(party -> Filters(Nil)),
      filtersForAnyParty = None,
      verbose = false,
    )

  protected def updateFormat(
      transactionsPartyO: Option[String],
      reassignmentsPartyO: Option[String] = None,
      topologyPartiesO: Option[Seq[String]] = None,
  ): Option[UpdateFormat] =
    Some(
      UpdateFormat(
        includeTransactions = transactionsPartyO.map(party =>
          TransactionFormat(Some(eventFormat(party)), TRANSACTION_SHAPE_ACS_DELTA)
        ),
        includeReassignments = reassignmentsPartyO.map(eventFormat),
        includeTopologyEvents = topologyPartiesO.map(parties =>
          TopologyFormat(Some(ParticipantAuthorizationTopologyFormat(parties = parties)))
        ),
      )
    )

  protected lazy val eventFormatPartyWildcard: Option[EventFormat] =
    Some(
      EventFormat(
        filtersByParty = Map.empty,
        filtersForAnyParty = Some(Filters(Nil)),
        verbose = false,
      )
    )

  protected def participantBegin: Long = 0L

}
