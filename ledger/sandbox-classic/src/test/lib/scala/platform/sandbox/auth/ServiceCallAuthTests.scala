// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import java.time.Duration
import java.util.UUID

import com.daml.grpc.{GrpcException, GrpcStatus}
import com.daml.ledger.api.auth.client.LedgerCallCredentials
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.admin.{user_management_service => proto}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.platform.sandbox.SandboxRequiringAuthorization
import com.daml.platform.sandbox.services.SandboxFixture
import io.grpc.Status
import io.grpc.stub.AbstractStub
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future
import scala.util.control.NonFatal

trait ServiceCallAuthTests
    extends AsyncFlatSpec
    with SandboxFixture
    with SandboxRequiringAuthorization
    with SuiteResourceManagementAroundAll
    with Matchers {

  def serviceCallName: String

  protected def serviceCallWithToken(token: Option[String]): Future[Any]

  /** Override this method in requests that require an application-id. Tests that use a user token
    * will call this method to avoid application_id checks from failing.
    */
  protected def serviceCallWithoutApplicationId(token: Option[String]): Future[Any] =
    serviceCallWithToken(token)

  protected def stub[A <: AbstractStub[A]](stub: A, token: Option[String]): A =
    token.fold(stub)(LedgerCallCredentials.authenticatingStub(stub, _))

  protected def expectSuccess(f: Future[Any]): Future[Assertion] = f.map((_: Any) => succeed)

  protected def expectPermissionDenied(f: Future[Any]): Future[Assertion] =
    expectFailure(f, Status.Code.PERMISSION_DENIED)

  protected def expectUnauthenticated(f: Future[Any]): Future[Assertion] =
    expectFailure(f, Status.Code.UNAUTHENTICATED)

  protected def expectInvalidArgument(f: Future[Any]): Future[Assertion] =
    expectFailure(f, Status.Code.INVALID_ARGUMENT)

  protected def expectUnimplemented(f: Future[Any]): Future[Assertion] =
    expectFailure(f, Status.Code.UNIMPLEMENTED)

  protected def expectFailure(f: Future[Any], code: Status.Code): Future[Assertion] =
    f.failed.collect {
      case GrpcException(GrpcStatus(`code`, _), _) => succeed
      case NonFatal(e) => fail(e)
    }

  protected def txFilterFor(party: String): Option[TransactionFilter] =
    Some(TransactionFilter(Map(party -> Filters())))

  protected def ledgerBegin: LedgerOffset =
    LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))

  protected val randomParty: String = UUID.randomUUID.toString
  protected val canActAsRandomParty: Option[String] =
    Option(customTokenToHeader(readWriteToken(randomParty)))
  protected val canActAsRandomPartyExpired: Option[String] =
    Option(
      customTokenToHeader(expiringIn(Duration.ofDays(-1), readWriteToken(UUID.randomUUID.toString)))
    )
  protected val canActAsRandomPartyExpiresTomorrow: Option[String] =
    Option(
      customTokenToHeader(expiringIn(Duration.ofDays(1), readWriteToken(UUID.randomUUID.toString)))
    )

  protected val canReadAsRandomParty: Option[String] =
    Option(customTokenToHeader(readOnlyToken(randomParty)))
  protected val canReadAsRandomPartyExpired: Option[String] =
    Option(
      customTokenToHeader(expiringIn(Duration.ofDays(-1), readOnlyToken(UUID.randomUUID.toString)))
    )
  protected val canReadAsRandomPartyExpiresTomorrow: Option[String] =
    Option(
      customTokenToHeader(expiringIn(Duration.ofDays(1), readOnlyToken(UUID.randomUUID.toString)))
    )

  protected val canReadAsAdmin: Option[String] =
    Option(customTokenToHeader(adminToken))

  protected val canReadAsAdminExpired: Option[String] =
    Option(customTokenToHeader(expiringIn(Duration.ofDays(-1), adminToken)))
  protected val canReadAsAdminExpiresTomorrow: Option[String] =
    Option(customTokenToHeader(expiringIn(Duration.ofDays(1), adminToken)))

  // Standard tokens for user authentication
  protected val canReadAsAdminStandardJWT: Option[String] =
    Option(toHeader(adminTokenStandardJWT))
  protected val canReadAsUnknownUserStandardJWT: Option[String] =
    Option(toHeader(unknownUserTokenStandardJWT))
  protected val canReadAsInvalidUserStandardJWT: Option[String] =
    Option(toHeader(invalidUserTokenStandardJWT))

  // Special tokens to test decoding users and rights from custom tokens
  protected val randomUserCanReadAsRandomParty: Option[String] =
    Option(customTokenToHeader(readOnlyToken(randomParty).copy(applicationId = Some(randomUserId))))
  protected val randomUserCanActAsRandomParty: Option[String] =
    Option(
      customTokenToHeader(readWriteToken(randomParty).copy(applicationId = Some(randomUserId)))
    )

  // Note: lazy val, because the ledger ID is only known after the sandbox start
  protected lazy val canReadAsRandomPartyActualLedgerId: Option[String] =
    Option(
      customTokenToHeader(forLedgerId(unwrappedLedgerId, readOnlyToken(UUID.randomUUID.toString)))
    )
  protected val canReadAsRandomPartyRandomLedgerId: Option[String] =
    Option(
      customTokenToHeader(
        forLedgerId(UUID.randomUUID.toString, readOnlyToken(UUID.randomUUID.toString))
      )
    )
  protected val canReadAsRandomPartyActualParticipantId: Option[String] =
    Option(
      customTokenToHeader(
        forParticipantId("sandbox-participant", readOnlyToken(UUID.randomUUID.toString))
      )
    )
  protected val canReadAsRandomPartyRandomParticipantId: Option[String] =
    Option(
      customTokenToHeader(
        forParticipantId(UUID.randomUUID.toString, readOnlyToken(UUID.randomUUID.toString))
      )
    )

  // Note: lazy val, because the ledger ID is only known after the sandbox start
  protected lazy val canReadAsAdminActualLedgerId: Option[String] =
    Option(customTokenToHeader(forLedgerId(unwrappedLedgerId, adminToken)))
  protected val canReadAsAdminRandomLedgerId: Option[String] =
    Option(customTokenToHeader(forLedgerId(UUID.randomUUID.toString, adminToken)))
  protected val canReadAsAdminActualParticipantId: Option[String] =
    Option(customTokenToHeader(forParticipantId("sandbox-participant", adminToken)))
  protected val canReadAsAdminRandomParticipantId: Option[String] =
    Option(customTokenToHeader(forParticipantId(UUID.randomUUID.toString, adminToken)))

  protected def createUserByAdmin(
      userId: String,
      rights: Vector[proto.Right] = Vector.empty,
  ): Future[(proto.User, Option[String])] = {
    val userToken = Option(toHeader(standardToken(userId)))
    val req = proto.CreateUserRequest(Some(proto.User(userId)), rights)
    stub(proto.UserManagementServiceGrpc.stub(channel), canReadAsAdminStandardJWT)
      .createUser(req)
      .map((_, userToken))
  }
}
