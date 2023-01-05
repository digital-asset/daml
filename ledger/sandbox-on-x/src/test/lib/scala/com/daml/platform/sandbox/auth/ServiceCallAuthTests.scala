// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.grpc.{GrpcException, GrpcStatus}
import com.daml.ledger.api.auth.client.LedgerCallCredentials
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.admin.{user_management_service => proto}
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.platform.sandbox.SandboxRequiringAuthorization
import com.daml.platform.sandbox.fixture.{CreatesParties, SandboxFixture}
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Authorization
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest}
import io.grpc.Status
import io.grpc.stub.AbstractStub
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration
import java.util.UUID
import scala.concurrent.Future
import scala.util.control.NonFatal

trait ServiceCallAuthTests
    extends AsyncFlatSpec
    with SandboxFixture
    with CreatesParties
    with SandboxRequiringAuthorization
    with SuiteResourceManagementAroundAll
    with Matchers {

  val securityAsset: SecurityTest =
    SecurityTest(property = Authorization, asset = s"User Endpoint $serviceCallName")

  val adminSecurityAsset: SecurityTest =
    SecurityTest(property = Authorization, asset = s"Admin Endpoint $serviceCallName")

  def attackPermissionDenied(threat: String): Attack = Attack(
    actor = s"Ledger API client calling $serviceCallName",
    threat = threat,
    mitigation = s"Refuse call by the client with PERMISSION_DENIED to $serviceCallName",
  )

  def attackInvalidArgument(threat: String): Attack = Attack(
    actor = s"Ledger API client calling $serviceCallName",
    threat = threat,
    mitigation = s"Refuse call by the client with INVALID_ARGUMENT to $serviceCallName",
  )

  def attackUnauthenticated(threat: String): Attack = Attack(
    actor = s"Ledger API client calling $serviceCallName",
    threat = threat,
    mitigation = s"Refuse call by the client with UNAUTHENTICATED to $serviceCallName",
  )

  def streamAttack(threat: String): Attack = Attack(
    actor = s"Ledger API stream client calling $serviceCallName",
    threat = threat,
    mitigation = s"Break the stream of $serviceCallName",
  )

  def serviceCallName: String

  protected def prerequisiteParties: List[String] = List.empty

  override def beforeAll(): Unit = {
    super.beforeAll()
    createPrerequisiteParties(canReadAsAdminStandardJWT.token, prerequisiteParties)
  }

  protected def serviceCall(context: ServiceCallContext): Future[Any]

  protected def stub[A <: AbstractStub[A]](stub: A, token: Option[String]): A =
    token.fold(stub)(LedgerCallCredentials.authenticatingStub(stub, _))

  protected def expectSuccess(f: Future[Any]): Future[Assertion] = f.map((_: Any) => succeed)

  protected def expectPermissionDenied(f: Future[Any]): Future[Assertion] =
    expectFailure(f, Status.Code.PERMISSION_DENIED)

  protected def expectUnauthenticated(f: Future[Any]): Future[Assertion] =
    expectFailure(f, Status.Code.UNAUTHENTICATED)

  protected def expectInvalidArgument(f: Future[Any]): Future[Assertion] =
    expectFailure(f, Status.Code.INVALID_ARGUMENT)

  protected def expectFailure(f: Future[Any], code: Status.Code): Future[Assertion] =
    f.failed.collect {
      case GrpcException(GrpcStatus(`code`, _), _) =>
        succeed
      case NonFatal(e) =>
        fail(e)
    }

  protected def txFilterFor(party: String): Option[TransactionFilter] =
    Some(TransactionFilter(Map(party -> Filters())))

  protected def ledgerBegin: LedgerOffset =
    LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))

  protected val randomParty: String = UUID.randomUUID.toString
  protected val noToken: ServiceCallContext = ServiceCallContext(None)
  protected val canActAsRandomParty: ServiceCallContext = ServiceCallContext(
    Option(toHeader(readWriteToken(randomParty)))
  )
  protected val canActAsRandomPartyExpired: ServiceCallContext = ServiceCallContext(
    Option(
      toHeader(expiringIn(Duration.ofDays(-1), readWriteToken(UUID.randomUUID.toString)))
    )
  )
  protected val canActAsRandomPartyExpiresTomorrow: ServiceCallContext = ServiceCallContext(
    Option(
      toHeader(expiringIn(Duration.ofDays(1), readWriteToken(UUID.randomUUID.toString)))
    )
  )

  protected val canReadAsRandomParty: ServiceCallContext = ServiceCallContext(
    Option(toHeader(readOnlyToken(randomParty)))
  )
  protected val canReadAsRandomPartyExpired: ServiceCallContext = ServiceCallContext(
    Option(
      toHeader(expiringIn(Duration.ofDays(-1), readOnlyToken(UUID.randomUUID.toString)))
    )
  )
  protected val canReadAsRandomPartyExpiresTomorrow: ServiceCallContext = ServiceCallContext(
    Option(
      toHeader(expiringIn(Duration.ofDays(1), readOnlyToken(UUID.randomUUID.toString)))
    )
  )

  protected val canReadAsAdmin: ServiceCallContext = ServiceCallContext(
    Option(toHeader(adminToken))
  )

  protected val canReadAsAdminExpired: ServiceCallContext = ServiceCallContext(
    Option(toHeader(expiringIn(Duration.ofDays(-1), adminToken)))
  )
  protected val canReadAsAdminExpiresTomorrow: ServiceCallContext = ServiceCallContext(
    Option(toHeader(expiringIn(Duration.ofDays(1), adminToken)))
  )

  // Standard tokens for user authentication
  protected val canReadAsAdminStandardJWT: ServiceCallContext = ServiceCallContext(
    Option(toHeader(adminTokenStandardJWT))
  )
  protected val canReadAsUnknownUserStandardJWT: ServiceCallContext = ServiceCallContext(
    Option(toHeader(unknownUserTokenStandardJWT))
  )
  protected val canReadAsInvalidUserStandardJWT: ServiceCallContext = ServiceCallContext(
    Option(toHeader(invalidUserTokenStandardJWT))
  )

  // Special tokens to test decoding users and rights from custom tokens
  protected val randomUserCanReadAsRandomParty: ServiceCallContext = ServiceCallContext(
    Option(toHeader(readOnlyToken(randomParty).copy(applicationId = Some(randomUserId()))))
  )
  protected val randomUserCanActAsRandomParty: ServiceCallContext = ServiceCallContext(
    Option(
      toHeader(readWriteToken(randomParty).copy(applicationId = Some(randomUserId())))
    )
  )

  // Note: lazy val, because the ledger ID is only known after the sandbox start
  protected lazy val canReadAsRandomPartyActualLedgerId: ServiceCallContext = ServiceCallContext(
    Option(
      toHeader(forLedgerId(unwrappedLedgerId, readOnlyToken(UUID.randomUUID.toString)))
    )
  )
  protected val canReadAsRandomPartyRandomLedgerId: ServiceCallContext = ServiceCallContext(
    Option(
      toHeader(
        forLedgerId(UUID.randomUUID.toString, readOnlyToken(UUID.randomUUID.toString))
      )
    )
  )
  protected val canReadAsRandomPartyActualParticipantId: ServiceCallContext = ServiceCallContext(
    Option(
      toHeader(
        forParticipantId("sandbox-participant", readOnlyToken(UUID.randomUUID.toString))
      )
    )
  )
  protected val canReadAsRandomPartyRandomParticipantId: ServiceCallContext = ServiceCallContext(
    Option(
      toHeader(
        forParticipantId(UUID.randomUUID.toString, readOnlyToken(UUID.randomUUID.toString))
      )
    )
  )

  // Note: lazy val, because the ledger ID is only known after the sandbox start
  protected lazy val canReadAsAdminActualLedgerId: ServiceCallContext = ServiceCallContext(
    Option(toHeader(forLedgerId(unwrappedLedgerId, adminToken)))
  )
  protected val canReadAsAdminRandomLedgerId: ServiceCallContext = ServiceCallContext(
    Option(toHeader(forLedgerId(UUID.randomUUID.toString, adminToken)))
  )
  protected val canReadAsAdminActualParticipantId: ServiceCallContext = ServiceCallContext(
    Option(toHeader(forParticipantId("sandbox-participant", adminToken)))
  )
  protected val canReadAsAdminRandomParticipantId: ServiceCallContext = ServiceCallContext(
    Option(toHeader(forParticipantId(UUID.randomUUID.toString, adminToken)))
  )

  protected def createUserByAdmin(
      userId: String,
      identityProviderId: String = "",
      rights: Vector[proto.Right] = Vector.empty,
      tokenIssuer: Option[String] = None,
  ): Future[(proto.User, ServiceCallContext)] = {
    val userToken = Option(toHeader(standardToken(userId, issuer = tokenIssuer)))
    val user = proto.User(
      id = userId,
      metadata = Some(ObjectMeta()),
      identityProviderId = identityProviderId,
    )
    val req = proto.CreateUserRequest(Some(user), rights)
    stub(proto.UserManagementServiceGrpc.stub(channel), canReadAsAdminStandardJWT.token)
      .createUser(req)
      .map(res => (res.user.get, ServiceCallContext(userToken, true, identityProviderId)))
  }

  protected def updateUser(
      accessToken: String,
      req: proto.UpdateUserRequest,
  ): Future[proto.UpdateUserResponse] = {
    stub(proto.UserManagementServiceGrpc.stub(channel), Some(accessToken))
      .updateUser(req)
  }
}
