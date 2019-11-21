// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import java.time.{Duration, Instant}
import java.util.UUID

import com.digitalasset.grpc.{GrpcException, GrpcStatus}
import com.digitalasset.jwt.domain.DecodedJwt
import com.digitalasset.jwt.{HMAC256Verifier, JwtSigner}
import com.digitalasset.ledger.api.auth.{AuthServiceJWT, AuthServiceJWTCodec, AuthServiceJWTPayload}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.client.auth.LedgerClientCallCredentials.authenticatingStub
import com.digitalasset.platform.sandbox.config.SandboxConfig
import io.grpc.Status
import io.grpc.stub.{AbstractStub, StreamObserver}
import org.scalatest.Suite
import scalaz.syntax.tag.ToTagOps

import scala.concurrent.{Future, Promise}

trait SandboxFixtureWithAuth extends SandboxFixture { self: Suite =>

  protected def stub[A <: AbstractStub[A]](stub: A, token: Option[String]): A =
    token.fold(stub)(authenticatingStub(stub))

  override protected def config: SandboxConfig =
    super.config.copy(
      authService = Some(
        AuthServiceJWT(HMAC256Verifier(jwtSecret)
          .getOrElse(sys.error("Failed to create HMAC256 verifier")))))

  protected def rwToken(party: String) = AuthServiceJWTPayload(
    ledgerId = None,
    participantId = None,
    applicationId = None,
    exp = None,
    admin = false,
    actAs = List(party),
    readAs = List(party)
  )

  protected def adminToken = AuthServiceJWTPayload(
    ledgerId = None,
    participantId = None,
    applicationId = None,
    exp = None,
    admin = true,
    actAs = List(),
    readAs = List()
  )

  protected lazy val wrappedLedgerId = ledgerId(Some(adminToken.asHeader()))
  protected lazy val unwrappedLedgerId = wrappedLedgerId.unwrap

  private val jwtHeader = """{"alg": "HS256", "typ": "JWT"}"""
  private val jwtSecret = UUID.randomUUID.toString

  implicit protected class AuthServiceJWTPayloadExtensions(payload: AuthServiceJWTPayload) {
    def expiresIn(t: java.time.Duration): AuthServiceJWTPayload =
      payload.copy(exp = Some(Instant.now.plus(t)))
    def expiresInFiveSeconds: AuthServiceJWTPayload = expiresIn(Duration.ofSeconds(5))
    def expiresTomorrow: AuthServiceJWTPayload = expiresIn(Duration.ofDays(1))
    def expired: AuthServiceJWTPayload = expiresIn(Duration.ofDays(-1))

    def signed(secret: String): String =
      JwtSigner.HMAC256
        .sign(DecodedJwt(jwtHeader, AuthServiceJWTCodec.compactPrint(payload)), secret)
        .getOrElse(sys.error("Failed to generate token"))
        .value

    def asHeader(secret: String = jwtSecret) = s"Bearer ${signed(secret)}"
  }

  /** Returns a future that fails iff the given stream immediately fails. */
  protected def streamResult[T](fn: StreamObserver[T] => Unit): Future[Unit] = {
    val promise = Promise[Unit]()
    fn(new StreamObserver[T] {
      def onNext(value: T): Unit = {
        val _ = promise.trySuccess(())
      }
      def onError(t: Throwable): Unit = {
        val _ = promise.tryFailure(t)
      }
      def onCompleted(): Unit = {
        val _ = promise.trySuccess(())
      }
    })
    promise.future
  }

  protected def expectExpiration[T](fn: StreamObserver[T] => Unit): Future[Throwable] = {
    val promise = Promise[Throwable]()
    fn(new StreamObserver[T] {
      @volatile private[this] var gotSomething = false
      def onNext(value: T): Unit = {
        gotSomething = true
      }
      def onError(t: Throwable): Unit = {
        t match {
          case GrpcException(GrpcStatus(Status.Code.PERMISSION_DENIED, _), _) if gotSomething =>
            val _ = promise.trySuccess(t)
          case _ =>
            val _ = promise.tryFailure(t)
        }
      }
      def onCompleted(): Unit = {
        val _ = promise.tryFailure(new RuntimeException("stream completed before token expiration"))
      }
    })
    promise.future
  }

  protected def txFilterFor(party: String) = Some(TransactionFilter(Map(party -> Filters())))

  protected def ledgerBegin: LedgerOffset =
    LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))

}
