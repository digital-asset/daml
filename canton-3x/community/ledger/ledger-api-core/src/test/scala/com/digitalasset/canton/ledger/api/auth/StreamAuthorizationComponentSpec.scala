// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import com.daml.grpc.adapter.client.pekko.ClientAdapter
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.api.v1.transaction_service.TransactionServiceGrpc.{
  TransactionService,
  TransactionServiceStub,
}
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetLatestPrunedOffsetsRequest,
  GetLatestPrunedOffsetsResponse,
  GetLedgerEndRequest,
  GetLedgerEndResponse,
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsRequest,
  GetTransactionsResponse,
}
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.tracing.NoOpTelemetry
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.digitalasset.canton.ledger.api.auth.services.TransactionServiceAuthorization
import com.digitalasset.canton.ledger.api.domain.UserRight.CanReadAs
import com.digitalasset.canton.ledger.api.domain.{IdentityProviderId, User}
import com.digitalasset.canton.ledger.api.grpc.StreamingServiceLifecycleManagement
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.apiserver.{ApiServiceOwner, GrpcServer}
import com.digitalasset.canton.platform.localstore.InMemoryUserManagementStore
import com.digitalasset.canton.platform.localstore.api.UserManagementStore
import com.digitalasset.canton.{BaseTest, UniquePortGenerator}
import io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.StreamObserver
import io.grpc.{
  BindableService,
  Channel,
  Context,
  Contexts,
  Metadata,
  ServerCall,
  ServerCallHandler,
  ServerInterceptor,
}
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.{Done, NotUsed}
import org.scalatest.Assertion
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.Try

class StreamAuthorizationComponentSpec
    extends AsyncFlatSpec
    with BaseTest
    with Matchers
    with PekkoBeforeAndAfterAll {

  private implicit val ec: ExecutionContextExecutor = materializer.executionContext

  behavior of s"Stream authorization"

  it should "be successfull in the happy path, and client cancellation tears down server side gRPC and pekko-streams too" in test {
    fixture =>
      // this stream takes 10 elements (takes 2 seconds to produce), then it is closed (user side cancellation).
      // after one second a scheduled user right check will commence, this check expected to be successful
      fixture.clientStream
        .take(10)
        .map(_ => logger.debug("received"))
        .run()
        .map(_ => fixture.waitForServerPekkoStream shouldBe None)
  }

  it should "cancel streams if user rights changed" in test { fixture =>
    fixture.clientStream
      .take(10)
      .zipWithIndex
      .map { case (_, index) =>
        if (index == 1) {
          // after 2 received entries (400 millis) the user right change,
          // which triggers a STALE_STREAM_AUTHORIZATION
          fixture.changeUserRights
        }
        logger.debug(s"received #$index")
      }
      .run()
      .failed
      .map { t =>
        // the client stream should be cancelled with error
        t.getMessage should include("STALE_STREAM_AUTHORIZATION")
        // the server stream should be completed
        fixture.waitForServerPekkoStream shouldBe None
      }
  }

  it should "cancel streams if authorization expired" in test { fixture =>
    fixture.clientStream
      .take(10)
      .zipWithIndex
      .map { case (_, index) =>
        if (index == 1) {
          // after 2 received entries (400 millis) the user right change,
          // which triggers a STALE_STREAM_AUTHORIZATION
          fixture.expireUserClaims
        }
        logger.debug(s"received #$index")
      }
      .run()
      .failed
      .map { t =>
        // the client stream should be cancelled with error
        t.getMessage should include("PERMISSION_DENIED")
        // the server stream should be completed
        fixture.waitForServerPekkoStream shouldBe None
      }
  }

  case class Fixture(
      clientStream: Source[GetTransactionsResponse, NotUsed],
      serverStreamFinished: Future[Done],
      userManagementStore: UserManagementStore,
      nowRef: AtomicReference[Instant],
  ) {
    def waitForServerPekkoStream: Option[Throwable] = {
      logger.debug("Started waiting for the server stream to finish.")
      Try(
        serverStreamFinished
          .futureValue(timeout = PatienceConfiguration.Timeout(FiniteDuration(5, "seconds")))
      ).toEither.swap.toOption
    }

    def changeUserRights = {
      userManagementStore
        .revokeRights(
          id = Ref.UserId.assertFromString(userId),
          rights = Set(CanReadAs(partyId1)),
          identityProviderId = IdentityProviderId.Default,
        )(LoggingContextWithTrace.ForTesting)
        .futureValue
        .isRight shouldBe true
    }

    def expireUserClaims =
      nowRef.getAndUpdate(x => x.plusSeconds(20))
  }

  private val userId = "user-id"
  val partyId1 = Ref.Party.assertFromString("party1")

  private def test(body: Fixture => Future[Any]): Future[Assertion] = {
    val ledgerId = "ledger-id"
    val participantId = "participant-id"
    val nowRef = new AtomicReference(Instant.now())
    val partyId2 = Ref.Party.assertFromString("party2")
    val claimSetFixture = ClaimSet.Claims(
      claims = List[Claim](ClaimPublic, ClaimReadAsParty(partyId1), ClaimReadAsParty(partyId2)),
      ledgerId = Some(ledgerId),
      participantId = Some(participantId),
      applicationId = Some(userId),
      expiration = Some(nowRef.get().plusSeconds(10)),
      identityProviderId = IdentityProviderId.Default,
      resolvedFromUser = true,
    )
    val authorizationClaimSetFixtureInterceptor = new ServerInterceptor {
      override def interceptCall[ReqT, RespT](
          call: ServerCall[ReqT, RespT],
          headers: Metadata,
          next: ServerCallHandler[ReqT, RespT],
      ): ServerCall.Listener[ReqT] = {
        val nextCtx =
          Context.current.withValue(AuthorizationInterceptor.contextKeyClaimSet, claimSetFixture)
        Contexts.interceptCall(nextCtx, call, headers, next)
      }
    }
    val userManagementStore = new InMemoryUserManagementStore(loggerFactory = loggerFactory)
    userManagementStore
      .createUser(
        user = User(
          id = Ref.UserId.assertFromString(userId),
          primaryParty = None,
          identityProviderId = IdentityProviderId.Default,
        ),
        rights = Set(
          CanReadAs(partyId1),
          CanReadAs(partyId2),
        ),
      )(LoggingContextWithTrace.ForTesting)
      .futureValue
      .isRight shouldBe true
    val authorizer = new Authorizer(
      now = () => nowRef.get(),
      ledgerId = ledgerId,
      participantId = participantId,
      userManagementStore = userManagementStore,
      ec = ec,
      userRightsCheckIntervalInSeconds = 1,
      pekkoScheduler = system.scheduler,
      jwtTimestampLeeway = None,
      telemetry = NoOpTelemetry,
      loggerFactory = loggerFactory,
    )
    val outerLoggerFactory = loggerFactory
    val transactionStreamTerminationPromise = Promise[Done]()
    val apiTransactionServiceFixture = new TransactionService
      with StreamingServiceLifecycleManagement {
      override def getTransactions(
          request: GetTransactionsRequest,
          responseObserver: StreamObserver[GetTransactionsResponse],
      ): Unit = registerStream(responseObserver) {
        Source
          .fromIterator(() => Iterator.continually(GetTransactionsResponse()))
          .map { elem =>
            Threading.sleep(200)
            logger.debug("sent")
            elem
          }
          .watchTermination() { case (mat, doneF) =>
            doneF.onComplete(transactionStreamTerminationPromise.complete)
            mat
          }
      }

      override protected def loggerFactory: NamedLoggerFactory = outerLoggerFactory

      def notSupported = throw new UnsupportedOperationException()

      override def getTransactionTrees(
          request: GetTransactionsRequest,
          responseObserver: StreamObserver[GetTransactionTreesResponse],
      ): Unit = notSupported

      override def getTransactionByEventId(
          request: GetTransactionByEventIdRequest
      ): Future[GetTransactionResponse] = notSupported

      override def getTransactionById(
          request: GetTransactionByIdRequest
      ): Future[GetTransactionResponse] = notSupported

      override def getFlatTransactionByEventId(
          request: GetTransactionByEventIdRequest
      ): Future[GetFlatTransactionResponse] = notSupported

      override def getFlatTransactionById(
          request: GetTransactionByIdRequest
      ): Future[GetFlatTransactionResponse] = notSupported

      override def getLedgerEnd(request: GetLedgerEndRequest): Future[GetLedgerEndResponse] =
        notSupported

      override def getLatestPrunedOffsets(
          request: GetLatestPrunedOffsetsRequest
      ): Future[GetLatestPrunedOffsetsResponse] = notSupported
    }
    val grpcServerPort = UniquePortGenerator.next
    val authorizedTransactionServiceOwner =
      ResourceOwner.forCloseable(() =>
        new TransactionServiceAuthorization(apiTransactionServiceFixture, authorizer)
      )

    def grpcServerOwnerFor(bindableService: BindableService) = GrpcServer.owner(
      address = None,
      desiredPort = grpcServerPort,
      maxInboundMessageSize = ApiServiceOwner.DefaultMaxInboundMessageSize,
      sslContext = None,
      interceptors = List(authorizationClaimSetFixtureInterceptor),
      metrics = Metrics.ForTesting,
      servicesExecutor = ec,
      services = List(bindableService),
      loggerFactory = loggerFactory,
    )

    val channelOwner = ResourceOwner.forChannel(
      NettyChannelBuilder
        .forAddress("localhost", grpcServerPort.unwrap)
        .usePlaintext(),
      FiniteDuration(10, "seconds"),
    )

    def getTransactions(
        channel: Channel,
        request: GetTransactionsRequest,
    ): Source[GetTransactionsResponse, NotUsed] =
      ClientAdapter.serverStreaming(
        request,
        new TransactionServiceStub(channel).getTransactions,
      )

    val transactionStreamOwner = for {
      transactionService <- authorizedTransactionServiceOwner
      _ <- grpcServerOwnerFor(transactionService)
      grpcChannel <- channelOwner
    } yield {
      getTransactions(
        grpcChannel,
        GetTransactionsRequest(
          ledgerId = ledgerId,
          filter = Some(
            TransactionFilter(
              Map(
                partyId1 -> Filters(),
                partyId2 -> Filters(),
              )
            )
          ),
        ),
      )
    }
    implicit val resourceContext = ResourceContext(ec)
    transactionStreamOwner
      .use { clientStream =>
        logger.info("Server and connected client created.")
        body(
          Fixture(
            clientStream,
            transactionStreamTerminationPromise.future,
            userManagementStore,
            nowRef,
          )
        )
          .transform { result =>
            logger.info("Test finished, starting teardown.")
            Try(result)
          }
      }
      .map { result =>
        logger.info("Teardown finished.")
        result.get // populate error
        succeed
      }
  }
}
