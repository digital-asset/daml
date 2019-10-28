// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import java.io.File
import java.nio.file.Files
import java.util.UUID

import com.digitalasset.daml.bazeltools.BazelRunfiles.rlocation
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.auth.{Claim, ClaimActAsParty, ClaimAdmin, ClaimPublic, Claims}
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.active_contracts_service.{
  GetActiveContractsRequest,
  GetActiveContractsResponse
}
import com.digitalasset.ledger.api.v1.admin.package_management_service.{
  ListKnownPackagesRequest,
  UploadDarFileRequest
}
import com.digitalasset.ledger.api.v1.admin.party_management_service.{
  AllocatePartyRequest,
  ListKnownPartiesRequest
}
import com.digitalasset.ledger.api.v1.command_completion_service.{
  CompletionEndRequest,
  CompletionStreamRequest,
  CompletionStreamResponse
}
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.ledger_configuration_service.{
  GetLedgerConfigurationRequest,
  GetLedgerConfigurationResponse
}
import com.digitalasset.ledger.api.v1.ledger_identity_service.GetLedgerIdentityRequest
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.testing.time_service.{GetTimeRequest, GetTimeResponse}
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetLedgerEndRequest,
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionTreesResponse,
  GetTransactionsRequest,
  GetTransactionsResponse
}
import com.digitalasset.platform.apitesting.{
  LedgerContext,
  MultiLedgerFixture,
  TestCommands,
  TestIdsGenerator
}
import com.digitalasset.platform.server.api.authorization.auth.AuthServiceStatic
import com.google.protobuf.ByteString
import io.grpc.{Status, StatusException, StatusRuntimeException}
import io.grpc.stub.StreamObserver
import org.scalatest.concurrent.AsyncTimeLimitedTests
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.{Future, Promise}
import scalaz.syntax.tag._

class AuthorizationIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture
    with SuiteResourceManagementAroundAll
    with AsyncTimeLimitedTests
    with Matchers {

  protected val testCommands = new TestCommands(config)
  protected val testIdsGenerator = new TestIdsGenerator(config)

  private val operator = testIdsGenerator.testPartyName("Operator")
  private val alice = testIdsGenerator.testPartyName("Alice")
  private val bob = testIdsGenerator.testPartyName("Bob")

  private val operatorHeader = s"Bearer $operator"
  private val aliceHeader = s"Bearer $alice"
  private val bobHeader = s"Bearer $bob"

  private val testApplicationId = "AuthorizationIT"

  override protected def config: Config =
    Config.default
      .withAuthService(AuthServiceStatic({
        case `aliceHeader` =>
          Claims(List[Claim](ClaimPublic, ClaimActAsParty(Ref.Party.assertFromString(alice))))
        case `bobHeader` =>
          Claims(List[Claim](ClaimPublic, ClaimActAsParty(Ref.Party.assertFromString(bob))))
        case `operatorHeader` =>
          Claims(
            List[Claim](
              ClaimPublic,
              ClaimAdmin,
              ClaimActAsParty(Ref.Party.assertFromString(operator))))
      }))

  "ActiveContractsService" when {
    "getActiveContracts" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val ledgerId = ctxNone.ledgerId.unwrap
        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)
        val ctxBob = ctxNone.withAuthorizationHeader(bobHeader)

        def call(ctx: LedgerContext, party: String) =
          streamResult[GetActiveContractsResponse](
            observer =>
              ctx.acsService.getActiveContracts(
                new GetActiveContractsRequest(ledgerId, txFilterFor(party)),
                observer))

        for {
          _ <- mustBeDenied(call(ctxNone, alice)) // Reading the ACS for Alice without authorization
          _ <- mustBeDenied(call(ctxBob, alice)) // Reading the ACS for Alice as Bob
          _ <- call(ctxAlice, alice) // Reading the ACS for Alice as Alice
        } yield {
          assert(true)
        }
      }
    }
  }

  "CommandCompletionService" when {
    "completionEnd" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val ledgerId = ctxNone.ledgerId.unwrap
        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)

        def call(ctx: LedgerContext) =
          ctx.commandCompletionService.completionEnd(new CompletionEndRequest(ledgerId))

        for {
          _ <- mustBeDenied(call(ctxNone)) // Reading completion end without authorization
          _ <- call(ctxAlice) // Reading completion end with authorization
        } yield {
          assert(true)
        }
      }
    }
    "completionStream" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val ledgerId = ctxNone.ledgerId
        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)
        val ctxBob = ctxNone.withAuthorizationHeader(bobHeader)

        def call(ctx: LedgerContext, party: String) =
          streamResult[CompletionStreamResponse](
            observer =>
              ctx.commandCompletionService.completionStream(
                new CompletionStreamRequest(
                  ledgerId.unwrap,
                  testApplicationId,
                  List(party),
                  Some(ledgerBegin)),
                observer))

        for {
          // Create some commands so that the completion stream is not empty
          _ <- ctxAlice.commandSubmissionService.submit(dummyCommandRequest(ledgerId, alice))

          _ <- mustBeDenied(call(ctxNone, alice)) // Reading completions for Alice without authorization
          _ <- mustBeDenied(call(ctxBob, alice)) // Reading completions for Alice as Bob
          _ <- call(ctxAlice, alice) // Reading completions for Alice as Alice
        } yield {
          assert(true)
        }
      }
    }
  }

  "CommandService" when {
    "submitAndWait" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val ledgerId = ctxNone.ledgerId
        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)
        val ctxBob = ctxNone.withAuthorizationHeader(bobHeader)

        def call(ctx: LedgerContext, party: String) =
          ctx.commandService.submitAndWait(dummySubmitAndWaitRequest(ledgerId, party))

        for {
          _ <- mustBeDenied(call(ctxNone, alice)) // Submitting commands for Alice without authorization
          _ <- mustBeDenied(call(ctxBob, alice)) // Submitting commands for Alice as Bob
          _ <- call(ctxAlice, alice) // Submitting commands for Alice as Alice
        } yield {
          assert(true)
        }
      }
    }
    "submitAndWaitForTransaction" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val ledgerId = ctxNone.ledgerId
        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)
        val ctxBob = ctxNone.withAuthorizationHeader(bobHeader)

        def call(ctx: LedgerContext, party: String) =
          ctx.commandService.submitAndWaitForTransaction(dummySubmitAndWaitRequest(ledgerId, party))

        for {
          _ <- mustBeDenied(call(ctxNone, alice)) // Submitting commands for Alice without authorization
          _ <- mustBeDenied(call(ctxBob, alice)) // Submitting commands for Alice as Bob
          _ <- call(ctxAlice, alice) // Submitting commands for Alice as Alice
        } yield {
          assert(true)
        }
      }
    }
    "submitAndWaitForTransactionId" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val ledgerId = ctxNone.ledgerId
        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)
        val ctxBob = ctxNone.withAuthorizationHeader(bobHeader)

        def call(ctx: LedgerContext, party: String) =
          ctx.commandService.submitAndWaitForTransactionId(
            dummySubmitAndWaitRequest(ledgerId, party))

        for {
          _ <- mustBeDenied(call(ctxNone, alice)) // Submitting commands for Alice without authorization
          _ <- mustBeDenied(call(ctxBob, alice)) // Submitting commands for Alice as Bob
          _ <- call(ctxAlice, alice) // Submitting commands for Alice as Alice
        } yield {
          assert(true)
        }
      }
    }
    "submitAndWaitForTransactionTree" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val ledgerId = ctxNone.ledgerId
        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)
        val ctxBob = ctxNone.withAuthorizationHeader(bobHeader)

        def call(ctx: LedgerContext, party: String) =
          ctx.commandService.submitAndWaitForTransactionTree(
            dummySubmitAndWaitRequest(ledgerId, party))

        for {
          _ <- mustBeDenied(call(ctxNone, alice)) // Submitting commands for Alice without authorization
          _ <- mustBeDenied(call(ctxBob, alice)) // Submitting commands for Alice as Bob
          _ <- call(ctxAlice, alice) // Submitting commands for Alice as Alice
        } yield {
          assert(true)
        }
      }
    }
  }

  "CommandSubmissionService" when {
    "submit" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val ledgerId = ctxNone.ledgerId
        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)
        val ctxBob = ctxNone.withAuthorizationHeader(bobHeader)

        def call(ctx: LedgerContext, party: String) =
          ctx.commandSubmissionService.submit(dummyCommandRequest(ledgerId, party))

        for {
          _ <- mustBeDenied(call(ctxNone, alice)) // Submitting commands for Alice without authorization
          _ <- mustBeDenied(call(ctxBob, alice)) // Submitting commands for Alice as Bob
          _ <- call(ctxAlice, alice) // Submitting commands for Alice as Alice
        } yield {
          assert(true)
        }
      }
    }
  }

  "LedgerConfigurationService" when {
    "getLedgerConfiguration" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val ledgerId = ctxNone.ledgerId.unwrap
        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)

        def call(ctx: LedgerContext) =
          streamResult[GetLedgerConfigurationResponse](
            observer =>
              ctx.ledgerConfigurationService
                .getLedgerConfiguration(new GetLedgerConfigurationRequest(ledgerId), observer)
          )

        for {
          _ <- mustBeDenied(call(ctxNone)) // Reading the ledger configuration without authorization
          _ <- call(ctxAlice) // Reading the ledger configuration with authorization
        } yield {
          assert(true)
        }
      }
    }
  }

  "LedgerIdentityService" when {
    "getLedgerIdentity" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)

        def call(ctx: LedgerContext) =
          ctx.ledgerIdentityService.getLedgerIdentity(new GetLedgerIdentityRequest())

        for {
          _ <- mustBeDenied(call(ctxNone)) // Reading the ledger ID without authorization
          _ <- call(ctxAlice) // Reading the ledger ID with authorization
        } yield {
          assert(true)
        }
      }
    }
  }

  "PackageManagementService" when {
    "listKnownPackages" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)
        val ctxAdmin = ctxNone.withAuthorizationHeader(operatorHeader)

        def call(ctx: LedgerContext) =
          ctx.packageManagementService.listKnownPackages(new ListKnownPackagesRequest())

        for {
          _ <- mustBeDenied(call(ctxNone)) // Listing packages without authorization
          _ <- mustBeDenied(call(ctxAlice)) // Listing packages as Alice (a regular user)
          _ <- call(ctxAdmin) // Listing packages as Operator (an admin)
        } yield {
          assert(true)
        }
      }
    }
    "uploadDarFile" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val darFile =
          Files.readAllBytes(new File(rlocation("ledger/test-common/Test-stable.dar")).toPath)

        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)
        val ctxAdmin = ctxNone.withAuthorizationHeader(operatorHeader)

        def call(ctx: LedgerContext) =
          ctx.packageManagementService.uploadDarFile(
            new UploadDarFileRequest(ByteString.copyFrom(darFile)))

        for {
          _ <- mustBeDenied(call(ctxNone)) // Uploading packages without authorization
          _ <- mustBeDenied(call(ctxAlice)) // Uploading packages as Alice (a regular user)
          _ <- call(ctxAdmin) // Uploading packages as Operator (an admin)
        } yield {
          assert(true)
        }
      }
    }
  }

  "PartyManagementService" when {
    "listKnownParties" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)
        val ctxAdmin = ctxNone.withAuthorizationHeader(operatorHeader)

        def call(ctx: LedgerContext) =
          ctx.partyManagementService.listKnownParties(new ListKnownPartiesRequest())

        for {
          _ <- mustBeDenied(call(ctxNone)) // Listing known parties without authorization
          _ <- mustBeDenied(call(ctxAlice)) // Listing known parties as Alice (a regular user)
          _ <- call(ctxAdmin) // Listing known parties as Operator (an admin)
        } yield {
          assert(true)
        }
      }
    }
    "allocateParty" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)
        val ctxAdmin = ctxNone.withAuthorizationHeader(operatorHeader)

        def call(ctx: LedgerContext) =
          ctx.partyManagementService.allocateParty(new AllocatePartyRequest("AuthorizationIT"))

        for {
          _ <- mustBeDenied(call(ctxNone)) // Allocating a party without authorization
          _ <- mustBeDenied(call(ctxAlice)) // Allocating a party as Alice (a regular user)
          _ <- call(ctxAdmin) // Allocating a party as Operator (an admin)
        } yield {
          assert(true)
        }
      }
    }
  }

  "TimeService" when {
    "getTime" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val ledgerId = ctxNone.ledgerId.unwrap
        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)
        val ctxAdmin = ctxNone.withAuthorizationHeader(operatorHeader)

        def call(ctx: LedgerContext) =
          streamResult[GetTimeResponse](observer =>
            ctx.timeService.getTime(new GetTimeRequest(ledgerId), observer))

        for {
          _ <- mustBeDenied(call(ctxNone)) // Getting the time without authorization
          _ <- call(ctxAlice) // Getting the time as Alice (a regular user)
          _ <- call(ctxAdmin) // Getting the time as Operator (an admin)
        } yield {
          assert(true)
        }
      }
    }
  }

  "TransactionService" when {
    "getLedgerEnd" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val ledgerId = ctxNone.ledgerId.unwrap
        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)
        val ctxAdmin = ctxNone.withAuthorizationHeader(operatorHeader)
        def call(ctx: LedgerContext) =
          ctx.transactionService.getLedgerEnd(new GetLedgerEndRequest(ledgerId))

        for {
          _ <- mustBeDenied(call(ctxNone)) // Reading ledger end without authorization
          _ <- call(ctxAlice) // Reading ledger end as Alice (a regular user)
          _ <- call(ctxAdmin) // Reading ledger end as Operator (an admin)
        } yield {
          assert(true)
        }
      }
    }
    "getTransactions" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val ledgerId = ctxNone.ledgerId.unwrap
        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)
        val ctxBob = ctxNone.withAuthorizationHeader(bobHeader)
        def call(ctx: LedgerContext, party: String) =
          streamResult[GetTransactionsResponse](
            observer =>
              ctx.transactionService.getTransactions(
                new GetTransactionsRequest(ledgerId, Some(ledgerBegin), None, txFilterFor(party)),
                observer))

        for {
          _ <- mustBeDenied(call(ctxNone, alice)) // Reading transactions for Alice without authorization
          _ <- mustBeDenied(call(ctxBob, alice)) // Reading transactions for Alice as Bob
          _ <- call(ctxAlice, alice) // Reading transactions for Alice as Alice
        } yield {
          assert(true)
        }
      }
    }
    "getTransactionTrees" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val ledgerId = ctxNone.ledgerId.unwrap
        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)
        val ctxBob = ctxNone.withAuthorizationHeader(bobHeader)
        def call(ctx: LedgerContext, party: String) =
          streamResult[GetTransactionTreesResponse](
            observer =>
              ctx.transactionService.getTransactionTrees(
                new GetTransactionsRequest(ledgerId, Some(ledgerBegin), None, txFilterFor(party)),
                observer))

        for {
          _ <- mustBeDenied(call(ctxNone, alice)) // Reading transactions for Alice without authorization
          _ <- mustBeDenied(call(ctxBob, alice)) // Reading transactions for Alice as Bob
          _ <- call(ctxAlice, alice) // Reading transactions for Alice as Alice
        } yield {
          assert(true)
        }
      }
    }
    "getTransactionById" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val ledgerId = ctxNone.ledgerId.unwrap
        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)
        val ctxBob = ctxNone.withAuthorizationHeader(bobHeader)
        def call(ctx: LedgerContext, party: String) =
          ctx.transactionService.getTransactionById(
            new GetTransactionByIdRequest(ledgerId, "does-not-exist", List(party)))

        for {
          _ <- mustBeDenied(call(ctxNone, alice)) // Reading transactions for Alice without authorization
          _ <- mustBeDenied(call(ctxBob, alice)) // Reading transactions for Alice as Bob
          _ <- mustFailWith(call(ctxAlice, alice), Status.Code.NOT_FOUND) // Reading transactions for Alice as Alice
        } yield {
          assert(true)
        }
      }
    }
    "getTransactionByEventId" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val ledgerId = ctxNone.ledgerId.unwrap
        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)
        val ctxBob = ctxNone.withAuthorizationHeader(bobHeader)
        def call(ctx: LedgerContext, party: String) =
          ctx.transactionService.getTransactionByEventId(
            new GetTransactionByEventIdRequest(ledgerId, "does-not-exist", List(party)))

        for {
          _ <- mustBeDenied(call(ctxNone, alice)) // Reading transactions for Alice without authorization
          _ <- mustBeDenied(call(ctxBob, alice)) // Reading transactions for Alice as Bob
          _ <- mustFailWith(call(ctxAlice, alice), Status.Code.NOT_FOUND) // Reading transactions for Alice as Alice
        } yield {
          assert(true)
        }
      }
    }
    "getFlatTransactionById" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val ledgerId = ctxNone.ledgerId.unwrap
        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)
        val ctxBob = ctxNone.withAuthorizationHeader(bobHeader)
        def call(ctx: LedgerContext, party: String) =
          ctx.transactionService.getFlatTransactionById(
            new GetTransactionByIdRequest(ledgerId, "does-not-exist", List(party)))

        for {
          _ <- mustBeDenied(call(ctxNone, alice)) // Reading transactions for Alice without authorization
          _ <- mustBeDenied(call(ctxBob, alice)) // Reading transactions for Alice as Bob
          _ <- mustFailWith(call(ctxAlice, alice), Status.Code.NOT_FOUND) // Reading transactions for Alice as Alice
        } yield {
          assert(true)
        }
      }
    }
    "getFlatTransactionByEventId" should {
      "work only when authorized" in allFixtures { ctxNone =>
        val ledgerId = ctxNone.ledgerId.unwrap
        val ctxAlice = ctxNone.withAuthorizationHeader(aliceHeader)
        val ctxBob = ctxNone.withAuthorizationHeader(bobHeader)
        def call(ctx: LedgerContext, party: String) =
          ctx.transactionService.getFlatTransactionByEventId(
            new GetTransactionByEventIdRequest(ledgerId, "does-not-exist", List(party)))

        for {
          _ <- mustBeDenied(call(ctxNone, alice)) // Reading transactions for Alice without authorization
          _ <- mustBeDenied(call(ctxBob, alice)) // Reading transactions for Alice as Bob
          _ <- mustFailWith(call(ctxAlice, alice), Status.Code.NOT_FOUND) // Reading transactions for Alice as Alice
        } yield {
          assert(true)
        }
      }
    }
  }

  private def mustBeDenied[T](future: Future[T]): Future[Throwable] =
    mustFailWith(future, Status.Code.PERMISSION_DENIED)

  private def mustFailWith[T](future: Future[T], expectedCode: Status.Code): Future[Throwable] = {
    future.failed.flatMap({
      case sre: StatusRuntimeException if sre.getStatus.getCode == expectedCode =>
        Future.successful(sre)
      case se: StatusException if se.getStatus.getCode == expectedCode => Future.successful(se)
      case t: Throwable =>
        Future.failed(new RuntimeException(s"Expected error $expectedCode, got $t"))
    })
  }

  private def txFilterFor(party: String) = Some(TransactionFilter(Map(party -> Filters())))

  private def ledgerBegin =
    LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))

  /** Returns a future that fails iff the given stream immediately fails. */
  private def streamResult[T](fn: StreamObserver[T] => Unit): Future[Unit] = {
    val promise = Promise[Unit]()
    object observer extends StreamObserver[T] {
      def onNext(value: T): Unit = {
        val _ = promise.trySuccess(())
      }
      def onError(t: Throwable): Unit = {
        val _ = promise.tryFailure(t)
      }
      def onCompleted(): Unit = {
        val _ = promise.trySuccess(())
      }
    }
    fn(observer)
    promise.future
  }

  private def dummyCommandRequest(ledgerId: LedgerId, submitter: String) = {
    val commandId = "AuthorizationIT-" + UUID.randomUUID().toString
    testCommands
      .dummyCommands(ledgerId, commandId, submitter)
      .update(_.commands.applicationId := testApplicationId)
  }

  private def dummySubmitAndWaitRequest(ledgerId: LedgerId, submitter: String) = {
    val request = dummyCommandRequest(ledgerId, submitter)
    SubmitAndWaitRequest(request.commands)
  }
}
