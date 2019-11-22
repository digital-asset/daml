// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.auth

import java.util.UUID

import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.digitalasset.ledger.api.v1.transaction_service._
import com.digitalasset.platform.sandbox.Expect
import com.digitalasset.platform.sandbox.services.{
  SandboxFixtureWithAuth,
  SubmitAndWaitDummyCommand,
  TestCommands
}
import com.digitalasset.timer.Delayed
import io.grpc.Status
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

final class TransactionServiceAuthIT
    extends AsyncFlatSpec
    with SandboxFixtureWithAuth
    with SuiteResourceManagementAroundAll
    with TestCommands
    with Matchers
    with Expect
    with SubmitAndWaitDummyCommand {

  override val appId = classOf[TransactionServiceAuthIT].getSimpleName

  override val submitter = "alice"

  private def ledgerEnd(token: Option[String]): Future[GetLedgerEndResponse] =
    stub(TransactionServiceGrpc.stub(channel), token)
      .getLedgerEnd(new GetLedgerEndRequest(unwrappedLedgerId))

  behavior of "TransactionService#LedgerEnd with authorization"

  it should "deny unauthorized calls" in {
    expect(ledgerEnd(None)).toBeDenied
  }
  it should "allow authenticated calls" in {
    expect(ledgerEnd(Option(rwToken(submitter).asHeader()))).toSucceed
  }
  it should "deny calls with expired tokens" in {
    expect(ledgerEnd(Option(rwToken(submitter).expired.asHeader()))).toBeDenied
  }
  it should "allow calls with non-expired tokens" in {
    expect(ledgerEnd(Option(rwToken(submitter).expiresTomorrow.asHeader()))).toSucceed
  }

  private def transactionRequest =
    new GetTransactionsRequest(unwrappedLedgerId, Some(ledgerBegin), None, txFilterFor(submitter))

  private def transactions(token: Option[String]): Future[Unit] =
    command.flatMap(
      _ =>
        streamResult[GetTransactionsResponse](observer =>
          stub(TransactionServiceGrpc.stub(channel), token)
            .getTransactions(transactionRequest, observer)))

  private def expiringTransactions(token: String): Future[Throwable] =
    expectExpiration[GetTransactionsResponse](
      observer =>
        stub(TransactionServiceGrpc.stub(channel), Some(token))
          .getTransactions(transactionRequest, observer))

  behavior of "TransactionService#GetTransactions with authorization"

  it should "deny unauthorized calls" in {
    expect(transactions(None)).toBeDenied
  }
  it should "allow authenticated calls" in {
    expect(transactions(Option(rwToken(submitter).asHeader()))).toSucceed
  }
  it should "deny calls with expired tokens" in {
    expect(transactions(Option(rwToken(submitter).expired.asHeader()))).toBeDenied
  }
  it should "allow calls with non-expired tokens" in {
    expect(transactions(Option(rwToken(submitter).expiresTomorrow.asHeader()))).toSucceed
  }
  it should "break a stream in flight upon token expiration" in {
    val _ = Delayed.Future.by(10.seconds)(issueCommand())
    expect(expiringTransactions(rwToken(submitter).expiresInFiveSeconds.asHeader())).toSucceed
  }

  private def transactionsTrees(token: Option[String]): Future[Unit] =
    command.flatMap(
      _ =>
        streamResult[GetTransactionTreesResponse](observer =>
          stub(TransactionServiceGrpc.stub(channel), token)
            .getTransactionTrees(transactionRequest, observer)))

  private def expiringTransactionTrees(token: String): Future[Throwable] =
    expectExpiration[GetTransactionTreesResponse](
      observer =>
        stub(TransactionServiceGrpc.stub(channel), Some(token))
          .getTransactionTrees(transactionRequest, observer))

  behavior of "TransactionService#GetTransactionsTrees with authorization"

  it should "deny unauthorized calls" in {
    expect(transactionsTrees(None)).toBeDenied
  }
  it should "allow authenticated calls" in {
    expect(transactionsTrees(Option(rwToken(submitter).asHeader()))).toSucceed
  }
  it should "deny calls with expired tokens" in {
    expect(transactionsTrees(Option(rwToken(submitter).expired.asHeader()))).toBeDenied
  }
  it should "allow calls with non-expired tokens" in {
    expect(transactionsTrees(Option(rwToken(submitter).expiresTomorrow.asHeader()))).toSucceed
  }
  it should "break a stream in flight upon token expiration" in {
    val _ = Delayed.Future.by(10.seconds)(issueCommand())
    expect(expiringTransactionTrees(rwToken(submitter).expiresInFiveSeconds.asHeader())).toSucceed
  }

  private def getTransactionById(token: Option[String]): Future[GetTransactionResponse] =
    stub(TransactionServiceGrpc.stub(channel), token)
      .getTransactionById(
        new GetTransactionByIdRequest(unwrappedLedgerId, UUID.randomUUID.toString, List(submitter)))

  behavior of "TransactionService#GetTransactionById with authorization"

  it should "deny unauthorized calls" in {
    expect(getTransactionById(None)).toBeDenied
  }
  it should "deny calls authorized for the wrong party" in {
    expect(getTransactionById(Option(rwToken("bob").asHeader()))).toBeDenied
  }
  it should "allow authenticated calls" in {
    expect(getTransactionById(Option(rwToken(submitter).asHeader())))
      .toFailWith(Status.Code.NOT_FOUND)
  }
  it should "deny calls with expired tokens" in {
    expect(getTransactionById(Option(rwToken(submitter).expired.asHeader())))
      .toFailWith(Status.Code.PERMISSION_DENIED)
  }
  it should "allow calls with non-expired tokens" in {
    expect(getTransactionById(Option(rwToken(submitter).expiresTomorrow.asHeader())))
      .toFailWith(Status.Code.NOT_FOUND)
  }

  private def getTransactionByEventId(token: Option[String]): Future[GetTransactionResponse] =
    stub(TransactionServiceGrpc.stub(channel), token)
      .getTransactionByEventId(
        new GetTransactionByEventIdRequest(
          unwrappedLedgerId,
          UUID.randomUUID.toString,
          List(submitter)))

  behavior of "TransactionService#GetTransactionByEventId with authorization"

  it should "deny unauthorized calls" in {
    expect(getTransactionByEventId(None)).toBeDenied
  }
  it should "deny calls authorized for the wrong party" in {
    expect(getTransactionByEventId(Option(rwToken("bob").asHeader()))).toBeDenied
  }
  it should "allow authenticated calls" in {
    expect(getTransactionByEventId(Option(rwToken(submitter).asHeader())))
      .toFailWith(Status.Code.NOT_FOUND)
  }
  it should "deny calls with expired tokens" in {
    expect(getTransactionByEventId(Option(rwToken(submitter).expired.asHeader())))
      .toFailWith(Status.Code.PERMISSION_DENIED)
  }
  it should "allow calls with non-expired tokens" in {
    expect(getTransactionByEventId(Option(rwToken(submitter).expiresTomorrow.asHeader())))
      .toFailWith(Status.Code.NOT_FOUND)
  }

  private def getFlatTransactionById(token: Option[String]): Future[GetFlatTransactionResponse] =
    stub(TransactionServiceGrpc.stub(channel), token)
      .getFlatTransactionById(
        new GetTransactionByIdRequest(unwrappedLedgerId, UUID.randomUUID.toString, List(submitter)))

  behavior of "TransactionService#GetFlatTransactionById with authorization"

  it should "deny unauthorized calls" in {
    expect(getFlatTransactionById(None)).toBeDenied
  }
  it should "deny calls authorized for the wrong party" in {
    expect(getFlatTransactionById(Option(rwToken("bob").asHeader()))).toBeDenied
  }
  it should "allow authenticated calls" in {
    expect(getFlatTransactionById(Option(rwToken(submitter).asHeader())))
      .toFailWith(Status.Code.NOT_FOUND)
  }
  it should "deny calls with expired tokens" in {
    expect(getFlatTransactionById(Option(rwToken(submitter).expired.asHeader())))
      .toFailWith(Status.Code.PERMISSION_DENIED)
  }
  it should "allow calls with non-expired tokens" in {
    expect(getFlatTransactionById(Option(rwToken(submitter).expiresTomorrow.asHeader())))
      .toFailWith(Status.Code.NOT_FOUND)
  }

  private def getFlatTransactionByEventId(
      token: Option[String]): Future[GetFlatTransactionResponse] =
    stub(TransactionServiceGrpc.stub(channel), token)
      .getFlatTransactionByEventId(
        new GetTransactionByEventIdRequest(
          unwrappedLedgerId,
          UUID.randomUUID.toString,
          List(submitter)))

  behavior of "TransactionService#GetFlatTransactionByEventId with authorization"

  it should "deny unauthorized calls" in {
    expect(getFlatTransactionByEventId(None)).toBeDenied
  }
  it should "deny calls authorized for the wrong party" in {
    expect(getFlatTransactionByEventId(Option(rwToken("bob").asHeader()))).toBeDenied
  }
  it should "allow authenticated calls" in {
    expect(getFlatTransactionByEventId(Option(rwToken(submitter).asHeader())))
      .toFailWith(Status.Code.NOT_FOUND)
  }
  it should "deny calls with expired tokens" in {
    expect(getFlatTransactionByEventId(Option(rwToken(submitter).expired.asHeader())))
      .toFailWith(Status.Code.PERMISSION_DENIED)
  }
  it should "allow calls with non-expired tokens" in {
    expect(getFlatTransactionByEventId(Option(rwToken(submitter).expiresTomorrow.asHeader())))
      .toFailWith(Status.Code.NOT_FOUND)
  }

}
