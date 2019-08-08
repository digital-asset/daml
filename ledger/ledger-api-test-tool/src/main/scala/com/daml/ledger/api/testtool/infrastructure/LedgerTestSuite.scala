// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.time.Instant

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite.SkipTestException
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.digitalasset.ledger.api.v1.value.Identifier
import io.grpc.{Status, StatusException, StatusRuntimeException}
import com.digitalasset.ledger.client.binding.{Contract, Primitive, Template, ValueDecoder}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

private[testtool] object LedgerTestSuite {

  final case class SkipTestException(message: String) extends RuntimeException(message)

}

private[testtool] abstract class LedgerTestSuite(val session: LedgerSession) {

  val name: String = getClass.getSimpleName

  val tests: Vector[LedgerTest] = Vector.empty

  final def skip(reason: String): Future[Unit] = Future.failed(new SkipTestException(reason))

  final def skipIf(reason: String)(p: => Boolean): Future[Unit] =
    if (p) skip(reason) else Future.successful(())

  final def time()(implicit context: LedgerTestContext): Future[Instant] =
    context.time

  final def passTime(t: Duration)(implicit context: LedgerTestContext): Future[Unit] =
    context.passTime(t)

  final def activeContracts(party: String, parties: String*)(
      implicit context: LedgerTestContext): Future[Vector[CreatedEvent]] =
    context.activeContracts(party +: parties, Seq.empty)

  final def activeContractsByTemplateId(party: String, parties: String*)(templateIds: Identifier*)(
      implicit context: LedgerTestContext): Future[Vector[CreatedEvent]] =
    context.activeContracts(party +: parties, templateIds)

  final def ledgerId()(implicit context: LedgerTestContext): Future[String] =
    context.ledgerId

  final def allocateParty()(implicit context: LedgerTestContext): Future[String] =
    context.allocateParty()

  final def allocateParties(n: Int)(implicit context: LedgerTestContext): Future[Vector[String]] =
    Future.sequence(Vector.fill(n)(allocateParty()))

  final def create[T <: Template[T]: ValueDecoder](template: Template[T])(party: String)(
      implicit context: LedgerTestContext): Future[Contract[T]] =
    context.create(party, template)

  final def exercise[T](exercise: Primitive.Update[T])(party: String)(
      implicit context: LedgerTestContext): Future[Unit] =
    context.exercise(party, exercise)

  final def flatTransactions(party: String, parties: String*)(
      implicit context: LedgerTestContext): Future[Vector[Transaction]] =
    context.flatTransactions(party +: parties, Seq.empty)

  final def flatTransactionsByTemplateId(party: String, parties: String*)(templateIds: Identifier*)(
      implicit context: LedgerTestContext): Future[Vector[Transaction]] =
    context.flatTransactions(party +: parties, templateIds)

  final def transactionTrees(party: String, parties: String*)(
      implicit context: LedgerTestContext): Future[Vector[TransactionTree]] =
    context.transactionTrees(party +: parties, Seq.empty)

  final def transactionTreesByTemplateId(party: String, parties: String*)(templateIds: Identifier*)(
      implicit context: LedgerTestContext): Future[Vector[TransactionTree]] =
    context.transactionTrees(party +: parties, templateIds)

  final def assertGrpcError[A](t: Throwable, expectedCode: Status.Code, pattern: String)(
      implicit ec: ExecutionContext): Unit = {
    assert(
      t.isInstanceOf[StatusRuntimeException] || t.isInstanceOf[StatusException],
      "Exception is neither a StatusRuntimeException nor a StatusException")
    val (actualCode, message) = t match {
      case sre: StatusRuntimeException => (sre.getStatus.getCode, sre.getStatus.getDescription)
      case se: StatusException => (se.getStatus.getCode, se.getStatus.getDescription)
    }
    assert(actualCode == expectedCode, s"Expected code [$expectedCode], but got [$actualCode].")
    assert(
      message.contains(pattern),
      s"Error message did not contain [$pattern], but was [$message].")
  }
}
