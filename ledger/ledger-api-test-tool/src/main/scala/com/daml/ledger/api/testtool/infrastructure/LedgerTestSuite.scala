// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.time.Instant

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite.SkipTestException
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.commands.Command
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.ledger.client.binding.Primitive.Party
import com.digitalasset.ledger.client.binding.{Contract, Primitive, Template, ValueDecoder}
import io.grpc.{Status, StatusException, StatusRuntimeException}
import scalapb.lenses.{Lens, Mutation}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

private[testtool] object LedgerTestSuite {

  final case class SkipTestException(message: String) extends RuntimeException(message)

}

private[testtool] abstract class LedgerTestSuite(val session: LedgerSession) {

  val name: String = getClass.getSimpleName

  val tests: Vector[LedgerTest] = Vector.empty

  final def skip(reason: String): Future[Unit] = Future.failed(SkipTestException(reason))

  final def skipIf(reason: String)(p: => Boolean): Future[Unit] =
    if (p) skip(reason) else Future.successful(())

  final def time()(implicit context: LedgerTestContext): Future[Instant] =
    context.time

  final def passTime(t: Duration)(implicit context: LedgerTestContext): Future[Unit] =
    context.passTime(t)

  final def activeContracts(party: Party, parties: Party*)(
      implicit context: LedgerTestContext): Future[Vector[CreatedEvent]] =
    context.activeContracts(party +: parties, Seq.empty)

  final def activeContractsByTemplateId(party: Party, parties: Party*)(templateIds: Identifier*)(
      implicit context: LedgerTestContext): Future[Vector[CreatedEvent]] =
    context.activeContracts(party +: parties, templateIds)

  final def ledgerId()(implicit context: LedgerTestContext): Future[String] =
    context.ledgerId

  final def allocateParty()(implicit context: LedgerTestContext): Future[Party] =
    context.allocateParty()

  final def allocateParties(n: Int)(implicit context: LedgerTestContext): Future[Vector[Party]] =
    Future.sequence(Vector.fill(n)(allocateParty()))

  final def create[T <: Template[T]: ValueDecoder](template: Template[T])(party: Party)(
      implicit context: LedgerTestContext): Future[Contract[T]] =
    context.create(party, template)

  final def createAndGetTransactionId[T <: Template[T]: ValueDecoder](template: Template[T])(
      party: Party)(implicit context: LedgerTestContext): Future[(String, Contract[T])] =
    context.createAndGetTransactionId(party, template)

  final def exercise[T](exercise: Party => Primitive.Update[T])(party: Party)(
      implicit context: LedgerTestContext): Future[TransactionTree] =
    context.exercise(party, exercise)

  final def flatTransactions(party: Party, parties: Party*)(
      implicit context: LedgerTestContext): Future[Vector[Transaction]] =
    context.flatTransactions(party +: parties, Seq.empty)

  final def flatTransactionsByTemplateId(party: Party, parties: Party*)(templateIds: Identifier*)(
      implicit context: LedgerTestContext): Future[Vector[Transaction]] =
    context.flatTransactions(party +: parties, templateIds)

  final def transactionTrees(party: Party, parties: Party*)(
      implicit context: LedgerTestContext): Future[Vector[TransactionTree]] =
    context.transactionTrees(party +: parties, Seq.empty)

  final def transactionTreeById(transactionId: String, party: Party, parties: Party*)(
      implicit context: LedgerTestContext): Future[TransactionTree] =
    context.transactionTreeById(transactionId, party +: parties)

  final def transactionTreesByTemplateId(party: Party, parties: Party*)(templateIds: Identifier*)(
      implicit context: LedgerTestContext): Future[Vector[TransactionTree]] =
    context.transactionTrees(party +: parties, templateIds)

  def submitAndWait(
      party: Party,
      command: Command,
      pollutants: (
          Lens[SubmitAndWaitRequest, SubmitAndWaitRequest] => Mutation[SubmitAndWaitRequest])*)(
      implicit context: LedgerTestContext): Future[Unit] =
    for {
      request <- context.prepareSubmission(party, command)
      polluted = request.update(pollutants: _*)
      response <- context.submitAndWait(polluted)
    } yield response

  def submitAndWaitForTransactionId(
      party: Party,
      command: Command,
      pollutants: (
          Lens[SubmitAndWaitRequest, SubmitAndWaitRequest] => Mutation[SubmitAndWaitRequest])*)(
      implicit context: LedgerTestContext): Future[String] =
    for {
      request <- context.prepareSubmission(party, command)
      polluted = request.update(pollutants: _*)
      response <- context.submitAndWaitForTransactionId(polluted)
    } yield response

  def submitAndWaitForTransaction(
      party: Party,
      command: Command,
      pollutants: (
          Lens[SubmitAndWaitRequest, SubmitAndWaitRequest] => Mutation[SubmitAndWaitRequest])*)(
      implicit context: LedgerTestContext): Future[Transaction] =
    for {
      request <- context.prepareSubmission(party, command)
      polluted = request.update(pollutants: _*)
      response <- context.submitAndWaitForTransaction(polluted)
    } yield response

  def submitAndWaitForTransactionTree(
      party: Party,
      command: Command,
      pollutants: (
          Lens[SubmitAndWaitRequest, SubmitAndWaitRequest] => Mutation[SubmitAndWaitRequest])*)(
      implicit context: LedgerTestContext): Future[TransactionTree] =
    for {
      request <- context.prepareSubmission(party, command)
      polluted = request.update(pollutants: _*)
      response <- context.submitAndWaitForTransactionTree(polluted)
    } yield response

  final def assertGrpcError[A](t: Throwable, expectedCode: Status.Code, pattern: String)(
      implicit ec: ExecutionContext): Unit = {

    val (actualCode, message) = t match {
      case sre: StatusRuntimeException => (sre.getStatus.getCode, sre.getStatus.getDescription)
      case se: StatusException => (se.getStatus.getCode, se.getStatus.getDescription)
      case _ =>
        throw new AssertionError(
          "Exception is neither a StatusRuntimeException nor a StatusException")
    }
    assert(actualCode == expectedCode, s"Expected code [$expectedCode], but got [$actualCode].")
    assert(
      message.contains(pattern),
      s"Error message did not contain [$pattern], but was [$message].")
  }
}
