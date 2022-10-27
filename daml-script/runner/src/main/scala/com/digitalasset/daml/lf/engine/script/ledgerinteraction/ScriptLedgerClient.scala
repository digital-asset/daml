// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.ledgerinteraction

import akka.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.{PartyDetails, User, UserRight}
import com.daml.lf.command
import com.daml.lf.data.Ref._
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.speedy.SValue
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import io.grpc.StatusRuntimeException
import scalaz.OneAnd

import scala.concurrent.{ExecutionContext, Future}

// We have our own type for time modes since TimeProviderType
// allows for more stuff that doesn’t make sense in Daml Script.
sealed trait ScriptTimeMode

object ScriptTimeMode {
  final case object Static extends ScriptTimeMode
  final case object WallClock extends ScriptTimeMode
}

object ScriptLedgerClient {

  sealed trait CommandResult
  final case class CreateResult(contractId: ContractId) extends CommandResult
  final case class ExerciseResult(
      templateId: Identifier,
      interfaceId: Option[Identifier],
      choice: ChoiceName,
      result: Value,
  ) extends CommandResult

  final case class ActiveContract(
      templateId: Identifier,
      contractId: ContractId,
      argument: Value,
  )

  final case class TransactionTree(rootEvents: List[TreeEvent])
  sealed trait TreeEvent
  final case class Exercised(
      templateId: Identifier,
      interfaceId: Option[Identifier],
      contractId: ContractId,
      choice: ChoiceName,
      argument: Value,
      childEvents: List[TreeEvent],
  ) extends TreeEvent
  final case class Created(
      templateId: Identifier,
      contractId: ContractId,
      argument: Value,
  ) extends TreeEvent
}

// This abstracts over the interaction with the ledger. This allows
// us to plug in something that interacts with the JSON API as well as
// something that works against the gRPC API.
trait ScriptLedgerClient {
  def query(parties: OneAnd[Set, Ref.Party], templateId: Identifier)(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Seq[ScriptLedgerClient.ActiveContract]]

  protected def transport: String

  final protected def unsupportedOn(what: String) =
    Future.failed(
      new UnsupportedOperationException(
        s"$what is not supported when running Daml Script over the $transport"
      )
    )

  def queryContractId(parties: OneAnd[Set, Ref.Party], templateId: Identifier, cid: ContractId)(
      implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Option[ScriptLedgerClient.ActiveContract]]

  def queryView(
      parties: OneAnd[Set, Ref.Party],
      interfaceId: Identifier,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Seq[(ContractId, Value)]]

  def queryViewContractId(
      parties: OneAnd[Set, Ref.Party],
      interfaceId: Identifier,
      cid: ContractId,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Option[Value]]

  def queryContractKey(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
      key: SValue,
      translateKey: (Identifier, Value) => Either[String, SValue],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Option[ScriptLedgerClient.ActiveContract]]

  def submit(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Either[StatusRuntimeException, Seq[ScriptLedgerClient.CommandResult]]]

  def submitMustFail(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit ec: ExecutionContext, mat: Materializer): Future[Either[Unit, Unit]]

  def submitTree(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit ec: ExecutionContext, mat: Materializer): Future[ScriptLedgerClient.TransactionTree]

  def allocateParty(partyIdHint: String, displayName: String)(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Ref.Party]

  def listKnownParties()(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[List[PartyDetails]]

  def getStaticTime()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Time.Timestamp]

  def setStaticTime(
      time: Time.Timestamp
  )(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory, mat: Materializer): Future[Unit]

  def createUser(user: User, rights: List[UserRight])(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[Unit]]

  def getUser(id: UserId)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[User]]

  def deleteUser(id: UserId)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[Unit]]

  def listAllUsers()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[List[User]]

  def grantUserRights(id: UserId, rights: List[UserRight])(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[List[UserRight]]]

  def revokeUserRights(id: UserId, rights: List[UserRight])(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[List[UserRight]]]

  def listUserRights(id: UserId)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[List[UserRight]]]
}
