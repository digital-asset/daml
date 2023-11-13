// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script
package v2.ledgerinteraction

import org.apache.pekko.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.{PartyDetails, User, UserRight}
import com.daml.lf.command
import com.daml.lf.data.Ref._
import com.daml.lf.data.{Bytes, Ref, Time}
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.speedy.SValue
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import io.grpc.StatusRuntimeException
import scalaz.OneAnd
import com.daml.lf.engine.script.{ledgerinteraction => abstractLedgers}

import scala.concurrent.{ExecutionContext, Future}

object ScriptLedgerClient {

  sealed trait CommandResult
  final case class CreateResult(contractId: ContractId) extends CommandResult
  final case class ExerciseResult(
      templateId: Identifier,
      interfaceId: Option[Identifier],
      choice: ChoiceName,
      result: Value,
  ) extends CommandResult

  type ActiveContract = Created
  val ActiveContract: Created.type = Created

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
      blob: Bytes,
  ) extends TreeEvent

  def realiseScriptLedgerClient(
      ledger: abstractLedgers.ScriptLedgerClient,
      enableContractUpgrading: Boolean,
  ): ScriptLedgerClient =
    ledger match {
      case abstractLedgers.GrpcLedgerClient(grpcClient, applicationId, oAdminClient) =>
        new grpcLedgerClient.GrpcLedgerClient(
          grpcClient,
          applicationId,
          oAdminClient,
          enableContractUpgrading,
        )
      case abstractLedgers.JsonLedgerClient(uri, token, envIface, actorSystem) =>
        if (enableContractUpgrading)
          throw new IllegalArgumentException("The JSON client does not support Upgrades")
        new JsonLedgerClient(uri, token, envIface, actorSystem)
      case abstractLedgers.IdeLedgerClient(compiledPackages, traceLog, warningLog, canceled) =>
        if (enableContractUpgrading)
          throw new IllegalArgumentException("The IDE Ledger client does not support Upgrades")
        new IdeLedgerClient(compiledPackages, traceLog, warningLog, canceled)
    }

  // Essentially PackageMetadata but without the possibility of extension
  final case class ReadablePackageId(
      name: PackageName,
      version: PackageVersion,
  )
}

// This abstracts over the interaction with the ledger. This allows
// us to plug in something that interacts with the JSON API as well as
// something that works against the gRPC API.
trait ScriptLedgerClient {
  def query(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Seq[ScriptLedgerClient.ActiveContract]]

  protected def transport: String

  val enableContractUpgrading: Boolean = false

  final protected def unsupportedOn(what: String) =
    Future.failed(
      new UnsupportedOperationException(
        s"$what is not supported when running Daml Script over the $transport"
      )
    )

  def queryContractId(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
      cid: ContractId,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Option[ScriptLedgerClient.ActiveContract]]

  def queryInterface(
      parties: OneAnd[Set, Ref.Party],
      interfaceId: Identifier,
      viewType: Ast.Type,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Seq[(ContractId, Option[Value])]]

  def queryInterfaceContractId(
      parties: OneAnd[Set, Ref.Party],
      interfaceId: Identifier,
      viewType: Ast.Type,
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
      disclosures: List[Disclosure],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Either[StatusRuntimeException, Seq[ScriptLedgerClient.CommandResult]]]

  def submitMustFail(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      disclosures: List[Disclosure],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit ec: ExecutionContext, mat: Materializer): Future[Either[Unit, Unit]]

  def submitTree(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      disclosures: List[Disclosure],
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

  def trySubmit(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      disclosures: List[Disclosure],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
      languageVersionLookup: PackageId => Either[String, LanguageVersion],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Either[SubmitError, Seq[ScriptLedgerClient.CommandResult]]]

  def trySubmitConcurrently(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      commandss: List[List[command.ApiCommand]],
      optLocation: Option[Location],
      languageVersionLookup: PackageId => Either[String, LanguageVersion],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[List[Either[SubmitError, Seq[ScriptLedgerClient.CommandResult]]]]

  def vetPackages(packages: List[ScriptLedgerClient.ReadablePackageId])(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit]

  def unvetPackages(packages: List[ScriptLedgerClient.ReadablePackageId])(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit]

  def listVettedPackages()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[List[ScriptLedgerClient.ReadablePackageId]]

  def listAllPackages()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[List[ScriptLedgerClient.ReadablePackageId]]

  def vetDar(name: String)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit]

  def unvetDar(name: String)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit]

  // TEMPORARY AND INTERNAL - once we have decided on a proper daml3-script upgrading interface for users, we will update this to be
  // specified per command
  // https://github.com/digital-asset/daml/issues/17703
  def setProvidePackageId(shouldProvide: Boolean)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit]
}
