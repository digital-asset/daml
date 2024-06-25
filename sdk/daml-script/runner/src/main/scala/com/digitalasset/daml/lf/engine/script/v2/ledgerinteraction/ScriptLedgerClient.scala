// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script
package v2.ledgerinteraction

import org.apache.pekko.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.ledger.api.domain.{PartyDetails, User, UserRight}
import com.digitalasset.daml.lf.CompiledPackages
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.{Bytes, Ref, Time}
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion}
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId
import scalaz.OneAnd
import com.digitalasset.daml.lf.engine.script.{ledgerinteraction => abstractLedgers}
import com.digitalasset.canton.logging.NamedLoggerFactory

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
      result: Value,
      childEvents: List[TreeEvent],
  ) extends TreeEvent
  final case class Created(
      templateId: Identifier,
      contractId: ContractId,
      argument: Value,
      blob: Bytes,
  ) extends TreeEvent

  def transactionTreeToCommandResults(tree: TransactionTree): List[CommandResult] =
    tree.rootEvents.map {
      case c: Created => CreateResult(c.contractId)
      case e: Exercised => ExerciseResult(e.templateId, e.interfaceId, e.choice, e.result)
    }

  final case class SubmitFailure(
      statusError: RuntimeException,
      submitError: SubmitError,
  )

  // Ideally this lives in ScriptF, but is needed to pass forward to IDELedgerClient
  sealed trait SubmissionErrorBehaviour

  object SubmissionErrorBehaviour {
    final case object MustFail extends SubmissionErrorBehaviour
    final case object MustSucceed extends SubmissionErrorBehaviour
    final case object Try extends SubmissionErrorBehaviour
  }

  final case class CommandWithMeta(command: ApiCommand, explicitPackageId: Boolean)

  def realiseScriptLedgerClient(
      ledger: abstractLedgers.ScriptLedgerClient,
      enableContractUpgrading: Boolean,
      compiledPackages: CompiledPackages,
  )(implicit namedLoggerFactory: NamedLoggerFactory): ScriptLedgerClient =
    ledger match {
      case abstractLedgers.GrpcLedgerClient(grpcClient, applicationId, oAdminClient) =>
        new grpcLedgerClient.GrpcLedgerClient(
          grpcClient,
          applicationId,
          oAdminClient,
          enableContractUpgrading,
          compiledPackages,
        )
      case abstractLedgers.IdeLedgerClient(pureCompiledPackages, traceLog, warningLog, canceled) =>
        if (enableContractUpgrading)
          throw new IllegalArgumentException("The IDE Ledger client does not support Upgrades")
        new IdeLedgerClient(
          pureCompiledPackages,
          traceLog,
          warningLog,
          canceled,
          namedLoggerFactory,
        )
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
      optPackagePreference: Option[List[PackageId]],
      commands: List[ScriptLedgerClient.CommandWithMeta],
      optLocation: Option[Location],
      languageVersionLookup: PackageId => Either[String, LanguageVersion],
      // TODO[SW]: The error behaviour handling logic is written in ScriptF, so each LedgerClient doesn't need to know about it
      // However, the IDELedgerClient knows more about the script being run than it should, and requires to know whether a transaction behaved correctly or not
      // in order to correctly set the partial transaction.
      // Alternative routes attempted:
      // - Omit the partial transaction, use the most recent transaction step.
      //     This doesn't work because we don't know which errors are transaction errors and which aren't - some can be thrown by both submissions and other script questions
      //     We could consider tagging all errors that _can_ be submission errors to include this data, either by adding a field, or using the `addSuppressed` method on RuntimeErrors
      //     In order for this to work, we'll also need an explicit error for submitMustFail succeeding, as right now its a UserError. Likely breaks tests
      //     This method should be discussed.
      // - Wrap any error coming from submission paths in IdeLedgerClient with an error that includes the partial tx - will likely break some tests, but would work
      //     Can't wrap the submitMustFail error, as it is thrown from ScriptF, after the IDELedgerClient, but the scenario service can assume that the most recent Commit is the
      //     partial tx for that.
      //     At that point though, the wrapper only needs to act as a tag to inform the scenario service to take the most recent transaction step as the partial tx
      //     again requires explicit mustFail succeeded error, which will likely break some tests
      // Suppressed exception tag is likely cleanest, though possibly a misuse of this feature.
      errorBehaviour: ScriptLedgerClient.SubmissionErrorBehaviour,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Either[
    ScriptLedgerClient.SubmitFailure,
    (Seq[ScriptLedgerClient.CommandResult], ScriptLedgerClient.TransactionTree),
  ]]

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
}
