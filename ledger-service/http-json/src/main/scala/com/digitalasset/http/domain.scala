// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import java.time.Instant

import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.std.vector._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{Applicative, Functor, Traverse, \/}

import scala.language.higherKinds

object domain {
  type Error = String

  case class JwtPayload(ledgerId: lar.LedgerId, applicationId: lar.ApplicationId, party: lar.Party)

  case class TemplateId[+PkgId](packageId: PkgId, moduleName: String, entityName: String)

  case class ActiveContract[+LfV](
      contractId: String,
      templateId: TemplateId.RequiredPkg,
      key: Option[LfV],
      argument: LfV,
      witnessParties: Seq[String],
      agreementText: String)

  case class ContractLookupRequest[+LfV](
      ledgerId: Option[String],
      id: (TemplateId.OptionalPkg, LfV) \/ (Option[TemplateId.OptionalPkg], String))

  case class GetActiveContractsRequest(templateIds: Set[TemplateId.OptionalPkg])

  case class GetActiveContractsResponse[+LfV](
      offset: String,
      workflowId: Option[String],
      activeContracts: Seq[ActiveContract[LfV]])

  object TemplateId {
    type OptionalPkg = TemplateId[Option[String]]
    type RequiredPkg = TemplateId[String]
    type NoPkg = TemplateId[Unit]

    def fromLedgerApi(in: lav1.value.Identifier): TemplateId.RequiredPkg =
      TemplateId(in.packageId, in.moduleName, in.entityName)
  }

  object ActiveContract {
    def fromLedgerApi(in: lav1.event.CreatedEvent): Error \/ ActiveContract[lav1.value.Value] =
      for {
        templateId <- in.templateId required "templateId"
        argument <- in.createArguments required "createArguments"
        boxedArgument = lav1.value.Value(lav1.value.Value.Sum.Record(argument))
      } yield
        ActiveContract(
          contractId = in.contractId,
          templateId = TemplateId fromLedgerApi templateId,
          key = in.contractKey,
          argument = boxedArgument,
          witnessParties = in.witnessParties,
          agreementText = in.agreementText getOrElse ""
        )

    implicit val covariant: Functor[ActiveContract] = new Functor[ActiveContract] {
      override def map[A, B](fa: ActiveContract[A])(f: A => B) =
        fa.copy(key = fa.key map f, argument = f(fa.argument))
    }
  }

  object ContractLookupRequest {
    implicit val covariant: Functor[ContractLookupRequest] = new Functor[ContractLookupRequest] {
      @SuppressWarnings(Array("org.wartremover.warts.Any"))
      override def map[A, B](fa: ContractLookupRequest[A])(f: A => B) =
        fa.copy(id = fa.id leftMap (_ map f))
    }
  }

  object GetActiveContractsResponse {
    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    def fromLedgerApi(in: lav1.active_contracts_service.GetActiveContractsResponse)
      : Error \/ GetActiveContractsResponse[lav1.value.Value] =
      for {
        activeContracts <- in.activeContracts.toVector traverse (ActiveContract.fromLedgerApi(_))
      } yield
        GetActiveContractsResponse(
          offset = in.offset,
          workflowId = Some(in.workflowId) filter (_.nonEmpty),
          activeContracts = activeContracts)

    implicit val covariant: Functor[GetActiveContractsResponse] =
      new Functor[GetActiveContractsResponse] {
        override def map[A, B](fa: GetActiveContractsResponse[A])(f: A => B) =
          fa copy (activeContracts = fa.activeContracts map (_ map f))
      }
  }

  private[this] implicit final class ErrorOps[A](private val o: Option[A]) extends AnyVal {
    def required(label: String): Error \/ A = o toRightDisjunction s"Missing required field $label"
  }

  final case class CommandMeta(
      workflowId: Option[lar.WorkflowId],
      commandId: Option[lar.CommandId],
      ledgerEffectiveTime: Option[Instant],
      maximumRecordTime: Option[Instant])

  final case class CreateCommand[+LfV](
      templateId: TemplateId.OptionalPkg,
      arguments: Option[Record[LfV]],
      meta: Option[CommandMeta])

  final case class ExerciseCommand[+LfV](
      templateId: TemplateId.OptionalPkg,
      contractId: lar.ContractId,
      choice: lar.Choice,
      arguments: Option[Record[LfV]],
      meta: Option[CommandMeta])

  trait HasTemplateId[F[_]] {
    def templateId(a: F[_]): TemplateId.OptionalPkg
  }

  type Field[+A] = (String, A)

  type Record[+A] = List[Field[A]]

  object Record {
    val traversal: Traverse[Record] =
      Traverse[List].compose[Field]
  }

  object CreateCommand {
    // TODO(Leo) Traverse[CreateCommand] and Traverse[ExerciseCommand] are almost the same, HasArguments typeclass?
    implicit val traverseInstance: Traverse[CreateCommand] = new Traverse[CreateCommand] {
      override def map[A, B](fa: CreateCommand[A])(f: A => B): CreateCommand[B] =
        fa.copy(arguments = fa.arguments.map(as => Record.traversal.map(as)(f)))

      override def traverseImpl[G[_]: Applicative, A, B](fa: CreateCommand[A])(
          f: A => G[B]): G[CreateCommand[B]] = {
        val gb: G[Option[Record[B]]] =
          fa.arguments.traverse(as => Record.traversal.traverse(as)(f))
        gb map (b => fa.copy(arguments = b))
      }
    }

    implicit val hasTemplateId: HasTemplateId[CreateCommand] =
      (a: CreateCommand[_]) => a.templateId
  }

  object ExerciseCommand {
    // TODO(Leo) Traverse[CreateCommand] and Traverse[ExerciseCommand] are almost the same, HasArguments typeclass?
    implicit val traverseInstance: Traverse[ExerciseCommand] = new Traverse[ExerciseCommand] {
      override def map[A, B](fa: ExerciseCommand[A])(f: A => B): ExerciseCommand[B] =
        fa.copy(arguments = fa.arguments.map(as => Record.traversal.map(as)(f)))

      override def traverseImpl[G[_]: Applicative, A, B](fa: ExerciseCommand[A])(
          f: A => G[B]): G[ExerciseCommand[B]] = {
        val gb: G[Option[List[Field[B]]]] =
          fa.arguments.traverse(as => Record.traversal.traverse(as)(f))
        gb map (b => fa.copy(arguments = b))
      }
    }

    implicit val hasTemplateId: HasTemplateId[ExerciseCommand] =
      (a: ExerciseCommand[_]) => a.templateId
  }
}
