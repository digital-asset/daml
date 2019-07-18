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

@SuppressWarnings(Array("org.wartremover.warts.Any"))
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

  private trait HasArguments[F[_]] {
    def arguments[LfV](a: F[LfV]): Option[Record[LfV]]
    def withArguments[LfV](a: F[_], newArguments: Option[Record[LfV]]): F[LfV]
  }

  private def traverseArgumentsInstance[F[_]](argumentsLens: HasArguments[F]): Traverse[F] =
    new Traverse[F] {
      import argumentsLens._
      override def map[A, B](fa: F[A])(f: A => B): F[B] =
        withArguments(fa, arguments(fa) map (Record.traversal.map(_)(f)))
      override def traverseImpl[G[_]: Applicative, A, B](fa: F[A])(f: A => G[B]): G[F[B]] =
        arguments(fa) traverse (Record.traversal.traverse(_)(f)) map (withArguments(fa, _))
    }

  type Field[+A] = (String, A)

  type Record[+A] = List[Field[A]]

  object Record {
    val traversal: Traverse[Record] =
      Traverse[List].compose[Field]
  }

  object CreateCommand {
    implicit val traverseInstance: Traverse[CreateCommand] =
      traverseArgumentsInstance {
        new HasArguments[CreateCommand] {
          override def arguments[LfV](a: CreateCommand[LfV]) = a.arguments
          override def withArguments[LfV](a: CreateCommand[_], newArguments: Option[Record[LfV]]) =
            a.copy(arguments = newArguments)
        }
      }

    implicit val hasTemplateId: HasTemplateId[CreateCommand] =
      (a: CreateCommand[_]) => a.templateId
  }

  object ExerciseCommand {
    implicit val traverseInstance: Traverse[ExerciseCommand] =
      traverseArgumentsInstance {
        new HasArguments[ExerciseCommand] {
          override def arguments[LfV](a: ExerciseCommand[LfV]) = a.arguments
          override def withArguments[LfV](
              a: ExerciseCommand[_],
              newArguments: Option[Record[LfV]]) = a.copy(arguments = newArguments)
        }
      }

    implicit val hasTemplateId: HasTemplateId[ExerciseCommand] =
      (a: ExerciseCommand[_]) => a.templateId
  }
}
