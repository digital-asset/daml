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
import scalaz.syntax.apply._
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
      override def map[A, B](fa: ContractLookupRequest[A])(f: A => B) =
        fa.copy(id = fa.id leftMap (_ map f))
    }
  }

  object GetActiveContractsResponse {
    def fromLedgerApi(in: lav1.active_contracts_service.GetActiveContractsResponse)
      : Error \/ GetActiveContractsResponse[lav1.value.Value] =
      for {
        activeContracts <- in.activeContracts.toVector traverseU (ActiveContract.fromLedgerApi(_))
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
      arguments: Option[Seq[Field[LfV]]],
      meta: Option[CommandMeta])

  final case class ExerciseCommand[+LfV](
      templateId: TemplateId.OptionalPkg,
      contractId: lar.ContractId,
      choice: lar.Choice,
      arguments: Option[Seq[Field[LfV]]],
      meta: Option[CommandMeta])

  // TODO(Leo): do you still need it?
  trait HasTemplateId[A] {
    def templateId(a: A): TemplateId.OptionalPkg
  }

  type Field[A] = (String, A)

  object Field {
    implicit val traverseField: Traverse[Field] = new Traverse[Field] {
      override def map[A, B](fa: Field[A])(f: A => B): Field[B] = (fa._1, f(fa._2))

      override def traverseImpl[G[_]: Applicative, A, B](fa: Field[A])(
          f: A => G[B]): G[Field[B]] = {
        import scalaz.syntax.apply._
        val g: Applicative[G] = implicitly
        val gs: G[String] = g.pure[String](fa._1)
        val gb: G[B] = f(fa._2)
        ^(gs, gb)((s, b) => (s, b))
      }
    }

    def mapFields[A, B](args: List[Field[A]])(f: A => B): List[Field[B]] =
      args.map(a => traverseField.map(a)(f))

    def traverseFields[G[_]: Applicative, A, B](as: Seq[(String, A)])(
        f: A => G[B]): G[List[(String, B)]] =
      as.toList.traverse(a => traverseField.traverse(a)(f))
  }

  object CreateCommand {
    // TODO(Leo) Traverse[CreateCommand] and Traverse[ExerciseCommand] are almost the same, HasArguments typeclass?
    implicit val traverse = new Traverse[CreateCommand] {
      override def map[A, B](fa: CreateCommand[A])(f: A => B): CreateCommand[B] =
        fa.copy(arguments = fa.arguments.map(as => Field.mapFields(as.toList)(f)))

      override def traverseImpl[G[_]: Applicative, A, B](fa: CreateCommand[A])(
          f: A => G[B]): G[CreateCommand[B]] = {

        val g: Applicative[G] = implicitly
        val ga: G[CreateCommand[A]] = g.pure(fa)
        val gb: G[Option[List[(String, B)]]] =
          fa.arguments.traverse(as => Field.traverseFields(as)(f))
        ^(ga, gb)((a, b) => a.copy(arguments = b))
      }
    }

    implicit val hasTemplateId: HasTemplateId[CreateCommand[_]] =
      (a: CreateCommand[_]) => a.templateId
  }

  object ExerciseCommand {
    // TODO(Leo) Traverse[CreateCommand] and Traverse[ExerciseCommand] are almost the same, HasArguments typeclass?
    implicit val traverse = new Traverse[ExerciseCommand] {
      override def map[A, B](fa: ExerciseCommand[A])(f: A => B): ExerciseCommand[B] =
        fa.copy(arguments = fa.arguments.map(as => Field.mapFields(as.toList)(f)))

      override def traverseImpl[G[_]: Applicative, A, B](fa: ExerciseCommand[A])(
          f: A => G[B]): G[ExerciseCommand[B]] = {

        val g: Applicative[G] = implicitly
        val ga: G[ExerciseCommand[A]] = g.pure(fa)
        val gb: G[Option[List[(String, B)]]] =
          fa.arguments.traverse(as => Field.traverseFields(as)(f))
        ^(ga, gb)((a, b) => a.copy(arguments = b))
      }
    }

    implicit val hasTemplateId: HasTemplateId[ExerciseCommand[_]] =
      (a: ExerciseCommand[_]) => a.templateId
  }
}
