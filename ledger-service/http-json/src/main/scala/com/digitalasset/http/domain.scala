// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import java.time.Instant

import com.digitalasset.daml.lf
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.http.util.IdentifierConverters
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.std.vector._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{-\/, Applicative, Traverse, \/, \/-}

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

    implicit val covariant: Traverse[ActiveContract] = new Traverse[ActiveContract] {

      override def map[A, B](fa: ActiveContract[A])(f: A => B): ActiveContract[B] =
        fa.copy(key = fa.key map f, argument = f(fa.argument))

      override def traverseImpl[G[_]: Applicative, A, B](fa: ActiveContract[A])(
          f: A => G[B]): G[ActiveContract[B]] = {
        import scalaz.syntax.apply._
        val gk: G[Option[B]] = fa.key traverse f
        val ga: G[B] = f(fa.argument)
        ^(gk, ga)((k, a) => fa.copy(key = k, argument = a))
      }
    }

    implicit val hasTemplateId: HasTemplateId[ActiveContract] = new HasTemplateId[ActiveContract] {
      override def templateId(fa: ActiveContract[_]): TemplateId.OptionalPkg =
        TemplateId(
          Some(fa.templateId.packageId),
          fa.templateId.moduleName,
          fa.templateId.entityName)

      override def lfIdentifier(
          fa: ActiveContract[_],
          templateId: TemplateId.RequiredPkg): lf.data.Ref.Identifier =
        IdentifierConverters.lfIdentifier(templateId)
    }
  }

  object ContractLookupRequest {
    implicit val covariant: Traverse[ContractLookupRequest] = new Traverse[ContractLookupRequest] {
      override def map[A, B](fa: ContractLookupRequest[A])(f: A => B) =
        fa.copy(id = fa.id leftMap (_ map f))

      override def traverseImpl[G[_]: Applicative, A, B](fa: ContractLookupRequest[A])(
          f: A => G[B]): G[ContractLookupRequest[B]] = {
        val G: Applicative[G] = implicitly
        fa.id match {
          case -\/(a) =>
            a.traverse(f).map(b => fa.copy(id = -\/(b)))
          case \/-(a) =>
            // TODO: we don't actually need to copy it, just need to adjust the type for the left side
            G.point(fa.copy(id = \/-(a)))
        }
      }
    }

    implicit val hasTemplateId: HasTemplateId[ContractLookupRequest] =
      new HasTemplateId[ContractLookupRequest] {
        override def templateId(fa: ContractLookupRequest[_]): TemplateId.OptionalPkg =
          fa.id match {
            case -\/((a, _)) => a
            case \/-((Some(a), _)) => a
            case \/-((None, _)) => TemplateId(None, "", "")
          }

        override def lfIdentifier(
            fa: ContractLookupRequest[_],
            templateId: TemplateId.RequiredPkg): Ref.Identifier =
          IdentifierConverters.lfIdentifier(templateId)
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

    implicit val covariant: Traverse[GetActiveContractsResponse] =
      new Traverse[GetActiveContractsResponse] {
        override def traverseImpl[G[_]: Applicative, A, B](fa: GetActiveContractsResponse[A])(
            f: A => G[B]): G[GetActiveContractsResponse[B]] = {

          val gas: G[List[ActiveContract[B]]] =
            fa.activeContracts.toList.traverse(a => a.traverse(f))
          gas.map(as => fa.copy(activeContracts = as))
        }
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
      argument: LfV,
      meta: Option[CommandMeta])

  final case class ExerciseCommand[+LfV](
      templateId: TemplateId.OptionalPkg,
      contractId: lar.ContractId,
      choice: lar.Choice,
      argument: LfV,
      meta: Option[CommandMeta])

  trait HasTemplateId[F[_]] {
    def templateId(fa: F[_]): TemplateId.OptionalPkg
    def lfIdentifier(fa: F[_], templateId: TemplateId.RequiredPkg): lf.data.Ref.Identifier
  }

  object CreateCommand {
    implicit val traverseInstance: Traverse[CreateCommand] = new Traverse[CreateCommand] {
      override def traverseImpl[G[_]: Applicative, A, B](fa: CreateCommand[A])(
          f: A => G[B]): G[CreateCommand[B]] =
        f(fa.argument).map(a => fa.copy(argument = a))
    }

    implicit val hasTemplateId: HasTemplateId[CreateCommand] = new HasTemplateId[CreateCommand] {
      override def templateId(fa: CreateCommand[_]): TemplateId.OptionalPkg = fa.templateId

      override def lfIdentifier(
          fa: CreateCommand[_],
          templateId: TemplateId.RequiredPkg): lf.data.Ref.Identifier =
        IdentifierConverters.lfIdentifier(templateId)
    }
  }

  object ExerciseCommand {
    implicit val traverseInstance: Traverse[ExerciseCommand] = new Traverse[ExerciseCommand] {
      override def traverseImpl[G[_]: Applicative, A, B](fa: ExerciseCommand[A])(
          f: A => G[B]): G[ExerciseCommand[B]] = f(fa.argument).map(a => fa.copy(argument = a))
    }

    implicit val hasTemplateId: HasTemplateId[ExerciseCommand] =
      new HasTemplateId[ExerciseCommand] {
        override def templateId(fa: ExerciseCommand[_]): TemplateId.OptionalPkg = fa.templateId

        override def lfIdentifier(
            fa: ExerciseCommand[_],
            templateId: TemplateId.RequiredPkg): lf.data.Ref.Identifier =
          IdentifierConverters.lfIdentifier(templateId, fa.choice)
      }
  }
}
