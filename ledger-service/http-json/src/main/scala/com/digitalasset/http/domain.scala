// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import java.time.Instant

import com.digitalasset.daml.lf
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.http.util.ClientUtil.boxedRecord
import com.digitalasset.http.util.IdentifierConverters
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.std.list._
import scalaz.std.vector._
import scalaz.std.option._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{-\/, @@, Applicative, Show, Tag, Traverse, \/, \/-}
import spray.json.JsValue

import scala.annotation.tailrec
import scala.language.higherKinds

@SuppressWarnings(Array("org.wartremover.warts.Any"))
object domain {

  case class Error(id: Symbol, message: String)

  object Error {
    implicit val errorShow: Show[Error] = Show shows { e =>
      s"domain.Error, ${e.id: Symbol}: ${e.message: String}"
    }
  }

  case class JwtPayload(ledgerId: lar.LedgerId, applicationId: lar.ApplicationId, party: Party)

  case class TemplateId[+PkgId](packageId: PkgId, moduleName: String, entityName: String)

  case class Contract[+LfV](value: ArchivedContract \/ ActiveContract[LfV])

  type InputContractRef[+LfV] =
    (TemplateId.OptionalPkg, LfV) \/ (Option[TemplateId.OptionalPkg], ContractId)

  case class ActiveContract[+LfV](
      contractId: ContractId,
      templateId: TemplateId.RequiredPkg,
      key: Option[LfV],
      argument: LfV,
      witnessParties: Seq[Party],
      signatories: Seq[Party],
      observers: Seq[Party],
      agreementText: String)

  case class ArchivedContract(
      contractId: ContractId,
      templateId: TemplateId.RequiredPkg,
      witnessParties: Seq[Party])

  sealed trait ContractLocator[+LfV]

  case class EnrichedContractKey[+LfV](
      templateId: TemplateId.OptionalPkg,
      key: LfV
  ) extends ContractLocator[LfV]

  case class EnrichedContractId(
      templateId: Option[TemplateId.OptionalPkg],
      contractId: domain.ContractId
  ) extends ContractLocator[Nothing]

  case class ContractLookupRequest[+LfV](id: ContractLocator[LfV])

  case class GetActiveContractsRequest(
      templateIds: Set[TemplateId.OptionalPkg],
      query: Map[String, JsValue])

  case class PartyDetails(party: Party, displayName: Option[String], isLocal: Boolean)

  final case class CommandMeta(
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

  final case class ExerciseResponse[+LfV](
      exerciseResult: LfV,
      contracts: List[Contract[LfV]]
  )

  object PartyDetails {
    def fromLedgerApi(p: com.digitalasset.ledger.api.domain.PartyDetails): PartyDetails =
      PartyDetails(Party(p.party), p.displayName, p.isLocal)
  }

  sealed trait OffsetTag
  type Offset = String @@ OffsetTag
  object Offset {
    private[http] val tag = Tag.of[OffsetTag]

    def apply(s: String): Offset = tag(s)

    def unwrap(x: Offset): String = tag.unwrap(x)

    def fromLedgerApi(
        gacr: lav1.active_contracts_service.GetActiveContractsResponse): Option[Offset] =
      Option(gacr.offset).filter(_.nonEmpty).map(x => Offset(x))

    def fromLedgerApi(tx: lav1.transaction.Transaction): Offset = Offset(tx.offset)

    def toLedgerApi(o: Offset): lav1.ledger_offset.LedgerOffset =
      lav1.ledger_offset.LedgerOffset(lav1.ledger_offset.LedgerOffset.Value.Absolute(unwrap(o)))
  }

  type Choice = lar.Choice
  val Choice = lar.Choice

  type ContractIdTag = lar.ContractIdTag
  type ContractId = lar.ContractId
  val ContractId = lar.ContractId

  type PartyTag = lar.PartyTag
  type Party = lar.Party
  val Party = lar.Party

  object TemplateId {
    type OptionalPkg = TemplateId[Option[String]]
    type RequiredPkg = TemplateId[String]
    type NoPkg = TemplateId[Unit]

    def fromLedgerApi(in: lav1.value.Identifier): TemplateId.RequiredPkg =
      TemplateId(in.packageId, in.moduleName, in.entityName)
  }

  object Contract {

    def fromTransaction(
        tx: lav1.transaction.Transaction): Error \/ List[Contract[lav1.value.Value]] = {
      tx.events.toList.traverse(fromEvent(_))
    }

    def fromTransactionTree(
        tx: lav1.transaction.TransactionTree): Error \/ Vector[Contract[lav1.value.Value]] = {
      tx.rootEventIds.toVector
        .map(fromTreeEvent(tx.eventsById))
        .sequence
        .map(_.flatten)
    }

    def fromEvent(event: lav1.event.Event): Error \/ Contract[lav1.value.Value] =
      event.event match {
        case lav1.event.Event.Event.Created(created) =>
          ActiveContract.fromLedgerApi(created).map(a => Contract(\/-(a)))
        case lav1.event.Event.Event.Archived(archived) =>
          ArchivedContract.fromLedgerApi(archived).map(a => Contract(-\/(a)))
        case lav1.event.Event.Event.Empty =>
          val errorMsg = s"Expected either Created or Archived event, got: Empty"
          -\/(Error('Contract_fromLedgerApi, errorMsg))
      }

    def fromTreeEvent(eventsById: Map[String, lav1.transaction.TreeEvent])(
        eventId: String): Error \/ Vector[Contract[lav1.value.Value]] = {
      import scalaz.syntax.applicative._

      @tailrec
      def loop(es: Vector[String], acc: Error \/ Vector[Contract[lav1.value.Value]])
        : Error \/ Vector[Contract[lav1.value.Value]] = es match {
        case Vector() =>
          acc
        case head +: tail =>
          eventsById(head).kind match {
            case lav1.transaction.TreeEvent.Kind.Created(created) =>
              val a = ActiveContract.fromLedgerApi(created).map(a => Contract(\/-(a)))
              val newAcc = ^(acc, a)(_ :+ _)
              loop(tail, newAcc)
            case lav1.transaction.TreeEvent.Kind.Exercised(exercised) =>
              val a = ArchivedContract.fromLedgerApi(exercised).map(_.map(a => Contract(-\/(a))))
              val newAcc = ^(acc, a)(_ ++ _.toVector)
              loop(exercised.childEventIds.toVector ++ tail, newAcc)
            case lav1.transaction.TreeEvent.Kind.Empty =>
              val errorMsg = s"Expected either Created or Exercised event, got: Empty"
              -\/(Error('Contract_fromTreeEvent, errorMsg))
          }
      }

      loop(Vector(eventId), \/-(Vector()))
    }

    implicit val covariant: Traverse[Contract] = new Traverse[Contract] {

      override def map[A, B](fa: Contract[A])(f: A => B): Contract[B] = {
        val valueB: ArchivedContract \/ ActiveContract[B] = fa.value.map(a => a.map(f))
        Contract(valueB)
      }

      override def traverseImpl[G[_]: Applicative, A, B](fa: Contract[A])(
          f: A => G[B]): G[Contract[B]] = {
        val valueB: G[ArchivedContract \/ ActiveContract[B]] = fa.value.traverse(a => a.traverse(f))
        valueB.map(x => Contract[B](x))
      }
    }
  }

  object ActiveContract {
    def fromLedgerApi(gacr: lav1.active_contracts_service.GetActiveContractsResponse)
      : Error \/ List[ActiveContract[lav1.value.Value]] = {
      gacr.activeContracts.toList.traverse(fromLedgerApi(_))
    }

    def fromLedgerApi(in: lav1.event.CreatedEvent): Error \/ ActiveContract[lav1.value.Value] =
      for {
        templateId <- in.templateId required "templateId"
        argument <- in.createArguments required "createArguments"
      } yield
        ActiveContract(
          contractId = ContractId(in.contractId),
          templateId = TemplateId fromLedgerApi templateId,
          key = in.contractKey,
          argument = boxedRecord(argument),
          witnessParties = Party.subst(in.witnessParties),
          signatories = Party.subst(in.signatories),
          observers = Party.subst(in.observers),
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
          templateId: TemplateId.RequiredPkg,
          f: PackageService.ResolveChoiceRecordId): Error \/ lf.data.Ref.Identifier =
        \/-(IdentifierConverters.lfIdentifier(templateId))
    }
  }

  object ArchivedContract {
    def fromLedgerApi(in: lav1.event.ArchivedEvent): Error \/ ArchivedContract =
      for {
        templateId <- in.templateId required "templateId"
      } yield
        ArchivedContract(
          contractId = ContractId(in.contractId),
          templateId = TemplateId fromLedgerApi templateId,
          witnessParties = Party.subst(in.witnessParties)
        )

    def fromLedgerApi(in: lav1.event.ExercisedEvent): Error \/ Option[ArchivedContract] =
      if (in.consuming) {
        for {
          templateId <- in.templateId.required("templateId")
        } yield
          Some(
            ArchivedContract(
              contractId = ContractId(in.contractId),
              templateId = TemplateId.fromLedgerApi(templateId),
              witnessParties = Party.subst(in.witnessParties)
            ))
      } else {
        \/-(None)
      }
  }

  object ContractLookupRequest {
    implicit val covariant: Traverse[ContractLookupRequest] = new Traverse[ContractLookupRequest] {
      override def traverseImpl[G[_]: Applicative, A, B](fa: ContractLookupRequest[A])(
          f: A => G[B]): G[ContractLookupRequest[B]] = {
        val G: Applicative[G] = implicitly
        fa.id match {
          case ka @ EnrichedContractKey(_, _) => ka.traverse(f).map(ContractLookupRequest.apply)
          case c: EnrichedContractId => G.point(ContractLookupRequest(c))
        }
      }
    }

    implicit val hasTemplateId: HasTemplateId[ContractLookupRequest] =
      new HasTemplateId[ContractLookupRequest] {
        override def templateId(fa: ContractLookupRequest[_]): TemplateId.OptionalPkg =
          fa.id match {
            case EnrichedContractKey(templateId, _) => templateId
            case EnrichedContractId(Some(templateId), _) => templateId
            case EnrichedContractId(None, _) => TemplateId(None, "", "")
          }

        override def lfIdentifier(
            fa: ContractLookupRequest[_],
            templateId: TemplateId.RequiredPkg,
            f: PackageService.ResolveChoiceRecordId): Error \/ Ref.Identifier =
          \/-(IdentifierConverters.lfIdentifier(templateId))
      }
  }

  object EnrichedContractKey {
    implicit var covariant: Traverse[EnrichedContractKey] = new Traverse[EnrichedContractKey] {

      override def map[A, B](fa: EnrichedContractKey[A])(f: A => B): EnrichedContractKey[B] =
        EnrichedContractKey(fa.templateId, f(fa.key))

      override def traverseImpl[G[_]: Applicative, A, B](fa: EnrichedContractKey[A])(
          f: A => G[B]): G[EnrichedContractKey[B]] =
        f(fa.key).map(b => EnrichedContractKey(fa.templateId, b))
    }
  }

  private[this] implicit final class ErrorOps[A](private val o: Option[A]) extends AnyVal {
    def required(label: String): Error \/ A =
      o toRightDisjunction Error('ErrorOps_required, s"Missing required field $label")
  }

  trait HasTemplateId[F[_]] {
    def templateId(fa: F[_]): TemplateId.OptionalPkg
    def lfIdentifier(
        fa: F[_],
        templateId: TemplateId.RequiredPkg,
        f: PackageService.ResolveChoiceRecordId): Error \/ lf.data.Ref.Identifier
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
          templateId: TemplateId.RequiredPkg,
          f: PackageService.ResolveChoiceRecordId): Error \/ lf.data.Ref.Identifier =
        \/-(IdentifierConverters.lfIdentifier(templateId))
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
            templateId: TemplateId.RequiredPkg,
            f: PackageService.ResolveChoiceRecordId): Error \/ lf.data.Ref.Identifier =
          for {
            apiId <- f(templateId, fa.choice)
              .leftMap(e => Error('ExerciseCommand_hasTemplateId_lfIdentifier, e.shows))
          } yield IdentifierConverters.lfIdentifier(apiId)
      }
  }

  object ExerciseResponse {
    implicit val traverseInstance: Traverse[ExerciseResponse] = new Traverse[ExerciseResponse] {
      override def traverseImpl[G[_]: Applicative, A, B](fa: ExerciseResponse[A])(
          f: A => G[B]): G[ExerciseResponse[B]] = {
        import scalaz.syntax.applicative._
        val gb: G[B] = f(fa.exerciseResult)
        val gbs: G[List[Contract[B]]] = fa.contracts.traverse(_.traverse(f))
        ^(gb, gbs) { (exerciseResult, contracts) =>
          ExerciseResponse(
            exerciseResult = exerciseResult,
            contracts = contracts
          )
        }
      }
    }
  }
}
