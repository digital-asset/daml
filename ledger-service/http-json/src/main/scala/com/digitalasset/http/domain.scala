// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import java.time.Instant

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import com.digitalasset.daml.lf
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.iface
import com.digitalasset.http.util.ClientUtil.boxedRecord
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.Isomorphism.{<~>, IsoFunctorTemplate}
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.vector._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{-\/, @@, Applicative, Bitraverse, NonEmptyList, Show, Tag, Traverse, \/, \/-}
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

  type LfValue = lf.value.Value[lf.value.Value.AbsoluteContractId]

  case class JwtPayload(ledgerId: lar.LedgerId, applicationId: lar.ApplicationId, party: Party)

  case class TemplateId[+PkgId](packageId: PkgId, moduleName: String, entityName: String)

  case class Contract[+LfV](value: ArchivedContract \/ ActiveContract[LfV])

  type InputContractRef[+LfV] =
    (TemplateId.OptionalPkg, LfV) \/ (Option[TemplateId.OptionalPkg], ContractId)

  type ResolvedContractRef[+LfV] =
    (TemplateId.RequiredPkg, LfV) \/ (TemplateId.RequiredPkg, ContractId)

  case class ActiveContract[+LfV](
      contractId: ContractId,
      templateId: TemplateId.RequiredPkg,
      key: Option[LfV],
      payload: LfV,
      signatories: Seq[Party],
      observers: Seq[Party],
      agreementText: String,
  )

  case class ArchivedContract(contractId: ContractId, templateId: TemplateId.RequiredPkg)

  sealed abstract class ContractLocator[+LfV] extends Product with Serializable

  final case class EnrichedContractKey[+LfV](
      templateId: TemplateId.OptionalPkg,
      key: LfV,
  ) extends ContractLocator[LfV]

  final case class EnrichedContractId(
      templateId: Option[TemplateId.OptionalPkg],
      contractId: domain.ContractId,
  ) extends ContractLocator[Nothing]

  case class GetActiveContractsRequest(
      templateIds: Set[TemplateId.OptionalPkg],
      query: Map[String, JsValue],
  )

  final case class SearchForeverRequest(
      queries: NonEmptyList[GetActiveContractsRequest]
  )

  case class PartyDetails(party: Party, displayName: Option[String], isLocal: Boolean)

  final case class CommandMeta(
      commandId: Option[CommandId],
      ledgerEffectiveTime: Option[Instant],
      maximumRecordTime: Option[Instant],
  )

  final case class CreateCommand[+LfV](
      templateId: TemplateId.OptionalPkg,
      payload: LfV,
      meta: Option[CommandMeta],
  )

  final case class ExerciseCommand[+LfV, +Ref](
      reference: Ref,
      choice: domain.Choice,
      argument: LfV,
      meta: Option[CommandMeta],
  )

  final case class ExerciseResponse[+LfV](
      exerciseResult: LfV,
      contracts: List[Contract[LfV]],
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
        gacr: lav1.active_contracts_service.GetActiveContractsResponse,
    ): Option[Offset] =
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

  type CommandIdTag = lar.CommandIdTag
  type CommandId = lar.CommandId
  val CommandId = lar.CommandId

  type PartyTag = lar.PartyTag
  type Party = lar.Party
  val Party = lar.Party

  object TemplateId {
    type OptionalPkg = TemplateId[Option[String]]
    type RequiredPkg = TemplateId[String]
    type NoPkg = TemplateId[Unit]

    def fromLedgerApi(in: lav1.value.Identifier): TemplateId.RequiredPkg =
      TemplateId(in.packageId, in.moduleName, in.entityName)

    def qualifiedName(a: TemplateId[_]): Ref.QualifiedName =
      Ref.QualifiedName(
        Ref.DottedName.assertFromString(a.moduleName),
        Ref.DottedName.assertFromString(a.entityName),
      )
  }

  object Contract {

    def fromTransaction(
        tx: lav1.transaction.Transaction,
    ): Error \/ List[Contract[lav1.value.Value]] = {
      tx.events.toList.traverse(fromEvent(_))
    }

    def fromTransactionTree(
        tx: lav1.transaction.TransactionTree,
    ): Error \/ Vector[Contract[lav1.value.Value]] = {
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

    def fromTreeEvent(
        eventsById: Map[String, lav1.transaction.TreeEvent],
    )(eventId: String): Error \/ Vector[Contract[lav1.value.Value]] = {
      import scalaz.syntax.applicative._

      @tailrec
      def loop(
          es: Vector[String],
          acc: Error \/ Vector[Contract[lav1.value.Value]],
      ): Error \/ Vector[Contract[lav1.value.Value]] = es match {
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

      override def traverseImpl[G[_]: Applicative, A, B](
          fa: Contract[A],
      )(f: A => G[B]): G[Contract[B]] = {
        val valueB: G[ArchivedContract \/ ActiveContract[B]] = fa.value.traverse(a => a.traverse(f))
        valueB.map(x => Contract[B](x))
      }
    }
  }

  object ActiveContract {

    def matchesKey(k: LfValue)(a: domain.ActiveContract[LfValue]): Boolean =
      a.key.fold(false)(_ == k)

    def fromLedgerApi(
        gacr: lav1.active_contracts_service.GetActiveContractsResponse,
    ): Error \/ List[ActiveContract[lav1.value.Value]] = {
      gacr.activeContracts.toList.traverse(fromLedgerApi(_))
    }

    def fromLedgerApi(in: lav1.event.CreatedEvent): Error \/ ActiveContract[lav1.value.Value] =
      for {
        templateId <- in.templateId required "templateId"
        payload <- in.createArguments required "createArguments"
      } yield
        ActiveContract(
          contractId = ContractId(in.contractId),
          templateId = TemplateId fromLedgerApi templateId,
          key = in.contractKey,
          payload = boxedRecord(payload),
          signatories = Party.subst(in.signatories),
          observers = Party.subst(in.observers),
          agreementText = in.agreementText getOrElse "",
        )

    implicit val covariant: Traverse[ActiveContract] = new Traverse[ActiveContract] {

      override def map[A, B](fa: ActiveContract[A])(f: A => B): ActiveContract[B] =
        fa.copy(key = fa.key map f, payload = f(fa.payload))

      override def traverseImpl[G[_]: Applicative, A, B](
          fa: ActiveContract[A],
      )(f: A => G[B]): G[ActiveContract[B]] = {
        import scalaz.syntax.apply._
        val gk: G[Option[B]] = fa.key traverse f
        val ga: G[B] = f(fa.payload)
        ^(gk, ga)((k, a) => fa.copy(key = k, payload = a))
      }
    }

    implicit val hasTemplateId: HasTemplateId[ActiveContract] = new HasTemplateId[ActiveContract] {
      override def templateId(fa: ActiveContract[_]): TemplateId.OptionalPkg =
        TemplateId(
          Some(fa.templateId.packageId),
          fa.templateId.moduleName,
          fa.templateId.entityName,
        )

      override def lfType(
          fa: ActiveContract[_],
          templateId: TemplateId.RequiredPkg,
          f: PackageService.ResolveTemplateRecordType,
          g: PackageService.ResolveChoiceRecordType,
          h: PackageService.ResolveKeyType,
      ): Error \/ LfType =
        f(templateId)
          .leftMap(e => Error('ActiveContract_hasTemplateId_lfType, e.shows))
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
            ),
          )
      } else {
        \/-(None)
      }
  }

  object ContractLocator {
    implicit val covariant: Traverse[ContractLocator] = new Traverse[ContractLocator] {

      override def map[A, B](fa: ContractLocator[A])(f: A => B): ContractLocator[B] = fa match {
        case ka: EnrichedContractKey[A] => EnrichedContractKey(ka.templateId, f(ka.key))
        case c: EnrichedContractId => c
      }

      override def traverseImpl[G[_]: Applicative, A, B](
          fa: ContractLocator[A],
      )(f: A => G[B]): G[ContractLocator[B]] = {
        fa match {
          case ka: EnrichedContractKey[A] =>
            f(ka.key).map(b => EnrichedContractKey(ka.templateId, b))
          case c: EnrichedContractId =>
            val G: Applicative[G] = implicitly
            G.point(c)
        }
      }
    }

    val structure: ContractLocator <~> InputContractRef =
      new IsoFunctorTemplate[ContractLocator, InputContractRef] {
        override def from[A](ga: InputContractRef[A]) =
          ga.fold((EnrichedContractKey[A] _).tupled, EnrichedContractId.tupled)

        override def to[A](fa: ContractLocator[A]) = fa match {
          case EnrichedContractId(otid, cid) => \/-((otid, cid))
          case EnrichedContractKey(tid, key) => -\/((tid, key))
        }
      }
  }

  object EnrichedContractKey {
    implicit val covariant: Traverse[EnrichedContractKey] = new Traverse[EnrichedContractKey] {
      override def traverseImpl[G[_]: Applicative, A, B](
          fa: EnrichedContractKey[A],
      )(f: A => G[B]): G[EnrichedContractKey[B]] = {
        f(fa.key).map(b => EnrichedContractKey(fa.templateId, b))
      }
    }

    implicit val hasTemplateId: HasTemplateId[EnrichedContractKey] =
      new HasTemplateId[EnrichedContractKey] {

        override def templateId(fa: EnrichedContractKey[_]): TemplateId.OptionalPkg = fa.templateId

        override def lfType(
            fa: EnrichedContractKey[_],
            templateId: TemplateId.RequiredPkg,
            f: PackageService.ResolveTemplateRecordType,
            g: PackageService.ResolveChoiceRecordType,
            h: PackageService.ResolveKeyType,
        ): Error \/ LfType =
          h(templateId)
            .leftMap(e => Error('EnrichedContractKey_hasTemplateId_lfType, e.shows))
      }
  }

  private[this] implicit final class ErrorOps[A](private val o: Option[A]) extends AnyVal {
    def required(label: String): Error \/ A =
      o toRightDisjunction Error('ErrorOps_required, s"Missing required field $label")
  }

  type LfType = iface.Type

  trait HasTemplateId[F[_]] {
    def templateId(fa: F[_]): TemplateId.OptionalPkg

    def lfType(
        fa: F[_],
        templateId: TemplateId.RequiredPkg,
        f: PackageService.ResolveTemplateRecordType,
        g: PackageService.ResolveChoiceRecordType,
        h: PackageService.ResolveKeyType,
    ): Error \/ LfType
  }

  object CreateCommand {
    implicit val traverseInstance: Traverse[CreateCommand] = new Traverse[CreateCommand] {
      override def traverseImpl[G[_]: Applicative, A, B](
          fa: CreateCommand[A],
      )(f: A => G[B]): G[CreateCommand[B]] =
        f(fa.payload).map(a => fa.copy(payload = a))
    }

    implicit val hasTemplateId: HasTemplateId[CreateCommand] = new HasTemplateId[CreateCommand] {
      override def templateId(fa: CreateCommand[_]): TemplateId.OptionalPkg = fa.templateId

      override def lfType(
          fa: CreateCommand[_],
          templateId: TemplateId.RequiredPkg,
          f: PackageService.ResolveTemplateRecordType,
          g: PackageService.ResolveChoiceRecordType,
          h: PackageService.ResolveKeyType,
      ): Error \/ LfType =
        f(templateId)
          .leftMap(e => Error('CreateCommand_hasTemplateId_lfType, e.shows))
    }
  }

  object ExerciseCommand {
    implicit val bitraverseInstance: Bitraverse[ExerciseCommand] = new Bitraverse[ExerciseCommand] {
      override def bitraverseImpl[G[_]: Applicative, A, B, C, D](
          fab: ExerciseCommand[A, B],
      )(f: A => G[C], g: B => G[D]): G[ExerciseCommand[C, D]] = {
        import scalaz.syntax.applicative._
        ^(f(fab.argument), g(fab.reference))((c, d) => fab.copy(argument = c, reference = d))
      }
    }

    implicit val leftTraverseInstance: Traverse[ExerciseCommand[+?, Nothing]] =
      bitraverseInstance.leftTraverse

    implicit val hasTemplateId =
      new HasTemplateId[ExerciseCommand[+?, domain.ContractLocator[_]]] {

        override def templateId(
            fab: ExerciseCommand[_, domain.ContractLocator[_]],
        ): TemplateId.OptionalPkg = {
          fab.reference match {
            case EnrichedContractKey(templateId, _) => templateId
            case EnrichedContractId(Some(templateId), _) => templateId
            case EnrichedContractId(None, _) =>
              throw new IllegalArgumentException(
                "Please specify templateId, optional templateId is not supported yet!",
              )
          }
        }

        override def lfType(
            fa: ExerciseCommand[_, domain.ContractLocator[_]],
            templateId: TemplateId.RequiredPkg,
            f: PackageService.ResolveTemplateRecordType,
            g: PackageService.ResolveChoiceRecordType,
            h: PackageService.ResolveKeyType,
        ): Error \/ LfType =
          g(templateId, fa.choice)
            .leftMap(e => Error('ExerciseCommand_hasTemplateId_lfType, e.shows))
      }
  }

  object ExerciseResponse {
    implicit val traverseInstance: Traverse[ExerciseResponse] = new Traverse[ExerciseResponse] {
      override def traverseImpl[G[_]: Applicative, A, B](
          fa: ExerciseResponse[A],
      )(f: A => G[B]): G[ExerciseResponse[B]] = {
        import scalaz.syntax.applicative._
        val gb: G[B] = f(fa.exerciseResult)
        val gbs: G[List[Contract[B]]] = fa.contracts.traverse(_.traverse(f))
        ^(gb, gbs) { (exerciseResult, contracts) =>
          ExerciseResponse(
            exerciseResult = exerciseResult,
            contracts = contracts,
          )
        }
      }
    }
  }

  sealed abstract class ServiceResponse extends Product with Serializable

  final case class OkResponse[R, W](
      result: R,
      warnings: Option[W],
      status: StatusCode = StatusCodes.OK,
  ) extends ServiceResponse

  final case class ErrorResponse[E](errors: E, status: StatusCode) extends ServiceResponse

  object OkResponse {
    implicit val covariant: Bitraverse[OkResponse] = new Bitraverse[OkResponse] {
      override def bitraverseImpl[G[_]: Applicative, A, B, C, D](
          fab: OkResponse[A, B],
      )(f: A => G[C], g: B => G[D]): G[OkResponse[C, D]] = {
        import scalaz.syntax.applicative._
        ^(f(fab.result), fab.warnings.traverse(g))((c, d) => fab.copy(result = c, warnings = d))
      }
    }
  }

  object ErrorResponse {
    implicit val traverseInstance: Traverse[ErrorResponse] = new Traverse[ErrorResponse] {
      override def traverseImpl[G[_]: Applicative, A, B](fa: ErrorResponse[A])(
          f: A => G[B],
      ): G[ErrorResponse[B]] = f(fa.errors).map(b => fa.copy(errors = b))
    }
  }

  sealed abstract class ServiceWarning extends Serializable with Product

  final case class UnknownTemplateIds(unknownTemplateIds: List[TemplateId.OptionalPkg])
      extends ServiceWarning
}
