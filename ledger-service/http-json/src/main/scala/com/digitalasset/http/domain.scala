// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import com.daml.lf.iface
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.api.{v1 => lav1}
import com.daml.scalautil.nonempty.NonEmpty
import com.daml.scalautil.nonempty.NonEmptyReturningOps._
import scalaz.Isomorphism.{<~>, IsoFunctorTemplate}
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.vector._
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.{-\/, Applicative, Bitraverse, Functor, NonEmptyList, OneAnd, Traverse, \/, \/-}
import spray.json.JsValue

import scala.annotation.tailrec

object domain extends com.daml.fetchcontracts.domain.Aliases {

  import com.daml.fetchcontracts.domain.`fc domain ErrorOps`

  trait UserToken {
    val username: String
  }

  trait JwtPayloadTag

  trait JwtPayloadG {
    val ledgerId: LedgerId
    val applicationId: ApplicationId
    val readAs: List[Party]
    val actAs: List[Party]
    val parties: PartySet
  }

  // write endpoints require at least one party in actAs
  // (only the first one is used for pre-multiparty ledgers)
  // but we can have multiple parties in readAs.
  final case class JwtWritePayload(
      ledgerId: LedgerId,
      applicationId: ApplicationId,
      submitter: NonEmptyList[Party],
      readAs: List[Party],
  ) extends JwtPayloadG {
    override val actAs: List[Party] = submitter.toList
    override val parties: PartySet =
      submitter.toSet1 ++ readAs
  }

  final case class JwtPayloadLedgerIdOnly(ledgerId: LedgerId)

  // As with JwtWritePayload, but supports empty `actAs`.  At least one of
  // `actAs` or `readAs` must be non-empty.
  sealed abstract case class JwtPayload private (
      ledgerId: LedgerId,
      applicationId: ApplicationId,
      readAs: List[Party],
      actAs: List[Party],
      parties: PartySet,
  ) extends JwtPayloadG {}

  object JwtPayload {
    def apply(
        ledgerId: LedgerId,
        applicationId: ApplicationId,
        readAs: List[Party],
        actAs: List[Party],
    ): Option[JwtPayload] = {
      (readAs ++ actAs) match {
        case NonEmpty(ps) =>
          Some(
            new JwtPayload(ledgerId, applicationId, readAs, actAs, ps.toSet) {}
          )
        case _ => None
      }
    }
  }

  case class Contract[LfV](value: ArchivedContract \/ ActiveContract[LfV])

  type InputContractRef[LfV] =
    (TemplateId.OptionalPkg, LfV) \/ (Option[TemplateId.OptionalPkg], ContractId)

  type ResolvedContractRef[LfV] =
    (TemplateId.RequiredPkg, LfV) \/ (TemplateId.RequiredPkg, ContractId)

  case class ArchivedContract(contractId: ContractId, templateId: TemplateId.RequiredPkg)

  final case class FetchRequest[+LfV](
      locator: ContractLocator[LfV],
      readAs: Option[NonEmptyList[Party]],
  ) {
    private[http] def traverseLocator[F[_]: Functor, OV](
        f: ContractLocator[LfV] => F[ContractLocator[OV]]
    ): F[FetchRequest[OV]] = f(locator) map (l => copy(locator = l))
  }

  sealed abstract class ContractLocator[+LfV] extends Product with Serializable

  final case class EnrichedContractKey[+LfV](
      templateId: TemplateId.OptionalPkg,
      key: LfV,
  ) extends ContractLocator[LfV]

  final case class EnrichedContractId(
      templateId: Option[TemplateId.OptionalPkg],
      contractId: domain.ContractId,
  ) extends ContractLocator[Nothing]

  final case class ContractKeyStreamRequest[+Cid, +LfV](
      contractIdAtOffset: Cid,
      ekey: EnrichedContractKey[LfV],
  )

  final case class GetActiveContractsRequest(
      templateIds: OneAnd[Set, TemplateId.OptionalPkg],
      query: Map[String, JsValue],
      readAs: Option[NonEmptyList[Party]],
  )

  final case class SearchForeverQuery(
      templateIds: OneAnd[Set, TemplateId.OptionalPkg],
      query: Map[String, JsValue],
      offset: Option[domain.Offset],
  )

  final case class SearchForeverRequest(
      queriesWithPos: NonEmptyList[(SearchForeverQuery, Int)]
  )

  final case class PartyDetails(identifier: Party, displayName: Option[String], isLocal: Boolean)

  final case class AllocatePartyRequest(identifierHint: Option[Party], displayName: Option[String])

  final case class CommandMeta(
      commandId: Option[CommandId],
      actAs: Option[NonEmptyList[Party]],
      readAs: Option[List[Party]],
  )

  final case class CreateCommand[+LfV, TmplId](
      templateId: TmplId,
      payload: LfV,
      meta: Option[CommandMeta],
  ) {
    def traversePayload[G[_]: Applicative, LfVB](
        f: LfV => G[LfVB]
    ): G[CreateCommand[LfVB, TmplId]] =
      Bitraverse[CreateCommand].leftTraverse.traverse(this)(f)
  }

  final case class ExerciseCommand[+LfV, +Ref](
      reference: Ref,
      choice: domain.Choice,
      argument: LfV,
      meta: Option[CommandMeta],
  )

  final case class CreateAndExerciseCommand[+Payload, +Arg, +TmplId](
      templateId: TmplId,
      payload: Payload,
      choice: domain.Choice,
      argument: Arg,
      meta: Option[CommandMeta],
  )

  final case class ExerciseResponse[LfV](
      exerciseResult: LfV,
      events: List[Contract[LfV]],
  )

  object PartyDetails {
    def fromLedgerApi(p: com.daml.ledger.api.domain.PartyDetails): PartyDetails =
      PartyDetails(Party(p.party), p.displayName, p.isLocal)
  }

  final case class StartingOffset(offset: Offset)

  type LedgerIdTag = lar.LedgerIdTag
  type LedgerId = lar.LedgerId
  val LedgerId = lar.LedgerId

  type ApplicationIdTag = lar.ApplicationIdTag
  type ApplicationId = lar.ApplicationId
  val ApplicationId = lar.ApplicationId

  type Choice = lar.Choice
  val Choice = lar.Choice

  type CommandIdTag = lar.CommandIdTag
  type CommandId = lar.CommandId
  val CommandId = lar.CommandId

  object Contract {

    def fromTransaction(
        tx: lav1.transaction.Transaction
    ): Error \/ List[Contract[lav1.value.Value]] = {
      tx.events.toList.traverse(fromEvent(_))
    }

    def fromTransactionTree(
        tx: lav1.transaction.TransactionTree
    ): Error \/ Vector[Contract[lav1.value.Value]] = {
      tx.rootEventIds.toVector
        .map(fromTreeEvent(tx.eventsById))
        .sequence
        .map(_.flatten)
    }

    def fromEvent(event: lav1.event.Event): Error \/ Contract[lav1.value.Value] =
      event.event match {
        case lav1.event.Event.Event.Created(created) =>
          ActiveContract.fromLedgerApi(created).map(a => Contract[lav1.value.Value](\/-(a)))
        case lav1.event.Event.Event.Archived(archived) =>
          ArchivedContract.fromLedgerApi(archived).map(a => Contract[lav1.value.Value](-\/(a)))
        case lav1.event.Event.Event.Empty =>
          val errorMsg = s"Expected either Created or Archived event, got: Empty"
          -\/(Error(Symbol("Contract_fromLedgerApi"), errorMsg))
      }

    def fromTreeEvent(
        eventsById: Map[String, lav1.transaction.TreeEvent]
    )(eventId: String): Error \/ Vector[Contract[lav1.value.Value]] = {
      import scalaz.syntax.applicative._

      @tailrec
      def loop(
          es: Vector[String],
          acc: Error \/ Vector[Contract[lav1.value.Value]],
      ): Error \/ Vector[Contract[lav1.value.Value]] = es match {
        case head +: tail =>
          eventsById(head).kind match {
            case lav1.transaction.TreeEvent.Kind.Created(created) =>
              val a =
                ActiveContract.fromLedgerApi(created).map(a => Contract[lav1.value.Value](\/-(a)))
              val newAcc = ^(acc, a)(_ :+ _)
              loop(tail, newAcc)
            case lav1.transaction.TreeEvent.Kind.Exercised(exercised) =>
              val a = ArchivedContract
                .fromLedgerApi(exercised)
                .map(_.map(a => Contract[lav1.value.Value](-\/(a))))
              val newAcc = ^(acc, a)(_ ++ _.toVector)
              loop(exercised.childEventIds.toVector ++ tail, newAcc)
            case lav1.transaction.TreeEvent.Kind.Empty =>
              val errorMsg = s"Expected either Created or Exercised event, got: Empty"
              -\/(Error(Symbol("Contract_fromTreeEvent"), errorMsg))
          }
        // Wildcard to make the exhaustiveness checker happy.
        case _ =>
          acc
      }

      loop(Vector(eventId), \/-(Vector()))
    }

    implicit val covariant: Traverse[Contract] = new Traverse[Contract] {

      override def map[A, B](fa: Contract[A])(f: A => B): Contract[B] = {
        val valueB: ArchivedContract \/ ActiveContract[B] = fa.value.map(a => a.map(f))
        Contract(valueB)
      }

      override def traverseImpl[G[_]: Applicative, A, B](
          fa: Contract[A]
      )(f: A => G[B]): G[Contract[B]] = {
        val valueB: G[ArchivedContract \/ ActiveContract[B]] = fa.value.traverse(a => a.traverse(f))
        valueB.map(x => Contract[B](x))
      }
    }
  }

  private[http] object ActiveContractExtras {
    // only used in integration tests
    implicit val `AcC hasTemplateId`: HasTemplateId[ActiveContract] =
      new HasTemplateId[ActiveContract] {
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
            g: PackageService.ResolveChoiceArgType,
            h: PackageService.ResolveKeyType,
        ): Error \/ LfType =
          f(templateId)
            .leftMap(e => Error(Symbol("ActiveContract_hasTemplateId_lfType"), e.shows))
      }
  }

  object ArchivedContract {
    def fromLedgerApi(in: lav1.event.ArchivedEvent): Error \/ ArchivedContract =
      for {
        templateId <- in.templateId required "templateId"
      } yield ArchivedContract(
        contractId = ContractId(in.contractId),
        templateId = TemplateId fromLedgerApi templateId,
      )

    def fromLedgerApi(in: lav1.event.ExercisedEvent): Error \/ Option[ArchivedContract] =
      if (in.consuming) {
        for {
          templateId <- in.templateId.required("templateId")
        } yield Some(
          ArchivedContract(
            contractId = ContractId(in.contractId),
            templateId = TemplateId.fromLedgerApi(templateId),
          )
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
          fa: ContractLocator[A]
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
          fa: EnrichedContractKey[A]
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
            g: PackageService.ResolveChoiceArgType,
            h: PackageService.ResolveKeyType,
        ): Error \/ LfType =
          h(templateId)
            .leftMap(e => Error(Symbol("EnrichedContractKey_hasTemplateId_lfType"), e.shows))
      }
  }

  object ContractKeyStreamRequest {
    implicit def covariantR[Off]: Traverse[ContractKeyStreamRequest[Off, *]] = {
      type F[A] = ContractKeyStreamRequest[Off, A]
      new Traverse[F] {
        override def traverseImpl[G[_]: Applicative, A, B](fa: F[A])(f: A => G[B]): G[F[B]] =
          fa.ekey traverse f map (ekey => fa copy (ekey = ekey))
      }
    }

    implicit def hasTemplateId[Off]: HasTemplateId[ContractKeyStreamRequest[Off, *]] =
      HasTemplateId.by[ContractKeyStreamRequest[Off, *]](_.ekey)
  }

  type LfType = iface.Type

  trait HasTemplateId[F[_]] {
    def templateId(fa: F[_]): TemplateId.OptionalPkg

    def lfType(
        fa: F[_],
        templateId: TemplateId.RequiredPkg,
        f: PackageService.ResolveTemplateRecordType,
        g: PackageService.ResolveChoiceArgType,
        h: PackageService.ResolveKeyType,
    ): Error \/ LfType
  }

  object HasTemplateId {
    def by[F[_]]: By[F] = new By[F](0)

    final class By[F[_]](private val ign: Int) extends AnyVal {
      def apply[G[_]](nt: F[_] => G[_])(implicit basis: HasTemplateId[G]): HasTemplateId[F] =
        new HasTemplateId[F] {
          override def templateId(fa: F[_]) = basis templateId nt(fa)

          override def lfType(
              fa: F[_],
              templateId: TemplateId.RequiredPkg,
              f: PackageService.ResolveTemplateRecordType,
              g: PackageService.ResolveChoiceArgType,
              h: PackageService.ResolveKeyType,
          ) = basis.lfType(nt(fa), templateId, f, g, h)
        }
    }
  }

  object CreateCommand {
    implicit val bitraverseInstance: Bitraverse[CreateCommand] = new Bitraverse[CreateCommand] {
      override def bitraverseImpl[G[_]: Applicative, A, B, C, D](
          fab: CreateCommand[A, B]
      )(f: A => G[C], g: B => G[D]): G[CreateCommand[C, D]] = {
        import scalaz.syntax.applicative._
        ^(f(fab.payload), g(fab.templateId))((c, d) => fab.copy(payload = c, templateId = d))
      }
    }
  }

  object ExerciseCommand {
    implicit val bitraverseInstance: Bitraverse[ExerciseCommand] = new Bitraverse[ExerciseCommand] {
      override def bitraverseImpl[G[_]: Applicative, A, B, C, D](
          fab: ExerciseCommand[A, B]
      )(f: A => G[C], g: B => G[D]): G[ExerciseCommand[C, D]] = {
        import scalaz.syntax.applicative._
        ^(f(fab.argument), g(fab.reference))((c, d) => fab.copy(argument = c, reference = d))
      }
    }

    implicit val leftTraverseInstance: Traverse[ExerciseCommand[+*, Nothing]] =
      bitraverseInstance.leftTraverse

    implicit val hasTemplateId =
      new HasTemplateId[ExerciseCommand[+*, domain.ContractLocator[_]]] {

        override def templateId(
            fab: ExerciseCommand[_, domain.ContractLocator[_]]
        ): TemplateId.OptionalPkg = {
          fab.reference match {
            case EnrichedContractKey(templateId, _) => templateId
            case EnrichedContractId(Some(templateId), _) => templateId
            case EnrichedContractId(None, _) =>
              throw new IllegalArgumentException(
                "Please specify templateId, optional templateId is not supported yet!"
              )
          }
        }

        override def lfType(
            fa: ExerciseCommand[_, domain.ContractLocator[_]],
            templateId: TemplateId.RequiredPkg,
            f: PackageService.ResolveTemplateRecordType,
            g: PackageService.ResolveChoiceArgType,
            h: PackageService.ResolveKeyType,
        ): Error \/ LfType =
          g(templateId, fa.choice)
            .leftMap(e => Error(Symbol("ExerciseCommand_hasTemplateId_lfType"), e.shows))
      }
  }

  object ExerciseResponse {
    implicit val traverseInstance: Traverse[ExerciseResponse] = new Traverse[ExerciseResponse] {
      override def traverseImpl[G[_]: Applicative, A, B](
          fa: ExerciseResponse[A]
      )(f: A => G[B]): G[ExerciseResponse[B]] = {
        import scalaz.syntax.applicative._
        val gb: G[B] = f(fa.exerciseResult)
        val gbs: G[List[Contract[B]]] = fa.events.traverse(_.traverse(f))
        ^(gb, gbs) { (exerciseResult, events) =>
          ExerciseResponse(
            exerciseResult = exerciseResult,
            events = events,
          )
        }
      }
    }
  }

  sealed abstract class SyncResponse[+R] extends Product with Serializable {
    def status: StatusCode
  }

  final case class OkResponse[+R](
      result: R,
      warnings: Option[ServiceWarning] = None,
      status: StatusCode = StatusCodes.OK,
  ) extends SyncResponse[R]

  final case class ErrorResponse(
      errors: List[String],
      warnings: Option[ServiceWarning],
      status: StatusCode,
  ) extends SyncResponse[Nothing]

  object OkResponse {
    implicit val covariant: Traverse[OkResponse] = new Traverse[OkResponse] {
      override def traverseImpl[G[_]: Applicative, A, B](fa: OkResponse[A])(
          f: A => G[B]
      ): G[OkResponse[B]] =
        f(fa.result).map(b => fa.copy(result = b))
    }
  }

  object SyncResponse {
    implicit val covariant: Traverse[SyncResponse] = new Traverse[SyncResponse] {
      override def traverseImpl[G[_]: Applicative, A, B](
          fa: SyncResponse[A]
      )(f: A => G[B]): G[SyncResponse[B]] = {
        import scalaz.syntax.functor._
        val G = implicitly[Applicative[G]]
        fa match {
          case err: ErrorResponse => G.point[SyncResponse[B]](err)
          case ok: OkResponse[A] => OkResponse.covariant.traverse(ok)(f).widen
        }
      }
    }
  }

  sealed abstract class ServiceWarning extends Serializable with Product

  final case class UnknownTemplateIds(unknownTemplateIds: List[TemplateId.OptionalPkg])
      extends ServiceWarning

  final case class UnknownParties(unknownParties: List[domain.Party]) extends ServiceWarning

  // It wraps warnings in the streaming API.. TODO(Leo): define AsyncResponse ADT
  final case class AsyncWarningsWrapper(warnings: ServiceWarning)
}
