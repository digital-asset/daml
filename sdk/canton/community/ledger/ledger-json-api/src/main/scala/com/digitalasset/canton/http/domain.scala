// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import org.apache.pekko.http.scaladsl.model.{StatusCode, StatusCodes}
import com.digitalasset.canton.ledger.api.refinements.ApiTypes as lar
import com.daml.ledger.api.v2 as lav2
import com.digitalasset.daml.lf.typesig
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyReturningOps.*
import com.digitalasset.canton.fetchcontracts.util.IdentifierConverters.apiIdentifier
import com.digitalasset.canton.ledger.api.domain.User
import com.google.protobuf.ByteString
import scalaz.Isomorphism.{<~>, IsoFunctorTemplate}
import scalaz.std.list.*
import scalaz.std.option.*
import scalaz.std.vector.*
import scalaz.syntax.apply.{^, ^^}
import scalaz.syntax.show.*
import scalaz.syntax.tag.*
import scalaz.syntax.traverse.*
import scalaz.{-\/, Applicative, Bitraverse, Functor, NonEmptyList, Traverse, \/, \/-}

import scala.annotation.tailrec

package object domain {

  import com.digitalasset.canton.fetchcontracts.domain as here
  import scalaz.{@@, Tag}

  type Error = here.Error
  final val Error = here.Error
  type LfValue = here.LfValue
  type ContractId = here.ContractId
  final val ContractId = here.ContractId
  type Party = here.Party
  final val Party = here.Party
  type PartySet = here.PartySet
  type Offset = here.Offset
  final val Offset = here.Offset
  type ActiveContract[+CtTyId, +LfV] = here.ActiveContract[CtTyId, LfV]
  final val ActiveContract = here.ActiveContract

  type InputContractRef[LfV] =
    (ContractTypeId.Template.RequiredPkg, LfV) \/ (Option[ContractTypeId.RequiredPkg], ContractId)

  type ResolvedContractRef[LfV] =
    (ContractTypeId.Template.RequiredPkg, LfV) \/ (ContractTypeId.RequiredPkg, ContractId)

  type ApplicationIdTag = lar.ApplicationIdTag
  type ApplicationId = lar.ApplicationId
  val ApplicationId = lar.ApplicationId

  type Choice = lar.Choice
  val Choice = lar.Choice

  type CommandIdTag = lar.CommandIdTag
  type CommandId = lar.CommandId
  val CommandId = lar.CommandId

  type SubmissionId = String @@ SubmissionIdTag
  val SubmissionId = Tag.of[SubmissionIdTag]

  type WorkflowIdTag = lar.WorkflowIdTag
  type WorkflowId = String @@ WorkflowIdTag
  val WorkflowId = Tag.of[WorkflowIdTag]

  type LfType = typesig.Type

  type RetryInfoDetailDuration = scala.concurrent.duration.Duration @@ RetryInfoDetailDurationTag
  val RetryInfoDetailDuration = Tag.of[RetryInfoDetailDurationTag]

  type CompletionOffset = String @@ CompletionOffsetTag
  val CompletionOffset = Tag.of[CompletionOffsetTag]

  type Base64 = ByteString @@ Base64Tag
  val Base64 = Tag.of[Base64Tag]

  type Base16 = ByteString @@ Base16Tag
  val Base16 = Tag.of[Base16Tag]
}

package domain {

  import com.daml.ledger.api.v2.commands.Commands
  import com.digitalasset.daml.lf.data.Ref.{HexString, PackageId, PackageRef}
  import com.digitalasset.canton.fetchcontracts.domain.`fc domain ErrorOps`
  import com.digitalasset.canton.platform.ApiOffset
  import com.digitalasset.canton.topology.DomainId

  sealed trait SubmissionIdTag

  sealed trait CompletionOffsetTag

  sealed trait Base64Tag
  sealed trait Base16Tag

  trait JwtPayloadTag

  trait JwtPayloadG {
    val applicationId: ApplicationId
    val readAs: List[Party]
    val actAs: List[Party]
    val parties: PartySet
  }

  // write endpoints require at least one party in actAs
  // (only the first one is used for pre-multiparty ledgers)
  // but we can have multiple parties in readAs.
  final case class JwtWritePayload(
      applicationId: ApplicationId,
      submitter: NonEmptyList[Party],
      readAs: List[Party],
  ) extends JwtPayloadG {
    override val actAs: List[Party] = submitter.toList
    override val parties: PartySet =
      submitter.toSet1 ++ readAs
  }

// As with JwtWritePayload, but supports empty `actAs`.  At least one of
// `actAs` or `readAs` must be non-empty.
  sealed abstract case class JwtPayload private (
      applicationId: ApplicationId,
      readAs: List[Party],
      actAs: List[Party],
      parties: PartySet,
  ) extends JwtPayloadG {}

  object JwtPayload {
    def apply(
        applicationId: ApplicationId,
        readAs: List[Party],
        actAs: List[Party],
    ): Option[JwtPayload] =
      (readAs ++ actAs) match {
        case NonEmpty(ps) =>
          Some(
            new JwtPayload(applicationId, readAs, actAs, ps.toSet) {}
          )
        case _ => None
      }
  }

  case class Contract[LfV](value: ArchivedContract \/ ActiveContract.ResolvedCtTyId[LfV])

  case class ArchivedContract(contractId: ContractId, templateId: ContractTypeId.RequiredPkgId)

  final case class FetchRequest[+LfV](
      locator: ContractLocator[LfV],
      readAs: Option[NonEmptyList[Party]],
  ) {
    def traverseLocator[F[_]: Functor, OV](
        f: ContractLocator[LfV] => F[ContractLocator[OV]]
    ): F[FetchRequest[OV]] = f(locator) map (l => copy(locator = l))
  }

  sealed abstract class ContractLocator[+LfV] extends Product with Serializable

  final case class EnrichedContractKey[+LfV](
      templateId: ContractTypeId.Template.RequiredPkg,
      key: LfV,
  ) extends ContractLocator[LfV]

  final case class EnrichedContractId(
      templateId: Option[ContractTypeId.RequiredPkg],
      contractId: domain.ContractId,
  ) extends ContractLocator[Nothing]

  final case class ContractKeyStreamRequest[+Cid, +LfV](
      contractIdAtOffset: Cid,
      ekey: EnrichedContractKey[LfV],
  )

  final case class GetActiveContractsRequest(
      templateIds: NonEmpty[Set[ContractTypeId.RequiredPkg]],
      readAs: Option[NonEmptyList[Party]],
  )

  final case class SearchForeverRequest(
      queriesWithPos: NonEmptyList[(SearchForeverQuery, Int)]
  )

  final case class SearchForeverQuery(
      templateIds: NonEmpty[Set[ContractTypeId.RequiredPkg]],
      offset: Option[domain.Offset],
  )

  final case class PartyDetails(identifier: Party, displayName: Option[String], isLocal: Boolean)

  // Important note: when changing this ADT, adapt the custom associated JsonFormat codec in JsonProtocol
  sealed abstract class UserRight extends Product with Serializable
  final case object ParticipantAdmin extends UserRight
  final case object IdentityProviderAdmin extends UserRight
  final case class CanActAs(party: Party) extends UserRight
  final case class CanReadAs(party: Party) extends UserRight
  final case object CanReadAsAnyParty extends UserRight

  object UserRights {
    import com.digitalasset.daml.lf.data.Ref
    import com.digitalasset.canton.ledger.api.domain.UserRight as LedgerUserRight
    import scalaz.syntax.std.either.*
    import scalaz.syntax.traverse.*

    def toLedgerUserRights(input: List[UserRight]): String \/ List[LedgerUserRight] =
      input.traverse {
        case ParticipantAdmin => \/.right(LedgerUserRight.ParticipantAdmin)
        case IdentityProviderAdmin => \/.right(LedgerUserRight.IdentityProviderAdmin)
        case CanActAs(party) =>
          Ref.Party.fromString(party.unwrap).map(LedgerUserRight.CanActAs.apply).disjunction
        case CanReadAs(party) =>
          Ref.Party.fromString(party.unwrap).map(LedgerUserRight.CanReadAs.apply).disjunction
        case CanReadAsAnyParty => \/.right(LedgerUserRight.CanReadAsAnyParty)
      }

    def fromLedgerUserRights(input: Seq[LedgerUserRight]): List[UserRight] = input
      .map[domain.UserRight] {
        case LedgerUserRight.ParticipantAdmin => ParticipantAdmin
        case LedgerUserRight.IdentityProviderAdmin => IdentityProviderAdmin
        case LedgerUserRight.CanReadAsAnyParty => CanReadAsAnyParty
        case LedgerUserRight.CanActAs(party) =>
          CanActAs(Party(party: String))
        case LedgerUserRight.CanReadAs(party) =>
          CanReadAs(Party(party: String))
      }
      .toList
  }

  final case class UserDetails(userId: String, primaryParty: Option[String])

  object UserDetails {
    def fromUser(user: User) =
      UserDetails(user.id, user.primaryParty)
  }

  final case class CreateUserRequest(
      userId: String,
      primaryParty: Option[String],
      rights: Option[List[UserRight]],
  )

  final case class ListUserRightsRequest(userId: String)

  final case class GrantUserRightsRequest(
      userId: String,
      rights: List[UserRight],
  )

  final case class RevokeUserRightsRequest(
      userId: String,
      rights: List[UserRight],
  )

  final case class GetUserRequest(userId: String)

  final case class DeleteUserRequest(userId: String)

  final case class AllocatePartyRequest(identifierHint: Option[Party], displayName: Option[String])

  // Important note: when changing this ADT, adapt the custom associated JsonFormat codec in JsonProtocol
  sealed abstract class DeduplicationPeriod extends Product with Serializable {
    def toProto: Commands.DeduplicationPeriod =
      this match {
        case DeduplicationPeriod.Duration(millis) =>
          Commands.DeduplicationPeriod.DeduplicationDuration(
            com.google.protobuf.duration.Duration(java.time.Duration.ofMillis(millis))
          )
        case DeduplicationPeriod.Offset(offset) =>
          Commands.DeduplicationPeriod
            .DeduplicationOffset(ApiOffset.assertFromStringToLong(offset))
      }
  }

  object DeduplicationPeriod {
    final case class Duration(durationInMillis: Long) extends domain.DeduplicationPeriod
    final case class Offset(offset: HexString) extends domain.DeduplicationPeriod
  }

  final case class DisclosedContract[+TmplId](
      contractId: ContractId,
      templateId: TmplId,
      createdEventBlob: Base64,
  ) {
    def toLedgerApi(implicit
        TmplId: TmplId <:< ContractTypeId.Template.RequiredPkg
    ): lav2.commands.DisclosedContract =
      lav2.commands.DisclosedContract(
        templateId = Some(apiIdentifier(templateId)),
        contractId = ContractId unwrap contractId,
        createdEventBlob = Base64 unwrap createdEventBlob,
      )
  }

  object DisclosedContract {
    type LAV = DisclosedContract[ContractTypeId.Template.RequiredPkg]

    implicit val covariant: Traverse[DisclosedContract] =
      new Traverse[DisclosedContract] {
        override def traverseImpl[G[_]: Applicative, A, B](
            fab: DisclosedContract[A]
        )(f: A => G[B]): G[DisclosedContract[B]] =
          f(fab.templateId).map(tId => fab.copy(templateId = tId))
      }
  }

  /** @tparam TmplId disclosed contracts' template ID
    */
  final case class CommandMeta[+TmplId](
      commandId: Option[CommandId],
      actAs: Option[NonEmptyList[Party]],
      readAs: Option[List[Party]],
      submissionId: Option[SubmissionId],
      workflowId: Option[WorkflowId],
      deduplicationPeriod: Option[domain.DeduplicationPeriod],
      disclosedContracts: Option[List[DisclosedContract[TmplId]]],
      domainId: Option[DomainId],
      packageIdSelectionPreference: Option[List[PackageId]],
  )

  object CommandMeta {
    type NoDisclosed = CommandMeta[Nothing]
    type IgnoreDisclosed = CommandMeta[Any]
    type LAV = CommandMeta[ContractTypeId.Template.RequiredPkg]

    implicit val covariant: Traverse[CommandMeta] = new Traverse[CommandMeta] {
      override def traverseImpl[G[_]: Applicative, A, B](
          fab: CommandMeta[A]
      )(f: A => G[B]): G[CommandMeta[B]] =
        fab.disclosedContracts
          .traverse(_.traverse(_.traverse(f)))
          .map(dc => fab.copy(disclosedContracts = dc))
    }
  }

  final case class CreateCommand[+LfV, +TmplId](
      templateId: TmplId,
      payload: LfV,
      meta: Option[CommandMeta.NoDisclosed],
  ) {
    def traversePayload[G[_]: Applicative, LfVB, TmplId0 >: TmplId](
        f: LfV => G[LfVB]
    ): G[CreateCommand[LfVB, TmplId0]] =
      Bitraverse[CreateCommand].leftTraverse[TmplId0].traverse(this)(f)
  }

  final case class ExerciseCommand[+PkgId, +LfV, +Ref](
      reference: Ref,
      choice: domain.Choice,
      argument: LfV,
      // passing a template ID is allowed; we distinguish internally
      choiceInterfaceId: Option[ContractTypeId[PkgId]],
      meta: Option[CommandMeta[ContractTypeId.Template[PkgId]]],
  )

  final case class CreateAndExerciseCommand[+Payload, +Arg, +TmplId, +IfceId](
      templateId: TmplId,
      payload: Payload,
      choice: domain.Choice,
      argument: Arg,
      // passing a template ID is allowed; we distinguish internally
      choiceInterfaceId: Option[IfceId],
      meta: Option[CommandMeta[TmplId]],
  )

  final case class CreateCommandResponse[+LfV](
      contractId: ContractId,
      templateId: ContractTypeId.Template.RequiredPkgId,
      key: Option[LfV],
      payload: LfV,
      signatories: Seq[Party],
      observers: Seq[Party],
      completionOffset: CompletionOffset,
  )

  final case class ExerciseResponse[LfV](
      exerciseResult: LfV,
      events: List[Contract[LfV]],
      completionOffset: CompletionOffset,
  )

  object PartyDetails {
    def fromLedgerApi(p: com.digitalasset.canton.ledger.api.domain.PartyDetails): PartyDetails =
      PartyDetails(Party(p.party), p.displayName, p.isLocal)
  }

  final case class StartingOffset(offset: Offset)

  object Contract {

    def fromTransactionTree(
        tx: lav2.transaction.TransactionTree
    ): Error \/ Vector[Contract[lav2.value.Value]] =
      tx.rootEventIds.toVector
        .map(fromTreeEvent(tx.eventsById))
        .sequence
        .map(_.flatten)

    private[this] def fromTreeEvent(
        eventsById: Map[String, lav2.transaction.TreeEvent]
    )(eventId: String): Error \/ Vector[Contract[lav2.value.Value]] = {
      @tailrec
      def loop(
          es: Vector[String],
          acc: Error \/ Vector[Contract[lav2.value.Value]],
      ): Error \/ Vector[Contract[lav2.value.Value]] = es match {
        case head +: tail =>
          eventsById(head).kind match {
            case lav2.transaction.TreeEvent.Kind.Created(created) =>
              val a =
                ActiveContract
                  .fromLedgerApi(domain.ActiveContract.ExtractAs.Template, created)
                  .map(a => Contract[lav2.value.Value](\/-(a)))
              val newAcc = ^(acc, a)(_ :+ _)
              loop(tail, newAcc)
            case lav2.transaction.TreeEvent.Kind.Exercised(exercised) =>
              val a = ArchivedContract
                .fromLedgerApi(exercised)
                .map(_.map(a => Contract[lav2.value.Value](-\/(a))))
              val newAcc = ^(acc, a)(_ ++ _.toVector)
              loop(exercised.childEventIds.toVector ++ tail, newAcc)
            case lav2.transaction.TreeEvent.Kind.Empty =>
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
        val valueB: ArchivedContract \/ ActiveContract.ResolvedCtTyId[B] =
          fa.value.map(a => a.map(f))
        Contract(valueB)
      }

      override def traverseImpl[G[_]: Applicative, A, B](
          fa: Contract[A]
      )(f: A => G[B]): G[Contract[B]] = {
        val valueB: G[ArchivedContract \/ ActiveContract.ResolvedCtTyId[B]] =
          fa.value.traverse(a => a.traverse(f))
        valueB.map(x => Contract[B](x))
      }
    }
  }

  object ActiveContractExtras {
    // only used in integration tests
    implicit val `AcC hasTemplateId`: HasTemplateId.Compat[ActiveContract.ResolvedCtTyId] =
      new HasTemplateId[ActiveContract.ResolvedCtTyId] {
        override def templateId(fa: ActiveContract.ResolvedCtTyId[_]): ContractTypeId.RequiredPkg =
          fa.templateId.map(PackageRef.Id(_))

        type TypeFromCtId = LfType

        override def lfType(
            fa: ActiveContract.ResolvedCtTyId[_],
            templateId: ContractTypeId.ResolvedPkgId,
            f: PackageService.ResolveTemplateRecordType,
            g: PackageService.ResolveChoiceArgType,
            h: PackageService.ResolveKeyType,
        ): Error \/ LfType =
          templateId match {
            case tid @ ContractTypeId.Template(_, _, _) =>
              f(tid: ContractTypeId.Template.ResolvedPkgId)
                .leftMap(e => Error(Symbol("ActiveContract_hasTemplateId_lfType"), e.shows))
            case other =>
              val errorMsg = s"Expect contract type Id to be template Id, got otherwise: $other"
              -\/(Error(Symbol("ActiveContract_hasTemplateId_lfType"), errorMsg))
          }
      }
  }

  object ArchivedContract {
    def fromLedgerApi(
        resolvedQuery: domain.ResolvedQuery,
        in: lav2.event.ArchivedEvent,
    ): Error \/ ArchivedContract = {
      val resolvedTemplateId = resolvedQuery match {
        case ResolvedQuery.ByInterfaceId(interfaceId) =>
          (in.templateId required "templateId")
            .map(ContractTypeId.Interface.fromLedgerApi)
            // Use the interface id that was queried for, but with the package id returned.
            .map(gotId => interfaceId.latestPkgId.copy(packageId = gotId.packageId))
        case _ =>
          (in.templateId required "templateId").map(ContractTypeId.Template.fromLedgerApi)
      }
      for {
        templateId <- resolvedTemplateId
      } yield ArchivedContract(
        contractId = ContractId(in.contractId),
        templateId = templateId,
      )
    }

    def fromLedgerApi(in: lav2.event.ExercisedEvent): Error \/ Option[ArchivedContract] =
      if (in.consuming) {
        for {
          templateId <- in.templateId.required("templateId")
        } yield Some(
          ArchivedContract(
            contractId = ContractId(in.contractId),
            templateId = ContractTypeId.Template fromLedgerApi templateId,
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
      )(f: A => G[B]): G[ContractLocator[B]] =
        fa match {
          case ka: EnrichedContractKey[A] =>
            f(ka.key).map(b => EnrichedContractKey(ka.templateId, b))
          case c: EnrichedContractId =>
            val G: Applicative[G] = implicitly
            G.point(c)
        }
    }

    val structure: ContractLocator <~> InputContractRef =
      new IsoFunctorTemplate[ContractLocator, InputContractRef] {
        override def from[A](ga: InputContractRef[A]) =
          ga.fold((EnrichedContractKey[A] _).tupled, (EnrichedContractId.apply _).tupled)

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
      )(f: A => G[B]): G[EnrichedContractKey[B]] =
        f(fa.key).map(b => EnrichedContractKey(fa.templateId, b))
    }

    implicit val hasTemplateId: HasTemplateId.Compat[EnrichedContractKey] =
      new HasTemplateId[EnrichedContractKey] {

        override def templateId(fa: EnrichedContractKey[_]): ContractTypeId.RequiredPkg =
          fa.templateId

        type TypeFromCtId = LfType

        override def lfType(
            fa: EnrichedContractKey[_],
            templateId: ContractTypeId.ResolvedPkgId,
            f: PackageService.ResolveTemplateRecordType,
            g: PackageService.ResolveChoiceArgType,
            h: PackageService.ResolveKeyType,
        ): Error \/ LfType =
          templateId match {
            case tid @ ContractTypeId.Template(_, _, _) =>
              h(tid: ContractTypeId.Template.ResolvedPkgId)
                .leftMap(e => Error(Symbol("EnrichedContractKey_hasTemplateId_lfType"), e.shows))
            case other =>
              val errorMsg = s"Expect contract type Id to be template Id, got otherwise: $other"
              -\/(Error(Symbol("EnrichedContractKey_hasTemplateId_lfType"), errorMsg))
          }
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

    implicit def hasTemplateId[Off]: HasTemplateId.Compat[ContractKeyStreamRequest[Off, *]] =
      HasTemplateId.by[ContractKeyStreamRequest[Off, *]](_.ekey)
  }

  trait HasTemplateId[-F[_]] {
    protected[this] type FHuh = F[_] // how to pronounce "F[?]" or "F huh?"

    def templateId(fa: F[_]): ContractTypeId.RequiredPkg

    type TypeFromCtId

    def lfType(
        fa: F[_],
        templateId: ContractTypeId.ResolvedPkgId,
        f: PackageService.ResolveTemplateRecordType,
        g: PackageService.ResolveChoiceArgType,
        h: PackageService.ResolveKeyType,
    ): Error \/ TypeFromCtId
  }

  object HasTemplateId {
    type Compat[-F[_]] = Aux[F, LfType]
    type Aux[-F[_], TFC0] = HasTemplateId[F] { type TypeFromCtId = TFC0 }

    def by[F[_]]: By[F] = new By[F](0)

    final class By[F[_]](private val ign: Int) extends AnyVal {
      def apply[G[_]](
          nt: F[_] => G[_]
      )(implicit basis: HasTemplateId[G]): Aux[F, basis.TypeFromCtId] =
        new HasTemplateId[F] {
          override def templateId(fa: F[_]) = basis templateId nt(fa)

          type TypeFromCtId = basis.TypeFromCtId

          override def lfType(
              fa: F[_],
              templateId: ContractTypeId.ResolvedPkgId,
              f: PackageService.ResolveTemplateRecordType,
              g: PackageService.ResolveChoiceArgType,
              h: PackageService.ResolveKeyType,
          ): Error \/ TypeFromCtId = basis.lfType(nt(fa), templateId, f, g, h)
        }
    }
  }

  object CreateCommand {
    type RequiredPkg[+LfV] = CreateCommand[LfV, ContractTypeId.Template.RequiredPkg]

    implicit val bitraverseInstance: Bitraverse[CreateCommand] = new Bitraverse[CreateCommand] {
      override def bitraverseImpl[G[_]: Applicative, A, B, C, D](
          fab: CreateCommand[A, B]
      )(f: A => G[C], g: B => G[D]): G[CreateCommand[C, D]] =
        ^(f(fab.payload), g(fab.templateId))((c, d) => fab.copy(payload = c, templateId = d))
    }
  }

  object ExerciseCommand {
    type RequiredPkg[+LfV, +R] = ExerciseCommand[PackageRef, LfV, R]

    implicit def bitraverseInstance[PkgId]: Bitraverse[ExerciseCommand[PkgId, *, *]] =
      new Bitraverse[ExerciseCommand[PkgId, *, *]] {
        override def bitraverseImpl[G[_]: Applicative, A, B, C, D](
            fab: ExerciseCommand[PkgId, A, B]
        )(f: A => G[C], g: B => G[D]): G[ExerciseCommand[PkgId, C, D]] =
          ^(f(fab.argument), g(fab.reference))((argument, reference) =>
            fab.copy(
              argument = argument,
              reference = reference,
            )
          )
      }

    implicit val leftTraverseInstance: Traverse[RequiredPkg[+*, Nothing]] =
      bitraverseInstance[PackageRef].leftTraverse

    implicit val hasTemplateId: HasTemplateId.Aux[RequiredPkg[
      +*,
      domain.ContractLocator[_],
    ], (Option[domain.ContractTypeId.Interface.ResolvedPkgId], LfType)] =
      new HasTemplateId[RequiredPkg[+*, domain.ContractLocator[_]]] {
        override def templateId(fab: FHuh): ContractTypeId.RequiredPkg =
          fab.choiceInterfaceId getOrElse (fab.reference match {
            case EnrichedContractKey(templateId, _) => templateId
            case EnrichedContractId(Some(templateId), _) => templateId
            case EnrichedContractId(None, _) =>
              throw new IllegalArgumentException(
                "Please specify templateId, optional templateId is not supported yet!"
              )
          })

        type TypeFromCtId = (Option[domain.ContractTypeId.Interface.ResolvedPkgId], LfType)

        override def lfType(
            fa: FHuh,
            templateId: ContractTypeId.ResolvedPkgId,
            f: PackageService.ResolveTemplateRecordType,
            g: PackageService.ResolveChoiceArgType,
            h: PackageService.ResolveKeyType,
        ) =
          g(templateId, fa.choice)
            .leftMap(e => Error(Symbol("ExerciseCommand_hasTemplateId_lfType"), e.shows))
      }
  }

  object CreateAndExerciseCommand {
    type LAVUnresolved = CreateAndExerciseCommand[
      lav2.value.Record,
      lav2.value.Value,
      domain.ContractTypeId.Template.RequiredPkg,
      domain.ContractTypeId.RequiredPkg,
    ]

    type LAVResolved = CreateAndExerciseCommand[
      lav2.value.Record,
      lav2.value.Value,
      domain.ContractTypeId.Template.RequiredPkg,
      domain.ContractTypeId.RequiredPkg,
    ]

    implicit final class `CAEC traversePayloadArg`[P, Ar, T, I](
        private val self: CreateAndExerciseCommand[P, Ar, T, I]
    ) extends AnyVal {
      def traversePayloadsAndArgument[G[_]: Applicative, P2, Ar2](
          f: P => G[P2],
          g: Ar => G[Ar2],
      ): G[CreateAndExerciseCommand[P2, Ar2, T, I]] =
        ^(f(self.payload), g(self.argument))((p, a) => self.copy(payload = p, argument = a))
    }

    implicit def covariant[P, Ar]: Bitraverse[CreateAndExerciseCommand[P, Ar, *, *]] =
      new Bitraverse[CreateAndExerciseCommand[P, Ar, *, *]] {
        override def bitraverseImpl[G[_]: Applicative, A, B, C, D](
            fa: CreateAndExerciseCommand[P, Ar, A, B]
        )(f: A => G[C], g: B => G[D]): G[CreateAndExerciseCommand[P, Ar, C, D]] =
          ^^(
            f(fa.templateId),
            fa.choiceInterfaceId traverse g,
            fa.meta traverse (_ traverse f),
          ) { (tId, ciId, meta) =>
            fa.copy(templateId = tId, choiceInterfaceId = ciId, meta = meta)
          }
      }
  }

  object ExerciseResponse {
    implicit val traverseInstance: Traverse[ExerciseResponse] = new Traverse[ExerciseResponse] {
      override def traverseImpl[G[_]: Applicative, A, B](
          fa: ExerciseResponse[A]
      )(f: A => G[B]): G[ExerciseResponse[B]] = {
        val gb: G[B] = f(fa.exerciseResult)
        val gbs: G[List[Contract[B]]] = fa.events.traverse(_.traverse(f))
        ^(gb, gbs) { (exerciseResult, events) =>
          fa.copy(
            exerciseResult = exerciseResult,
            events = events,
          )
        }
      }
    }
  }
  object CreateCommandResponse {
    implicit val covariant: Traverse[CreateCommandResponse] = new Traverse[CreateCommandResponse] {

      override def map[A, B](fa: CreateCommandResponse[A])(f: A => B): CreateCommandResponse[B] =
        fa.copy(key = fa.key map f, payload = f(fa.payload))

      override def traverseImpl[G[_]: Applicative, A, B](
          fa: CreateCommandResponse[A]
      )(f: A => G[B]): G[CreateCommandResponse[B]] = {
        import scalaz.syntax.apply.*
        val gk: G[Option[B]] = fa.key traverse f
        val ga: G[B] = f(fa.payload)
        ^(gk, ga)((k, a) => fa.copy(key = k, payload = a))
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

  sealed trait RetryInfoDetailDurationTag

  // Important note: when changing this ADT, adapt the custom associated JsonFormat codec in JsonProtocol
  sealed trait ErrorDetail extends Product with Serializable
  final case class ResourceInfoDetail(name: String, typ: String) extends ErrorDetail
  final case class ErrorInfoDetail(errorCodeId: String, metadata: Map[String, String])
      extends ErrorDetail
  final case class RetryInfoDetail(duration: RetryInfoDetailDuration) extends ErrorDetail
  final case class RequestInfoDetail(correlationId: String) extends ErrorDetail

  object ErrorDetail {
    import com.daml.error.utils.ErrorDetails
    def fromErrorUtils(errorDetail: ErrorDetails.ErrorDetail): domain.ErrorDetail =
      errorDetail match {
        case ErrorDetails.ResourceInfoDetail(name, typ) => domain.ResourceInfoDetail(name, typ)
        case ErrorDetails.ErrorInfoDetail(errorCodeId, metadata) =>
          domain.ErrorInfoDetail(errorCodeId, metadata)
        case ErrorDetails.RetryInfoDetail(duration) =>
          domain.RetryInfoDetail(domain.RetryInfoDetailDuration(duration))
        case ErrorDetails.RequestInfoDetail(correlationId) =>
          domain.RequestInfoDetail(correlationId)
      }
  }

  final case class LedgerApiError(
      code: Int,
      message: String,
      details: Seq[ErrorDetail],
  )

  final case class ErrorResponse(
      errors: List[String],
      warnings: Option[ServiceWarning],
      status: StatusCode,
      ledgerApiError: Option[LedgerApiError] = None,
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
        val G = implicitly[Applicative[G]]
        fa match {
          case err: ErrorResponse => G.point[SyncResponse[B]](err)
          case ok: OkResponse[A] => OkResponse.covariant.traverse(ok)(f).widen
        }
      }
    }
  }

  sealed abstract class ServiceWarning extends Serializable with Product

  final case class UnknownTemplateIds(unknownTemplateIds: List[ContractTypeId.RequiredPkg])
      extends ServiceWarning

  final case class UnknownParties(unknownParties: List[domain.Party]) extends ServiceWarning

  final case class AsyncWarningsWrapper(warnings: ServiceWarning)

  import com.digitalasset.daml.lf.data.Ref

  import scala.collection.IterableOps

  /** A contract type ID that may be either a template or an interface ID.
    * A [[ContractTypeId.ResolvedPkg]] ID will always be either [[ContractTypeId.Template]]
    * or [[ContractTypeId.Interface]]; an
    * unresolved ID may be one of those, which indicates an expectation of what
    * the resolved ID will be, or neither, which indicates that resolving what
    * kind of ID this is will be part of the resolution.
    *
    * Built-in equality is solely determined by the triple of package ID, module
    * name, entity name.  This is because there are likely insidious expectations
    * that this be true dating to before contract type IDs were distinguished at
    * all, and we are only interested in distinguishing them statically, which
    * these types do, and by pattern-matching, which does work.
    *
    * {{{
    *   val selector: ContractTypeId[Unit] = Template((), "M", "E")
    *   selector match {
    *     case ContractTypeId.Unknown(p, m, e) => // this will not match
    *     case ContractTypeId.Interface(p, m, e) => // this will not match
    *     case ContractTypeId.Template(p, m, e) => // this will match
    *   }
    * }}}
    */
  sealed abstract class ContractTypeId[+PkgId]
      extends Product3[PkgId, String, String]
      with Serializable
      with ContractTypeId.Ops[ContractTypeId, PkgId] {
    val packageId: PkgId
    val moduleName: String
    val entityName: String

    override def _1 = packageId

    override def _2 = moduleName

    override def _3 = entityName

    // the only way we want to tell the difference dynamically is when
    // pattern-matching.  If we didn't need that for query, we wouldn't even
    // bother with different classes, we would just use different newtypes.
    // Which would yield exactly the following dynamic equality behavior.
    override final def equals(o: Any) = o match {
      case o: ContractTypeId[_] =>
        (this eq o) || {
          packageId == o.packageId && moduleName == o.moduleName && entityName == o.entityName
        }
      case _ => false
    }

    override final def hashCode = {
      import scala.util.hashing.MurmurHash3 as H
      H.productHash(this, H.productSeed, ignorePrefix = true)
    }

    def fqn: String = s"${packageId.toString}:$moduleName:$entityName"
  }

  object ResolvedQuery {
    def apply[CtId[T] <: ContractTypeId[T]](resolved: ContractTypeRef[CtId]): ResolvedQuery =
      resolved match {
        case t: ContractTypeRef.TemplateRef => ByTemplateId(t)
        case i: ContractTypeRef.InterfaceRef => ByInterfaceId(i)
      }

    def apply(resolved: Set[_ <: ContractTypeRef[ContractTypeId]]): Unsupported \/ ResolvedQuery = {
      import com.daml.nonempty.{NonEmpty, Singleton}
      val (templateIds, interfaceIds) = partitionRefs(resolved)
      templateIds match {
        case NonEmpty(templateIds) =>
          interfaceIds match {
            case NonEmpty(_) => -\/(CannotQueryBothTemplateIdsAndInterfaceIds)
            case _ => \/-(ByTemplateIds(templateIds))
          }
        case _ =>
          interfaceIds match {
            case NonEmpty(Singleton(interfaceId)) => \/-(ByInterfaceId(interfaceId))
            case NonEmpty(_) => -\/(CannotQueryManyInterfaceIds)
            case _ => -\/(CannotBeEmpty)
          }
      }
    }

    def partition[CC[_], C, Pkg](
        resolved: IterableOps[ContractTypeId.Definite[Pkg], CC, C]
    ): (CC[ContractTypeId.Template[Pkg]], CC[ContractTypeId.Interface[Pkg]]) =
      resolved.partitionMap {
        case t @ ContractTypeId.Template(_, _, _) => Left(t)
        case i @ ContractTypeId.Interface(_, _, _) => Right(i)
      }

    def partitionRefs[CC[_], C](
        resolved: IterableOps[ContractTypeRef[ContractTypeId], CC, C]
    ): (CC[ContractTypeRef.TemplateRef], CC[ContractTypeRef.InterfaceRef]) =
      resolved.partitionMap {
        case t: ContractTypeRef.TemplateRef => Left(t)
        case i: ContractTypeRef.InterfaceRef => Right(i)
      }

    sealed abstract class Unsupported(val errorMsg: String) extends Product with Serializable

    final case object CannotQueryBothTemplateIdsAndInterfaceIds
        extends Unsupported("Cannot query both templates IDs and interface IDs")

    final case object CannotQueryManyInterfaceIds
        extends Unsupported("Cannot query more than one interface ID")

    final case object CannotBeEmpty
        extends Unsupported("Cannot resolve any template ID from request")

    final case class ByTemplateIds(templateIds: NonEmpty[Set[ContractTypeRef.TemplateRef]])
        extends ResolvedQuery {
      def resolved = templateIds
    }

    final case class ByTemplateId(templateId: ContractTypeRef.TemplateRef) extends ResolvedQuery {
      def resolved = NonEmpty(Set, templateId)
    }

    final case class ByInterfaceId(interfaceId: ContractTypeRef.InterfaceRef)
        extends ResolvedQuery {
      def resolved = NonEmpty(Set, interfaceId)
    }
  }

  sealed abstract class ResolvedQuery extends Product with Serializable {
    def resolved: NonEmpty[Set[_ <: ContractTypeRef.Resolved]]
  }

  object ContractTypeId extends ContractTypeIdLike[ContractTypeId] {
    final case class Unknown[+PkgId](
        packageId: PkgId,
        moduleName: String,
        entityName: String,
    ) extends ContractTypeId[PkgId]
        with Ops[Unknown, PkgId] {
      override def productPrefix = "ContractTypeId"

      override def copy[PkgId0](
          packageId: PkgId0 = packageId,
          moduleName: String = moduleName,
          entityName: String = entityName,
      ) = Unknown(packageId, moduleName, entityName)
    }

    sealed abstract class Definite[+PkgId] extends ContractTypeId[PkgId] with Ops[Definite, PkgId]

    /** A contract type ID known to be a template, not an interface.  When resolved,
      * it indicates that the LF environment associates this ID with a template.
      * When unresolved, it indicates that the intent is to search only template
      * IDs for resolution, and that resolving to an interface ID should be an error.
      */
    final case class Template[+PkgId](packageId: PkgId, moduleName: String, entityName: String)
        extends Definite[PkgId]
        with Ops[Template, PkgId] {
      override def productPrefix = "TemplateId"

      override def copy[PkgId0](
          packageId: PkgId0 = packageId,
          moduleName: String = moduleName,
          entityName: String = entityName,
      ) = Template(packageId, moduleName, entityName)
    }

    /** A contract type ID known to be an interface, not a template.  When resolved,
      * it indicates that the LF environment associates this ID with an interface.
      * When unresolved, it indicates that the intent is to search only interface
      * IDs for resolution, and that resolving to a template ID should be an error.
      */
    final case class Interface[+PkgId](packageId: PkgId, moduleName: String, entityName: String)
        extends Definite[PkgId]
        with Ops[Interface, PkgId] {
      override def productPrefix = "InterfaceId"

      override def copy[PkgId0](
          packageId: PkgId0 = packageId,
          moduleName: String = moduleName,
          entityName: String = entityName,
      ) = Interface(packageId, moduleName, entityName)
    }

    override def apply[PkgId](
        packageId: PkgId,
        moduleName: String,
        entityName: String,
    ): ContractTypeId[PkgId] =
      Unknown(packageId, moduleName, entityName)

    // Product3 makes custom unapply really cheap
    def unapply[PkgId](ctId: ContractTypeId[PkgId]): Some[ContractTypeId[PkgId]] = Some(ctId)

    // belongs in ultimate parent `object`
    implicit def `ContractTypeId covariant`[F[T] <: ContractTypeId[T] with ContractTypeId.Ops[F, T]]
        : Traverse[F] =
      new Traverse[F] {
        override def map[A, B](fa: F[A])(f: A => B): F[B] =
          fa.copy(packageId = f(fa.packageId))

        override def traverseImpl[G[_]: Applicative, A, B](fa: F[A])(f: A => G[B]): G[F[B]] =
          f(fa.packageId) map (p2 => fa.copy(packageId = p2))
      }

    object Unknown extends Like[Unknown]

    object Template extends Like[Template]

    object Interface extends Like[Interface]

    // TODO #14727 make an opaque subtype, produced by PackageService on
    // confirmed-present IDs only.  Can probably start by adding
    // `with Definite[Any]` here and seeing what happens
    /** A resolved [[ContractTypeId]], typed `CtTyId`. */
    type WithDefiniteOps[+CtId[_], Pkg] = CtId[Pkg] with Definite[Pkg] with Ops[CtId, Pkg]

    type ResolvedOf[+CtId[_]] = WithDefiniteOps[CtId, Ref.PackageRef]
    type ResolvedPkgIdOf[+CtId[_]] = WithDefiniteOps[CtId, Ref.PackageId]

    type Like[CtId[T] <: ContractTypeId[T]] = ContractTypeIdLike[CtId]

    // CtId serves the same role as `CC` on scala.collection.IterableOps
    sealed trait Ops[+CtId[_], +PkgId] {
      this: ContractTypeId[PkgId] =>
      def copy[PkgId0](
          packageId: PkgId0 = packageId,
          moduleName: String = moduleName,
          entityName: String = entityName,
      ): CtId[PkgId0] with Ops[CtId, PkgId0]
    }

    def withPkgRef[CtId[T] <: ContractTypeId[T]](
        id: CtId[Ref.PackageId] with Ops[CtId, Ref.PackageId]
    ): CtId[Ref.PackageRef] =
      id.copy(packageId = Ref.PackageRef.Id(id.packageId): Ref.PackageRef)
  }

  /** A contract type ID companion. */
  sealed abstract class ContractTypeIdLike[CtId[T] <: ContractTypeId[T]] {
    type RequiredPkg = CtId[Ref.PackageRef]
    type RequiredPkgId = CtId[Ref.PackageId]
    type ResolvedPkg = ContractTypeId.ResolvedOf[CtId]
    type ResolvedPkgId = ContractTypeId.ResolvedPkgIdOf[CtId]

    // treat the companion like a typeclass instance
    implicit def `ContractTypeIdLike companion`: this.type = this

    def apply[PkgId](
        packageId: PkgId,
        moduleName: String,
        entityName: String,
    ): CtId[PkgId] with ContractTypeId.Ops[CtId, PkgId]

    final def fromLedgerApi(in: lav2.value.Identifier): RequiredPkgId =
      apply(Ref.PackageId.assertFromString(in.packageId), in.moduleName, in.entityName)

    private[this] def qualifiedName(a: CtId[_]): Ref.QualifiedName =
      Ref.QualifiedName(
        Ref.DottedName.assertFromString(a.moduleName),
        Ref.DottedName.assertFromString(a.entityName),
      )
  }

  // Represents information about a contract type id that may use a name as the package reference.
  // i.e. `orig` may have a form or either "#foo:Bar:Baz" or "123:Bar:Baz".
  // If a package name is provided, then `allIds` may resolve to multiple values, for the relevant
  // template in each package id that shares the same package name.
  sealed abstract class ContractTypeRef[+CtTyId[T] <: ContractTypeId[T]](
      orig: ContractTypeId.ResolvedOf[CtTyId],
      ids: NonEmpty[Seq[ContractTypeId.ResolvedPkgIdOf[CtTyId]]],
      val name: Option[Ref.PackageName],
  ) {
    def allPkgIds: NonEmpty[Set[_ <: ContractTypeId.ResolvedPkgIdOf[CtTyId]]] = ids.toSet
    def latestPkgId: ContractTypeId.ResolvedPkgIdOf[CtTyId] = ids.head
    def original: ContractTypeId.ResolvedOf[CtTyId] = orig
  }

  object ContractTypeRef {
    type Resolved = ContractTypeRef[ContractTypeId]

    def unnamed[CtTyId[T] <: ContractTypeId[T]](
        id: ContractTypeId.ResolvedPkgIdOf[CtTyId]
    ): ContractTypeRef[CtTyId] = {
      val idWithRef = id.copy(packageId = Ref.PackageRef.Id(id.packageId): Ref.PackageRef)
      apply[CtTyId](idWithRef, NonEmpty(Seq, id.packageId), None)
    }

    def apply[CtId[T] <: ContractTypeId[T]](
        id: ContractTypeId.ResolvedOf[CtId], // The original template/interface id.
        pkgIds: NonEmpty[Seq[Ref.PackageId]], // Package ids with same name, by version, descending
        name: Option[Ref.PackageName], // The package name info
    ): ContractTypeRef[CtId] =
      ((id: ContractTypeId.Definite[Ref.PackageRef]) match {
        case t @ ContractTypeId.Template(_, _, _) => TemplateRef(t, pkgIds, name)
        case i @ ContractTypeId.Interface(_, _, _) => InterfaceRef(i, pkgIds, name)
      }).asInstanceOf[ContractTypeRef[CtId]] // CtId <: Definite, because id is resolved

    final case class TemplateRef(
        orig: ContractTypeId.Template.ResolvedPkg,
        pkgIds: NonEmpty[Seq[Ref.PackageId]],
        override val name: Option[Ref.PackageName],
    ) extends ContractTypeRef[ContractTypeId.Template](
          orig,
          pkgIds.map(pkgId => orig.copy(packageId = pkgId)),
          name,
        )

    final case class InterfaceRef(
        orig: ContractTypeId.Interface.ResolvedPkg,
        pkgIds: NonEmpty[Seq[Ref.PackageId]],
        override val name: Option[Ref.PackageName],
    ) extends ContractTypeRef[ContractTypeId.Interface](
          orig,
          pkgIds.map(pkgId => orig.copy(packageId = pkgId)),
          name,
        )
  }
}
