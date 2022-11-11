// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.participant.util

import java.time.Instant

import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event, ExercisedEvent}
import com.daml.ledger.api.v1.transaction.TreeEvent
import com.daml.ledger.api.v1.{value => api}
import com.daml.lf.data.LawlessTraversals._
import com.daml.lf.data.Ref.Identifier
import com.daml.lf.data.{Numeric, Ref}
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.{Node, NodeId}
import com.daml.lf.value.{Value => Lf}
import com.google.protobuf.empty.Empty
import com.google.protobuf.timestamp.Timestamp

/** Translates [[com.daml.lf.value.Value]] values to [[com.daml.ledger.api.v1.value]] values.
  *
  * All conversion functions are pure and total.
  *
  * Most conversion functions have a verbose flag:
  * - If verbose mode is disabled, then all resulting Api values have missing type identifiers and record field names.
  * - If verbose mode is enabled, then type identifiers and record field names are copied from the input Daml-LF values.
  *   The caller is responsible for filling in missing type information using [[com.daml.lf.engine.ValueEnricher]],
  *   which may involve loading Daml-LF packages.
  */
object LfEngineToApi {

  private[this] type LfValue = Lf

  def toApiIdentifier(identifier: Identifier): api.Identifier = {
    api.Identifier(
      identifier.packageId,
      identifier.qualifiedName.module.toString(),
      identifier.qualifiedName.name.toString(),
    )
  }

  def toTimestamp(instant: Instant): Timestamp = {
    Timestamp.apply(instant.getEpochSecond, instant.getNano)
  }

  def lfValueToApiRecord(
      verbose: Boolean,
      recordValue: LfValue,
  ): Either[String, api.Record] = {
    recordValue match {
      case Lf.ValueRecord(tycon, fields) =>
        val fs = fields.foldLeft[Either[String, Vector[api.RecordField]]](Right(Vector.empty)) {
          case (Left(e), _) => Left(e)
          case (Right(acc), (mbLabel, value)) =>
            lfValueToApiValue(verbose, value)
              .map(v => api.RecordField(if (verbose) mbLabel.getOrElse("") else "", Some(v)))
              .map(acc :+ _)
        }
        val mbId = if (verbose) {
          tycon.map(toApiIdentifier)
        } else {
          None
        }

        fs.map(api.Record(mbId, _))
      case other =>
        Left(s"Expected value to be record, but got $other")
    }

  }
  def lfValueToApiValue(
      verbose: Boolean,
      lf: Option[LfValue],
  ): Either[String, Option[api.Value]] =
    lf.fold[Either[String, Option[api.Value]]](Right(None))(
      lfValueToApiValue(verbose, _).map(Some(_))
    )

  def lfValueToApiValue(
      verbose: Boolean,
      value0: LfValue,
  ): Either[String, api.Value] =
    value0 match {
      case Lf.ValueUnit => Right(api.Value(api.Value.Sum.Unit(Empty())))
      case Lf.ValueNumeric(d) =>
        Right(api.Value(api.Value.Sum.Numeric(Numeric.toString(d))))
      case Lf.ValueContractId(c) => Right(api.Value(api.Value.Sum.ContractId(c.coid)))
      case Lf.ValueBool(b) => Right(api.Value(api.Value.Sum.Bool(b)))
      case Lf.ValueDate(d) => Right(api.Value(api.Value.Sum.Date(d.days)))
      case Lf.ValueTimestamp(t) => Right(api.Value(api.Value.Sum.Timestamp(t.micros)))
      case Lf.ValueInt64(i) => Right(api.Value(api.Value.Sum.Int64(i)))
      case Lf.ValueParty(p) => Right(api.Value(api.Value.Sum.Party(p)))
      case Lf.ValueText(t) => Right(api.Value(api.Value.Sum.Text(t)))
      case Lf.ValueOptional(o) => // TODO DEL-7054 add test coverage
        o.fold[Either[String, api.Value]](
          Right(api.Value(api.Value.Sum.Optional(api.Optional.defaultInstance)))
        )(v =>
          lfValueToApiValue(verbose, v).map(c =>
            api.Value(api.Value.Sum.Optional(api.Optional(Some(c))))
          )
        )
      case Lf.ValueTextMap(m) =>
        m.toImmArray.reverse
          .foldLeft[Either[String, List[api.Map.Entry]]](Right(List.empty)) {
            case (Right(list), (k, v)) =>
              lfValueToApiValue(verbose, v).map(w => api.Map.Entry(k, Some(w)) :: list)
            case (left, _) => left
          }
          .map(list => api.Value(api.Value.Sum.Map(api.Map(list))))
      case Lf.ValueGenMap(entries) =>
        entries.reverseIterator
          .foldLeft[Either[String, List[api.GenMap.Entry]]](Right(List.empty)) {
            case (acc, (k, v)) =>
              for {
                tail <- acc
                key <- lfValueToApiValue(verbose, k)
                value <- lfValueToApiValue(verbose, v)
              } yield api.GenMap.Entry(Some(key), Some(value)) :: tail
          }
          .map(list => api.Value(api.Value.Sum.GenMap(api.GenMap(list))))
      case Lf.ValueList(vs) =>
        vs.toImmArray.toSeq.traverseEitherStrictly(lfValueToApiValue(verbose, _)) map { xs =>
          api.Value(api.Value.Sum.List(api.List(xs)))
        }
      case Lf.ValueVariant(tycon, variant, v) =>
        lfValueToApiValue(verbose, v) map { x =>
          api.Value(
            api.Value.Sum.Variant(
              api.Variant(
                tycon.filter(_ => verbose).map(toApiIdentifier),
                variant,
                Some(x),
              )
            )
          )
        }
      case Lf.ValueEnum(tyCon, value) =>
        Right(
          api.Value(
            api.Value.Sum.Enum(
              api.Enum(
                tyCon.filter(_ => verbose).map(toApiIdentifier),
                value,
              )
            )
          )
        )
      case Lf.ValueRecord(tycon, fields) =>
        fields.toSeq.traverseEitherStrictly { field =>
          lfValueToApiValue(verbose, field._2) map { x =>
            api.RecordField(
              if (verbose)
                field._1.getOrElse("")
              else
                "",
              Some(x),
            )
          }
        } map { apiFields =>
          api.Value(
            api.Value.Sum.Record(
              api.Record(
                if (verbose)
                  tycon.map(toApiIdentifier)
                else
                  None,
                apiFields,
              )
            )
          )
        }
      case _: Lf.ValueAny =>
        lfValueToApiValue(verbose, Lf.ValueUnit)
    }

  def lfContractKeyToApiValue(
      verbose: Boolean,
      lf: Node.KeyWithMaintainers,
  ): Either[String, api.Value] =
    lfValueToApiValue(verbose, lf.key)

  def lfContractKeyToApiValue(
      verbose: Boolean,
      lf: Option[Node.KeyWithMaintainers],
  ): Either[String, Option[api.Value]] =
    lf.fold[Either[String, Option[api.Value]]](Right(None))(
      lfContractKeyToApiValue(verbose, _).map(Some(_))
    )

  def lfNodeCreateToEvent(
      verbose: Boolean,
      trId: Ref.LedgerString,
      nodeId: NodeId,
      node: Node.Create,
  ): Either[String, Event] =
    for {
      arg <- lfValueToApiRecord(verbose, node.arg)
      key <- lfContractKeyToApiValue(verbose, node.key)
    } yield Event(
      Event.Event.Created(
        CreatedEvent(
          eventId = EventId(trId, nodeId).toLedgerString,
          contractId = node.coid.coid,
          templateId = Some(toApiIdentifier(node.templateId)),
          contractKey = key,
          createArguments = Some(arg),
          witnessParties = node.stakeholders.toSeq,
          signatories = node.signatories.toSeq,
          observers = node.stakeholders.diff(node.signatories).toSeq,
          agreementText = Some(node.agreementText),
        )
      )
    )

  def lfNodeExercisesToEvent(
      trId: Ref.LedgerString,
      nodeId: NodeId,
      node: Node.Exercise,
  ): Either[String, Event] =
    Either.cond(
      node.consuming,
      Event(
        Event.Event.Archived(
          ArchivedEvent(
            eventId = EventId(trId, nodeId).toLedgerString,
            contractId = node.targetCoid.coid,
            templateId = Some(toApiIdentifier(node.templateId)),
            witnessParties = node.stakeholders.toSeq,
          )
        )
      ),
      "illegal conversion of non-consuming exercise to archived event",
    )

  def lfNodeCreateToTreeEvent(
      verbose: Boolean,
      eventId: EventId,
      witnessParties: Set[Ref.Party],
      node: Node.Create,
  ): Either[String, TreeEvent] =
    for {
      arg <- lfValueToApiRecord(verbose, node.arg)
      key <- lfContractKeyToApiValue(verbose, node.key)
    } yield TreeEvent(
      TreeEvent.Kind.Created(
        CreatedEvent(
          eventId = eventId.toLedgerString,
          contractId = node.coid.coid,
          templateId = Some(toApiIdentifier(node.templateId)),
          contractKey = key,
          createArguments = Some(arg),
          witnessParties = witnessParties.toSeq,
          signatories = node.signatories.toSeq,
          observers = node.stakeholders.diff(node.signatories).toSeq,
          agreementText = Some(node.agreementText),
        )
      )
    )

  def lfNodeExercisesToTreeEvent(
      verbose: Boolean,
      trId: Ref.LedgerString,
      eventId: EventId,
      witnessParties: Set[Ref.Party],
      node: Node.Exercise,
      filterChildren: NodeId => Boolean,
  ): Either[String, TreeEvent] =
    for {
      arg <- lfValueToApiValue(verbose, node.chosenValue)
      result <- lfValueToApiValue(verbose, node.exerciseResult)
    } yield {
      TreeEvent(
        TreeEvent.Kind.Exercised(
          ExercisedEvent(
            eventId = eventId.toLedgerString,
            contractId = node.targetCoid.coid,
            templateId = Some(toApiIdentifier(node.templateId)),
            interfaceId = node.interfaceId.map(toApiIdentifier),
            choice = node.choiceId,
            choiceArgument = Some(arg),
            actingParties = node.actingParties.toSeq,
            consuming = node.consuming,
            witnessParties = witnessParties.toSeq,
            childEventIds = node.children.iterator
              .filter(filterChildren)
              .map(EventId(trId, _).toLedgerString)
              .toSeq,
            exerciseResult = result,
          )
        )
      )
    }

  @throws[RuntimeException]
  def assertOrRuntimeEx[A](failureContext: String, ea: Either[String, A]): A =
    ea.fold(e => throw new RuntimeException(s"Unexpected error when $failureContext: $e"), identity)

}
