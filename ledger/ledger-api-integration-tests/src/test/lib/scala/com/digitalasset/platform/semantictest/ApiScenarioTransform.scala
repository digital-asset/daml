// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.semantictest

import com.digitalasset.daml.lf.data.Ref.{PackageId, QualifiedName}
import com.digitalasset.daml.lf.data.{BackStack, ImmArray, Ref}
import com.digitalasset.daml.lf.engine.CreateEvent
import com.digitalasset.daml.lf.lfpackage.Ast
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, VersionedValue}
import com.digitalasset.daml.lf.value.{Value, ValueVersions}
import com.digitalasset.ledger.api.v1.event.{
  CreatedEvent => ApiCreatedEvent,
  ExercisedEvent => ApiExercisedEvent
}
import com.digitalasset.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.digitalasset.ledger.api.v1.value.{Record, Value => ApiValue}
import com.digitalasset.ledger.api.validation.CommandSubmissionRequestValidator
import com.digitalasset.platform.common.{PlatformTypes => P}
import com.digitalasset.platform.server.api.validation.{ErrorFactories, IdentifierResolver}
import io.grpc.StatusRuntimeException
import scalaz.Traverse
import scalaz.std.either._
import scalaz.std.list._

import scala.concurrent.Future
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any"
  ))
class ApiScenarioTransform(ledgerId: String, packages: Map[Ref.PackageId, Ast.Package])
    extends ErrorFactories {

  private def toLfVersionedValue[Cid](
      record: Record): Either[StatusRuntimeException, VersionedValue[AbsoluteContractId]] = {
    recordToLfValue(record).flatMap(determineVersion)
  }

  private def toLfVersionedValue[Cid](
      value: ApiValue): Either[StatusRuntimeException, VersionedValue[AbsoluteContractId]] = {
    toLfValue(value).flatMap(determineVersion)
  }

  private def determineVersion(value: Value[AbsoluteContractId])
    : Either[StatusRuntimeException, VersionedValue[AbsoluteContractId]] =
    ValueVersions
      .asVersionedValue(value)
      .fold[Either[StatusRuntimeException, VersionedValue[AbsoluteContractId]]](
        s => Left(invalidArgument(s"Cannot parse '$value' as versioned value: $s")),
        Right.apply)

  private val validator =
    new CommandSubmissionRequestValidator(
      ledgerId,
      IdentifierResolver(_ => Future.successful(None)))

  private def recordToLfValue[Cid](record: Record) =
    toLfValue(ApiValue(ApiValue.Sum.Record(record)))

  private def toLfValue[Cid](
      apiV: ApiValue): Either[StatusRuntimeException, Value[AbsoluteContractId]] =
    validator.validateValue(apiV)

  // this is roughly the inverse operation of EventConverter in sandbox
  def eventsFromApiTransaction(transactionTree: TransactionTree)
    : Either[RuntimeException, P.Events[String, AbsoluteContractId]] = {
    case class ExerciseWaiting(
        index: Int,
        requiredChildrenNum: Int,
        childIndices: BackStack[String],
        currentLength: Int,
        e: P.ExerciseEvent[String, AbsoluteContractId]) {
      def update(childIndex: String): ExerciseWaiting = copy(
        childIndices = this.childIndices :+ childIndex,
        currentLength = this.currentLength + 1
      )

      def getEvent: P.ExerciseEvent[String, AbsoluteContractId] =
        if (currentLength == requiredChildrenNum) {
          e.copy(children = childIndices.toImmArray)
        } else {
          sys.error("getEvent called in invalid state on incomplete exercise")
        }
    }

    def toLfCreated(p: Ast.Package, createdEvent: ApiCreatedEvent)
      : Either[StatusRuntimeException, P.CreateEvent[AbsoluteContractId]] = {
      val witnesses = P.parties(createdEvent.witnessParties)
      toLfVersionedValue(createdEvent.getCreateArguments).map { value =>
        P.CreateEvent(
          AbsoluteContractId(createdEvent.contractId),
          Ref.Identifier(
            P.packageId(createdEvent.getTemplateId.packageId),
            QualifiedName(
              P.mn(createdEvent.getTemplateId.moduleName),
              P.dn(createdEvent.getTemplateId.entityName))
          ),
          value,
          // conversion is imperfect as stakeholders are not determinable from events yet
          witnesses,
          witnesses
        )
      }
    }

    def toLfExercised[EventId](
        p: Ast.Package,
        exercisedEvent: ApiExercisedEvent,
        convertEvId: String => EventId)
      : Either[StatusRuntimeException, P.ExerciseEvent[EventId, AbsoluteContractId]] = {
      val witnesses = P.parties(exercisedEvent.witnessParties)
      toLfVersionedValue(exercisedEvent.getChoiceArgument).map { value =>
        P.ExerciseEvent(
          AbsoluteContractId(exercisedEvent.contractId),
          Ref.Identifier(
            P.packageId(exercisedEvent.getTemplateId.packageId),
            QualifiedName(
              Ref.ModuleName.fromString(exercisedEvent.getTemplateId.moduleName).right.get,
              Ref.DottedName.fromString(exercisedEvent.getTemplateId.entityName).right.get
            )
          ),
          exercisedEvent.choice,
          value,
          P.parties(exercisedEvent.actingParties),
          exercisedEvent.consuming,
          ImmArray(exercisedEvent.childEventIds.map(convertEvId)),
          // conversion is imperfect as stakeholders are not determinable from events yet
          witnesses,
          witnesses
        )
      }
    }
    val converted: Either[RuntimeException, Map[String, P.Event[String, AbsoluteContractId]]] =
      Traverse[List]
        .traverseU(transactionTree.eventsById.toList)({
          case (k, treeEvent) =>
            val either = treeEvent match {
              case TreeEvent(TreeEvent.Kind.Empty) =>
                Left(new RuntimeException(s"Received empty TreeEvent at key '$k'"))
              case TreeEvent(TreeEvent.Kind.Created(created)) =>
                toLfCreated(packages(P.packageId(created.getTemplateId.packageId)), created)
              case TreeEvent(TreeEvent.Kind.Exercised(exercised)) =>
                toLfExercised(
                  packages(P.packageId(exercised.getTemplateId.packageId)),
                  exercised,
                  identity)
            }
            either.map((k, _))
        })
        .map(_.toMap)

    val roots = ImmArray(transactionTree.rootEventIds)

    converted.map(P.Events(roots, _))
  }

  def lfCreatedFromApiEvent(createdEvent: ApiCreatedEvent): Either[
    StatusRuntimeException,
    CreateEvent[AbsoluteContractId, VersionedValue[AbsoluteContractId]]] = {
    val witnesses = P.parties(createdEvent.witnessParties)
    validator
      .validateValue(ApiValue(ApiValue.Sum.Record(createdEvent.getCreateArguments)))
      .map { value =>
        P.CreateEvent(
          AbsoluteContractId(createdEvent.contractId),
          Ref.Identifier(
            P.packageId(createdEvent.getTemplateId.packageId),
            Ref.QualifiedName(
              P.mn(createdEvent.getTemplateId.moduleName),
              P.dn(createdEvent.getTemplateId.entityName))
          ),
          P.asVersionedValue(value)
            .getOrElse(sys.error("can't convert create event")),
          witnesses,
          witnesses
        )
      }
  }
}

object ApiScenarioTransform {
  def apply(ledgerId: String, packages: Map[PackageId, Ast.Package]): ApiScenarioTransform =
    new ApiScenarioTransform(ledgerId, packages)
}
