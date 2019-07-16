// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.semantictest

import com.digitalasset.daml.lf.data.Ref.{ChoiceName, ContractIdString, PackageId, QualifiedName}
import com.digitalasset.daml.lf.data.{BackStack, ImmArray, Ref}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.transaction.Node.KeyWithMaintainers
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, VersionedValue}
import com.digitalasset.daml.lf.value.{Value, ValueVersions}
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.event.{
  CreatedEvent => ApiCreatedEvent,
  ExercisedEvent => ApiExercisedEvent
}
import com.digitalasset.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.digitalasset.ledger.api.v1.value.{Record, Value => ApiValue}
import com.digitalasset.ledger.api.validation.CommandsValidator
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

  private def toContractId(s: String): Either[StatusRuntimeException, ContractIdString] =
    ContractIdString.fromString(s).left.map(e => invalidArgument(s"Cannot parse contractId: $e"))

  private def toLfVersionedValue[Cid](
      record: Record
  ): Either[StatusRuntimeException, VersionedValue[AbsoluteContractId]] =
    recordToLfValue(record).flatMap(determineVersion)

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

  private val commandsValidator =
    new CommandsValidator(
      LedgerId(ledgerId),
      IdentifierResolver(_ => Future.successful(None))
    )

  private def recordToLfValue[Cid](record: Record) =
    toLfValue(ApiValue(ApiValue.Sum.Record(record)))

  private def toLfValue[Cid](
      apiV: ApiValue): Either[StatusRuntimeException, Value[AbsoluteContractId]] =
    commandsValidator.validateValue(apiV)

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
      val signatories = P.parties(createdEvent.signatories)
      val observers = P.parties(createdEvent.observers)
      for {
        coid <- toContractId(createdEvent.contractId)
        value <- toLfVersionedValue(createdEvent.getCreateArguments)
      } yield
        P.CreateEvent(
          AbsoluteContractId(coid),
          Ref.Identifier(
            P.packageId(createdEvent.getTemplateId.packageId),
            QualifiedName(
              P.mn(createdEvent.getTemplateId.moduleName),
              P.dn(createdEvent.getTemplateId.entityName))
          ),
          // conversion is imperfect as maintainers are not determinable from events yet
          createdEvent.contractKey.map(key =>
            toLfVersionedValue(key).fold(throw _, KeyWithMaintainers(_, Set.empty))),
          value,
          createdEvent.agreementText.getOrElse(""),
          signatories,
          observers,
          witnesses
        )
    }

    def toLfExercised[EventId](
        p: Ast.Package,
        exercisedEvent: ApiExercisedEvent,
        convertEvId: String => EventId)
      : Either[StatusRuntimeException, P.ExerciseEvent[EventId, AbsoluteContractId]] = {
      val witnesses = P.parties(exercisedEvent.witnessParties)
      for {
        coid <- toContractId(exercisedEvent.contractId)
        value <- toLfVersionedValue(exercisedEvent.getChoiceArgument)
        result <- toLfVersionedValue(exercisedEvent.getExerciseResult)
      } yield {
        P.ExerciseEvent(
          AbsoluteContractId(coid),
          Ref.Identifier(
            P.packageId(exercisedEvent.getTemplateId.packageId),
            QualifiedName(
              Ref.ModuleName.fromString(exercisedEvent.getTemplateId.moduleName).right.get,
              Ref.DottedName.fromString(exercisedEvent.getTemplateId.entityName).right.get
            )
          ),
          ChoiceName.assertFromString(exercisedEvent.choice),
          value,
          P.parties(exercisedEvent.actingParties),
          exercisedEvent.consuming,
          ImmArray(exercisedEvent.childEventIds.map(convertEvId)),
          // conversion is imperfect as stakeholders are not determinable from events yet
          witnesses,
          witnesses,
          Some(result)
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
}

object ApiScenarioTransform {
  def apply(ledgerId: String, packages: Map[PackageId, Ast.Package]): ApiScenarioTransform =
    new ApiScenarioTransform(ledgerId, packages)
}
