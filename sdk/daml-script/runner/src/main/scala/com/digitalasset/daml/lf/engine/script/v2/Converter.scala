// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package script
package v2

import com.daml.ledger.api.v2.event.{Event, ExercisedEvent}
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.javaapi.data.{Transaction => JavaTransaction}
import com.daml.ledger.javaapi.data.{ExercisedEvent => JavaExercisedEvent}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.ledger.api.util.LfEngineToApi.toApiIdentifier
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.engine.Result.lookupHandler
import com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction.ScriptLedgerClient
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.LookupError
import com.digitalasset.daml.lf.engine.ScriptEngine.ExtendedValue
import com.digitalasset.daml.lf.stablepackages.StablePackagesV2
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value._
import scala.jdk.CollectionConverters._
import scalaz.std.list._
import scalaz.std.either._
import scalaz.std.option._
import scalaz.syntax.traverse._

object Converter extends script.ConverterMethods(StablePackagesV2) {
  import com.digitalasset.daml.lf.script.converter.Converter._

  def translateTransactionTree(
      lookupChoice: (
          Identifier,
          Option[Identifier],
          ChoiceName,
      ) => Either[String, TemplateChoiceSignature],
      isKnownPackage: PackageId => Boolean,
      scriptIds: ScriptIds,
      tree: ScriptLedgerClient.TransactionTree,
  ): Either[String, ExtendedValue] = {
    def damlTree(s: String) =
      scriptIds.damlScriptModule("Daml.Script.Internal.Questions.TransactionTree", s)
    // Both `TreeEvent` and `Exercised` share the same stable package, for they are mutually recursive
    // Explicit builder for these types
    def damlTreeTreeEvent(s: String) =
      scriptIds.damlScriptModuleExplicit(
        "Daml.Script.Internal.Questions.TransactionTree",
        "Daml.Script.Internal.Questions.TransactionTree.Stable.TreeEvent",
        s,
      )
    def createdEvent(
        tplId: Identifier,
        contractId: ContractId,
        anyTemplate: ExtendedValue,
    ): ExtendedValue =
      ValueVariant(
        Some(damlTreeTreeEvent("TreeEvent")),
        Name.assertFromString("CreatedEvent"),
        record(
          damlTree("Created"),
          ("contractId", fromAnyContractId(scriptIds, toApiIdentifier(tplId), contractId)),
          ("argument", anyTemplate),
        ),
      )
    def exercisedEvent(
        tplId: Identifier,
        contractId: ContractId,
        choiceName: ChoiceName,
        anyChoice: Either[String, ExtendedValue],
        childEvents: List[ScriptLedgerClient.TreeEvent],
    ): Either[String, ExtendedValue] =
      for {
        evs <- childEvents.traverse(translateTreeEvent(_))
        anyChoice <- anyChoice
      } yield ValueVariant(
        Some(damlTreeTreeEvent("TreeEvent")),
        Name.assertFromString("ExercisedEvent"),
        record(
          damlTreeTreeEvent("Exercised"),
          ("contractId", fromAnyContractId(scriptIds, toApiIdentifier(tplId), contractId)),
          ("choice", ValueText(choiceName)),
          ("argument", anyChoice),
          ("childEvents", ValueList(evs.to(FrontStack))),
        ),
      )
    def translateTreeEvent(ev: ScriptLedgerClient.TreeEvent): Either[String, ExtendedValue] =
      ev match {
        case ScriptLedgerClient.Created(tplId, contractId, argument, _) =>
          Right(createdEvent(tplId, contractId, fromAnyTemplate(tplId, argument)))
        case ScriptLedgerClient.OpaqueCreated(tplId, contractId) =>
          fromOpaqueAnyTemplate(tplId, isKnownPackage).map(createdEvent(tplId, contractId, _))
        case ScriptLedgerClient.Exercised(
              tplId,
              ifaceId,
              contractId,
              choiceName,
              arg,
              _, // Result cannot be encoded in daml without some kind of `AnyChoiceResult` type, likely using the `Choice` constraint to unpack.
              childEvents,
            ) =>
          exercisedEvent(
            tplId,
            contractId,
            choiceName,
            fromAnyChoice(lookupChoice, tplId, ifaceId, choiceName, arg),
            childEvents,
          )
        case ScriptLedgerClient.OpaqueExercised(
              tplId,
              ifaceId,
              contractId,
              choiceName,
              childEvents,
            ) =>
          exercisedEvent(
            tplId,
            contractId,
            choiceName,
            fromOpaqueAnyChoice(tplId, ifaceId, choiceName, isKnownPackage),
            childEvents,
          )
      }
    for {
      events <- tree.rootEvents.traverse(translateTreeEvent(_)): Either[String, List[ExtendedValue]]
    } yield record(
      damlTree("TransactionTree"),
      ("rootEvents", ValueList(events.to(FrontStack))),
    )
  }

  def fromCommandResult(
      scriptIds: ScriptIds,
      commandResult: ScriptLedgerClient.CommandResult,
  ): ExtendedValue = {
    def scriptCommands(s: String) =
      scriptIds.damlScriptModule("Daml.Script.Internal.Questions.Commands", s)
    commandResult match {
      case ScriptLedgerClient.CreateResult(contractId) =>
        ValueVariant(
          Some(scriptCommands("CommandResult")),
          Ref.Name.assertFromString("CreateResult"),
          ValueContractId(contractId),
        )
      case r: ScriptLedgerClient.ExerciseResult =>
        ValueVariant(
          Some(scriptCommands("CommandResult")),
          Ref.Name.assertFromString("ExerciseResult"),
          r.result,
        )
    }
  }

  // Convert a Created event to a pair of (ContractId (), AnyTemplate)
  def fromCreated(
      contract: ScriptLedgerClient.ActiveContract,
      targetTemplateId: Identifier,
  ): ExtendedValue = {
    makeTuple(
      ValueContractId(contract.contractId),
      fromAnyTemplate(
        targetTemplateId,
        contract.argument,
      ),
    )
  }

  // Where an event sits in a transaction tree. A top-level event comes with the target of its
  // command.
  private sealed trait EventPosition
  private case object Nested extends EventPosition
  private final case class TopLevel(target: ScriptLedgerClient.CommandTarget) extends EventPosition

  def fromTransaction(
      tx: Transaction,
      commandTargets: List[ScriptLedgerClient.CommandTarget],
      isKnownPackage: PackageId => Boolean,
      isKnownTemplate: Identifier => Boolean,
      resolvePackageName: PackageName => Option[PackageId],
      enricher: Enricher,
  )(implicit traceContext: TraceContext): Either[String, ScriptLedgerClient.TransactionTree] = {
    def convEvent(
        ev: Int,
        position: EventPosition,
    ): Either[String, ScriptLedgerClient.TreeEvent] = {
      val javaTx = JavaTransaction.fromProto(Transaction.toJavaProto(tx))

      // The best approximation of `tplId` using a package known to the script, if any: `tplId`
      // itself if the script knows its package, else the same template in the version of its
      // package picked by `resolvePackageName` (i.e. `tplId` upgraded/downgraded), if that version
      // has the template.
      def resolveTplId(tplId: Identifier, pkgName: PackageName): Option[Identifier] =
        if (isKnownPackage(tplId.packageId)) Some(tplId)
        else
          resolvePackageName(pkgName)
            .map(pkgId => tplId.copy(pkg = pkgId))
            .filter(isKnownTemplate)

      // The approximation of `tplId` that an event is converted against, if any: for the
      // top-level event of a command on a template, the template in the version the script used
      // for the command; otherwise, the best approximation of `tplId`.
      def approximateTplId(tplId: Identifier, pkgName: PackageName): Option[Identifier] =
        position match {
          case TopLevel(ScriptLedgerClient.TemplateTarget(pkgId)) => Some(tplId.copy(pkg = pkgId))
          case TopLevel(ScriptLedgerClient.InterfaceTarget) | Nested =>
            resolveTplId(tplId, pkgName)
        }

      // Converts a value with `result` and builds the event from it with `build`. A nested event
      // may lack its choice or interface in the packages known to the script, or fail to be
      // upgraded/downgraded to the approximation of its template. Such an event is not a command
      // result, so rather than failing the submission, it becomes `opaque`.
      def enrichOrOpaque(
          result: Result[Value],
          upgradedOrDowngraded: Boolean,
          opaque: => ScriptLedgerClient.TreeEvent,
          build: Value => Either[String, ScriptLedgerClient.TreeEvent],
      ): Either[String, ScriptLedgerClient.TreeEvent] =
        result.consume(lookupHandler()) match {
          case Right(value) => build(value)
          case Left(Error.Preprocessing(Error.Preprocessing.Lookup(_: LookupError.NotFound)))
              if position == Nested =>
            Right(opaque)
          case Left(Error.Preprocessing(_: Error.Preprocessing.TypeMismatch))
              if position == Nested && upgradedOrDowngraded =>
            Right(opaque)
          case Left(err) => Left(err.toString)
        }

      // Only the results of top-level events become command results, so the results of nested
      // events are not converted.
      def enrichChoiceResult(
          exercised: ExercisedEvent,
          tplId: Identifier,
          ifaceId: Option[Identifier],
          choice: ChoiceName,
      ): Either[String, Option[Value]] =
        position match {
          case TopLevel(_) =>
            for {
              choiceResult <- NoLoggingValueValidator
                .validateValue(exercised.getExerciseResult)
                .left
                .map(_.toString)
              enrichedChoiceResult <- enricher
                .enrichChoiceResult(tplId, ifaceId, choice, choiceResult)
                .consume(lookupHandler())
                .left
                .map(_.toString)
            } yield Some(enrichedChoiceResult)
          case Nested => Right(None)
        }

      javaTx.getEventsById.asScala.get(ev).toRight(s"Event id $ev does not exist").flatMap {
        event =>
          Event.fromJavaProto(event.toProtoEvent).event match {
            case Event.Event.Created(created) =>
              for {
                tplId <- Converter.fromApiIdentifier(created.getTemplateId)
                cid <- ContractId.fromString(created.contractId)
                arg <-
                  NoLoggingValueValidator
                    .validateRecord(created.getCreateArguments)
                    .left
                    .map(err => s"Failed to validate create argument: $err")
                pkgName <- PackageName.fromString(created.packageName)
                event <- position match {
                  case TopLevel(ScriptLedgerClient.InterfaceTarget) =>
                    throw new RuntimeException(
                      s"Unexpected top-level create of $tplId for a command on an interface"
                    )
                  case _ =>
                    lazy val opaque = ScriptLedgerClient.OpaqueCreated(tplId, cid)
                    approximateTplId(tplId, pkgName) match {
                      // No approximation of the template is known to the script
                      case None => Right(opaque)
                      case Some(approxTplId) =>
                        enrichOrOpaque(
                          enricher.enrichContract(approxTplId, arg),
                          upgradedOrDowngraded = approxTplId != tplId,
                          opaque = opaque,
                          build = enrichedArg =>
                            Right(
                              ScriptLedgerClient.Created(
                                approxTplId,
                                cid,
                                enrichedArg,
                                Bytes.fromByteString(created.createdEventBlob),
                              )
                            ),
                        )
                    }
                }
              } yield event
            case Event.Event.Exercised(exercised) =>
              for {
                tplId <- Converter.fromApiIdentifier(exercised.getTemplateId)
                ifaceId <- exercised.interfaceId.traverse(Converter.fromApiIdentifier)
                cid <- ContractId.fromString(exercised.contractId)
                choice <- ChoiceName.fromString(exercised.choice)
                pkgName <- PackageName.fromString(exercised.packageName)
                choiceArg <- NoLoggingValueValidator
                  .validateValue(exercised.getChoiceArgument)
                  .left
                  .map(err => s"Failed to validate exercise argument: $err")
                childEvents <- javaTx
                  .getChildNodeIds(
                    JavaExercisedEvent.fromProto(ExercisedEvent.toJavaProto(exercised))
                  )
                  .asScala
                  .toList
                  .traverse(convEvent(_, Nested))
                // An exercise by interface is converted against the interface, which is never
                // upgraded/downgraded. Any other exercise is converted against its template.
                convertedAgainstTemplate = ifaceId.isEmpty
                event <- {
                  lazy val opaque =
                    ScriptLedgerClient.OpaqueExercised(tplId, ifaceId, cid, choice, childEvents)
                  (approximateTplId(tplId, pkgName), ifaceId) match {
                    // No approximation of the template is known to the script
                    case (None, None) => Right(opaque)
                    case (oApproxTplId, _) =>
                      // An exercise by interface keeps its real template id if it has no
                      // approximation: its argument is converted against the interface.
                      val eventTplId = oApproxTplId.getOrElse(tplId)
                      enrichOrOpaque(
                        enricher.enrichChoiceArgument(eventTplId, ifaceId, choice, choiceArg),
                        upgradedOrDowngraded = convertedAgainstTemplate && eventTplId != tplId,
                        opaque = opaque,
                        build = enrichedChoiceArg =>
                          enrichChoiceResult(exercised, eventTplId, ifaceId, choice).map(
                            ScriptLedgerClient.Exercised(
                              eventTplId,
                              ifaceId,
                              cid,
                              choice,
                              enrichedChoiceArg,
                              _,
                              childEvents,
                            )
                          ),
                      )
                  }
                }
              } yield event
            case Event.Event.Archived(_) =>
              throw new RuntimeException(
                "Unexpected archived event in transaction with LedgerEffects shape"
              )
            case Event.Event.Empty =>
              throw new RuntimeException("Unexpected empty event encountered in transaction")
          }
      }
    }
    for {
      rootEvents <- JavaTransaction
        .fromProto(Transaction.toJavaProto(tx))
        .getRootNodeIds()
        .asScala
        .toList
        .zip(commandTargets)
        .traverse { case (nodeId, target) =>
          convEvent(nodeId, TopLevel(target))
        }
    } yield {
      ScriptLedgerClient.TransactionTree(rootEvents)
    }
  }

  def toPackageId(v: ExtendedValue): Either[String, PackageId] =
    v match {
      case ValueRecord(_, ImmArray((_, ValueText(packageId)))) =>
        Right(PackageId.assertFromString(packageId))
      case _ => Left(s"Expected PackageId but got $v")
    }

  def toCommandWithMeta(
      v: ExtendedValue,
      lookupContractKeyType: Identifier => Either[String, Type],
      legacyAnyContractKey: Boolean = false,
  ): Either[String, ScriptLedgerClient.CommandWithMeta] =
    v match {
      // Pre-stable daml-script used explicit fields for commandWithMeta.
      case ValueRecord(_, ImmArray((_, command), (_, ValueBool(explicitPackageId)))) =>
        for {
          command <- toCommand(command, lookupContractKeyType, legacyAnyContractKey)
        } yield ScriptLedgerClient.CommandWithMeta(command, explicitPackageId)
      // Stable daml-script uses a LedgerValue map for metadata, for extensibility in the daml types
      case ValueRecord(_, ImmArray((_, command), (_, m @ ValueGenMap(entries)))) => {
        // Metadata mapping is `Map Text LedgerValue`
        val commandMetadata = entries.toList.map {
          case (ValueText(k), v) => (k, v)
          case _ => throw new RuntimeException(s"Expected Map Text LedgerValue but got $m")
        }.toMap
        val explicitPackageId = commandMetadata.get("explicitPackageId") match {
          case Some(ValueBool(b)) => b
          case _ => false // Default to upgrade compatible commands when not provided
        }
        for {
          // Stable daml-script does not support legacy AnyContractKey
          command <- toCommand(command, lookupContractKeyType, false)
        } yield ScriptLedgerClient.CommandWithMeta(command, explicitPackageId)
      }
      case _ => Left(s"Expected CommandWithMeta but got $v")
    }

  def castCommandExtendedValue(value: ExtendedValue): Either[String, Value] =
    castExtendedValue(value).left.map(_.getMessage)

  def toCommand(
      v: ExtendedValue,
      lookupContractKeyType: Identifier => Either[String, Type],
      legacyAnyContractKey: Boolean = false,
  ): Either[String, command.ApiCommand] =
    v match {
      case ValueVariant(_, "Create", ValueRecord(_, ImmArray((_, anyTemplateSValue)))) =>
        for {
          anyTemplate <- toAnyTemplate(anyTemplateSValue)
          argument <- castCommandExtendedValue(anyTemplate.arg)
        } yield command.ApiCommand.Create(
          templateRef = anyTemplate.ty.toRef,
          argument = argument,
        )
      case ValueVariant(
            _,
            "Exercise",
            ValueRecord(_, ImmArray((_, tIdSValue), (_, cIdSValue), (_, anyChoiceSValue))),
          ) =>
        for {
          typeId <- typeRepToIdentifier(tIdSValue)
          cid <- toContractId(cIdSValue)
          anyChoice <- toAnyChoice(anyChoiceSValue)
          argument <- castCommandExtendedValue(anyChoice.arg)
        } yield command.ApiCommand.Exercise(
          typeRef = typeId.toRef,
          contractId = cid,
          choiceId = anyChoice.name,
          argument = argument,
        )
      case ValueVariant(
            _,
            "ExerciseByKey",
            ValueRecord(_, ImmArray((_, tIdSValue), (_, anyKeySValue), (_, anyChoiceSValue))),
          ) =>
        for {
          typeId <- typeRepToIdentifier(tIdSValue)
          anyKey <- toAnyContractKey(anyKeySValue, lookupContractKeyType, legacyAnyContractKey)
          contractKey <- castCommandExtendedValue(anyKey.key)
          anyChoice <- toAnyChoice(anyChoiceSValue)
          argument <- castCommandExtendedValue(anyChoice.arg)
        } yield command.ApiCommand.ExerciseByKey(
          templateRef = typeId.toRef,
          contractKey = contractKey,
          choiceId = anyChoice.name,
          argument = argument,
        )
      case ValueVariant(
            _,
            "CreateAndExercise",
            ValueRecord(_, ImmArray((_, anyTemplateSValue), (_, anyChoiceSValue))),
          ) =>
        for {
          anyTemplate <- toAnyTemplate(anyTemplateSValue)
          createArgument <- castCommandExtendedValue(anyTemplate.arg)
          anyChoice <- toAnyChoice(anyChoiceSValue)
          choiceArgument <- castCommandExtendedValue(anyChoice.arg)
        } yield command.ApiCommand.CreateAndExercise(
          templateRef = anyTemplate.ty.toRef,
          createArgument = createArgument,
          choiceId = anyChoice.name,
          choiceArgument = choiceArgument,
        )
      case _ => Left(s"Expected command but got $v")
    }

  // Encodes as Daml.Script.Internal.Questions.Packages.PackageName
  def fromReadablePackageId(
      scriptIds: ScriptIds,
      packageName: ScriptLedgerClient.ReadablePackageId,
  ): ExtendedValue = {
    val packageNameTy =
      scriptIds.damlScriptModule("Daml.Script.Internal.Questions.Packages", "PackageName")
    record(
      packageNameTy,
      ("name", ValueText(packageName.name.toString)),
      ("version", ValueText(packageName.version.toString)),
    )
  }
}
