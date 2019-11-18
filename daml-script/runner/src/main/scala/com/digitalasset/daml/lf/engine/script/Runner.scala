// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.typesafe.scalalogging.StrictLogging
import io.grpc.StatusRuntimeException
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scalaz.{\/-}
import scalaz.std.either._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import spray.json._

import com.digitalasset.daml.lf.PureCompiledPackages
import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.data.FrontStack
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.engine.ValueTranslator
import com.digitalasset.daml.lf.iface
import com.digitalasset.daml.lf.iface.EnvironmentInterface
import com.digitalasset.daml.lf.iface.reader.InterfaceReader
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.{Compiler, Pretty, Speedy, SValue, TraceLog}
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.daml.lf.value.json.ApiCodecCompressed
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.refinements.ApiTypes.ApplicationId
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.commands._
import com.digitalasset.ledger.api.v1.transaction_filter.{
  Filters,
  TransactionFilter,
  InclusiveFilters
}
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.services.commands.CommandUpdater

object LfValueCodec extends ApiCodecCompressed[AbsoluteContractId](false, false) {
  override final def apiContractIdToJsValue(obj: AbsoluteContractId) =
    JsString(obj.coid)
  override final def jsValueToApiContractId(json: JsValue) = json match {
    case JsString(s) =>
      ContractIdString.fromString(s).fold(deserializationError(_), AbsoluteContractId)
    case _ => deserializationError("ContractId must be a string")
  }
}

class Runner(
    dar: Dar[(PackageId, Package)],
    applicationId: ApplicationId,
    commandUpdater: CommandUpdater)
    extends StrictLogging {

  val ifaceDar = dar.map(pkg => InterfaceReader.readInterface(() => \/-(pkg))._2)

  val envIface = EnvironmentInterface.fromReaderInterfaces(ifaceDar)
  def damlLfTypeLookup(id: Identifier): Option[iface.DefDataType.FWT] =
    envIface.typeDecls.get(id).map(_.`type`)

  val darMap: Map[PackageId, Package] = dar.all.toMap
  val compiler = Compiler(darMap)
  val scriptModuleName = DottedName.assertFromString("Daml.Script")
  val scriptPackageId: PackageId = dar.all
    .find {
      case (pkgId, pkg) => pkg.modules.contains(scriptModuleName)
    }
    .get
    ._1
  val scriptTyCon = Identifier(
    scriptPackageId,
    QualifiedName(scriptModuleName, DottedName.assertFromString("Script")))
  val stdlibPackageId =
    dar.all
      .find {
        case (pkgId, pkg) =>
          pkg.modules.contains(DottedName.assertFromString("DA.Internal.LF"))
      }
      .get
      ._1
  def lookupChoiceTy(id: Identifier, choice: Name): Either[String, Type] =
    for {
      pkg <- darMap
        .get(id.packageId)
        .fold[Either[String, Package]](Left(s"Failed to find package ${id.packageId}"))(Right(_))
      module <- pkg.modules
        .get(id.qualifiedName.module)
        .fold[Either[String, Module]](Left(s"Failed to find module ${id.qualifiedName.module}"))(
          Right(_))
      definition <- module.definitions
        .get(id.qualifiedName.name)
        .fold[Either[String, Definition]](Left(s"Failed to find ${id.qualifiedName.name}"))(
          Right(_))
      tpl <- definition match {
        case DDataType(_, _, DataRecord(_, Some(tpl))) => Right(tpl)
        case _ => Left(s"Expected template definition but got $definition")
      }
      choice <- tpl.choices
        .get(choice)
        .fold[Either[String, TemplateChoice]](Left(s"Failed to find choice $choice in $id"))(
          Right(_))
    } yield choice.returnType

  // We overwrite the definition of toLedgerValue with an identity function.
  // This is a type error but Speedy doesn’t care about the types and the only thing we do
  // with the result is convert it to ledger values/record so this is safe.
  val definitionMap =
    compiler.compilePackages(darMap.keys) +
      (LfDefRef(
        Identifier(
          scriptPackageId,
          QualifiedName(scriptModuleName, DottedName.assertFromString("fromLedgerValue")))) ->
        SEMakeClo(Array(), 1, SEVar(1)))
  val compiledPackages = PureCompiledPackages(darMap, definitionMap).right.get
  val valueTranslator = new ValueTranslator(compiledPackages)

  def toSubmitRequest(ledgerId: LedgerId, party: SParty, cmds: Seq[Command]) = {
    val commands = Commands(
      party = party.value,
      commands = cmds,
      ledgerId = ledgerId.unwrap,
      applicationId = applicationId.unwrap,
      commandId = UUID.randomUUID.toString,
      ledgerEffectiveTime = None,
      maximumRecordTime = None,
    )
    SubmitAndWaitRequest(Some(commandUpdater.applyOverrides(commands)))
  }

  def run(client: LedgerClient, scriptId: Identifier, inputValue: Option[JsValue])(
      implicit ec: ExecutionContext,
      mat: ActorMaterializer): Future[SValue] = {
    val scriptTy = darMap
      .get(scriptId.packageId)
      .flatMap(_.lookupIdentifier(scriptId.qualifiedName).toOption) match {
      case Some(DValue(ty, _, _, _)) => ty
      case Some(d @ DDataType(_, _, _)) =>
        throw new RuntimeException(s"Expected DAML script but got datatype $d")
      case None => throw new RuntimeException(s"Could not find DAML script $scriptId")
    }
    def assertScriptTy(ty: Type) = {
      ty match {
        case TApp(TTyCon(tyCon), _) if tyCon == scriptTyCon => {}
        case _ => throw new RuntimeException(s"Expected type 'Script a' but got $ty")
      }
    }
    val scriptExpr = inputValue match {
      case None => {
        assertScriptTy(scriptTy)
        SEVal(LfDefRef(scriptId), None)
      }
      case Some(inputJson) =>
        scriptTy match {
          case TApp(TApp(TBuiltin(BTArrow), param), result) => {
            assertScriptTy(result)
            val paramIface = Converter.toIfaceType(param) match {
              case Left(s) => throw new ConverterException(s"Failed to convert $result: $s")
              case Right(ty) => ty
            }
            val inputLfVal = inputJson.convertTo[Value[AbsoluteContractId]](
              LfValueCodec.apiValueJsonReader(paramIface, damlLfTypeLookup(_)))
            SEApp(SEVal(LfDefRef(scriptId), None), Array(SEValue(SValue.fromValue(inputLfVal))))
          }
          case _ =>
            throw new RuntimeException(
              s"Expected $scriptId to have function type but got $scriptTy")
        }

    }
    var machine =
      Speedy.Machine.fromSExpr(scriptExpr, false, compiledPackages)

    def stepToValue() = {
      while (!machine.isFinal) {
        machine.step() match {
          case SResultContinue => ()
          case SResultError(err) => {
            logger.error(Pretty.prettyError(err, machine.ptx).render(80))
            throw err
          }
          case res => {
            throw new RuntimeException(s"Unexpected speedy result $res")
          }
        }
      }
      // TODO Share this logic with the trigger runner
      var traceEmpty = true
      machine.traceLog.iterator.foreach {
        case (msg, optLoc) =>
          traceEmpty = false
          println(s"TRACE ${Pretty.prettyLoc(optLoc).render(80)}: $msg")
      }
      if (!traceEmpty) {
        machine = machine.copy(traceLog = TraceLog(machine.traceLog.capacity))
      }
    }

    stepToValue()
    machine.toSValue match {
      // Unwrap Script newtype
      case SRecord(_, _, vals) if vals.size == 1 => {
        machine.ctrl = Speedy.CtrlExpr(SEValue(vals.get(0)))
      }
      case v => throw new ConverterException(s"Expected record with 1 field but got $v")
    }

    def go(): Future[SValue] = {
      stepToValue()
      machine.toSValue match {
        case SVariant(_, "Free", v) => {
          v match {
            case SVariant(_, "Submit", v) => {
              v match {
                case SRecord(_, _, vals) if vals.size == 3 => {
                  val freeAp = vals.get(1) match {
                    // Unwrap Commands newtype
                    case SRecord(_, _, vals) if vals.size == 1 => vals.get(0)
                    case v =>
                      throw new ConverterException(s"Expected record with 1 field but got $v")
                  }
                  val requestOrErr = for {
                    party <- Converter.toParty(vals.get(0))
                    commands <- Converter.toCommands(compiledPackages, freeAp)
                  } yield toSubmitRequest(client.ledgerId, party, commands)
                  val request = requestOrErr.fold(s => throw new ConverterException(s), identity)
                  val f =
                    client.commandServiceClient
                      .submitAndWaitForTransactionTree(request)
                      .map(Right(_))
                      .recover({ case s: StatusRuntimeException => Left(s) })
                  f.flatMap({
                    case Right(transactionTree) => {
                      val events =
                        transactionTree.getTransaction.rootEventIds.map(evId =>
                          transactionTree.getTransaction.eventsById(evId))
                      val filled =
                        Converter.fillCommandResults(
                          compiledPackages,
                          lookupChoiceTy,
                          valueTranslator,
                          freeAp,
                          events) match {
                          case Left(s) => throw new ConverterException(s)
                          case Right(r) => r
                        }
                      machine.ctrl = Speedy.CtrlExpr(filled)
                      go()
                    }
                    case Left(statusEx) => {
                      val res = Converter
                        .fromStatusException(scriptPackageId, statusEx)
                        .fold(s => throw new ConverterException(s), identity)
                      machine.ctrl =
                        Speedy.CtrlExpr(SEApp(SEValue(vals.get(2)), Array(SEValue(res))))
                      go()
                    }
                  })
                }
                case _ => throw new RuntimeException(s"Expected record with 2 fields but got $v")
              }
            }
            case SVariant(_, "Query", v) => {
              v match {
                case SRecord(_, _, vals) if vals.size == 3 => {
                  val continue = vals.get(2)
                  val filterOrErr = for {
                    party <- Converter.toParty(vals.get(0))
                    tplId <- Converter.typeRepToIdentifier(vals.get(1))
                  } yield
                    TransactionFilter(
                      List((party.value, Filters(Some(InclusiveFilters(Seq(tplId)))))).toMap)
                  val filter = filterOrErr.fold(s => throw new ConverterException(s), identity)
                  val acsResponses = client.activeContractSetClient
                    .getActiveContracts(filter, verbose = true)
                    .runWith(Sink.seq)
                  acsResponses.flatMap(acsPages => {
                    val res =
                      FrontStack(acsPages.flatMap(page => page.activeContracts))
                        .traverseU(Converter.fromCreated(valueTranslator, stdlibPackageId, _))
                        .fold(s => throw new ConverterException(s), identity)
                    machine.ctrl =
                      Speedy.CtrlExpr(SEApp(SEValue(continue), Array(SEValue(SList(res)))))
                    go()
                  })
                }
                case _ => throw new RuntimeException(s"Expected record with 3 fields but got $v")
              }
            }
            case SVariant(_, "AllocParty", v) => {
              v match {
                case SRecord(_, _, vals) if vals.size == 2 => {
                  val displayName = vals.get(0) match {
                    case SText(value) => value
                    case v => throw new ConverterException(s"Expected SText but got $v")
                  }
                  val continue = vals.get(1)
                  val f =
                    client.partyManagementClient.allocateParty(None, Some(displayName))
                  f.flatMap(allocRes => {
                    val party = allocRes.party
                    machine.ctrl =
                      Speedy.CtrlExpr(SEApp(SEValue(continue), Array(SEValue(SParty(party)))))
                    go()
                  })
                }
                case _ => throw new RuntimeException(s"Expected record with 2 fields but got $v")
              }
            }
            case _ =>
              throw new RuntimeException(s"Expected Submit, Query or AllocParty but got $v")
          }
        }
        case SVariant(_, "Pure", v) => Future { v }
        case v => throw new RuntimeException(s"Expected Free or Pure but got $v")
      }
    }

    go()
  }
}
