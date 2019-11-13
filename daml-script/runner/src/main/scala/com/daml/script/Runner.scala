// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.typesafe.scalalogging.StrictLogging
import java.time.Instant
import java.util
import java.util.UUID
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scalaz.syntax.tag._

import com.digitalasset.api.util.TimestampConversion.fromInstant
import com.digitalasset.daml.lf.PureCompiledPackages
import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.data.FrontStack
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.{Compiler, Pretty, Speedy, SValue}
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.refinements.ApiTypes.ApplicationId
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.commands._
import com.digitalasset.ledger.api.v1.event.{CreatedEvent}
import com.digitalasset.ledger.api.v1.transaction_filter.{
  Filters,
  TransactionFilter,
  InclusiveFilters
}
import com.digitalasset.ledger.api.v1.value.{Identifier => ApiIdentifier}
import com.digitalasset.ledger.api.validation.ValueValidator
import com.digitalasset.ledger.client.LedgerClient

class Runner(dar: Dar[(PackageId, Package)], applicationId: ApplicationId) extends StrictLogging {

  val darMap: Map[PackageId, Package] = dar.all.toMap
  val compiler = Compiler(darMap)
  val scriptModuleName = DottedName.assertFromString("Daml.Script")
  val scriptPackageId: PackageId = dar.all
    .find {
      case (pkgId, pkg) => pkg.modules.contains(scriptModuleName)
    }
    .get
    ._1
  val stdlibPackageId =
    dar.all
      .find {
        case (pkgId, pkg) =>
          pkg.modules.contains(DottedName.assertFromString("DA.Internal.LF"))
      }
      .get
      ._1

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

  def toIdentifier(v: SRecord): ApiIdentifier = {
    val tId = v.values.get(0).asInstanceOf[STypeRep].ty.asInstanceOf[TTyCon].tycon
    ApiIdentifier(tId.packageId, tId.qualifiedName.module.toString, tId.qualifiedName.name.toString)
  }

  def toSubmitRequest(ledgerId: LedgerId, party: SParty, cmds: Seq[Command]) = {
    val commands = Commands(
      party = party.value,
      commands = cmds,
      ledgerId = ledgerId.unwrap,
      applicationId = applicationId.unwrap,
      commandId = UUID.randomUUID.toString,
      ledgerEffectiveTime = Some(fromInstant(Instant.EPOCH)),
      maximumRecordTime = Some(fromInstant(Instant.EPOCH.plusSeconds(5)))
    )
    SubmitAndWaitRequest(Some(commands))
  }

  def run(client: LedgerClient, scriptId: Identifier)(
      implicit ec: ExecutionContext,
      mat: ActorMaterializer): Future[SValue] = {
    val scriptExpr = EVal(scriptId)
    val machine =
      Speedy.Machine.fromSExpr(compiler.compile(scriptExpr), false, compiledPackages)

    def go(): Future[SValue] = {
      while (!machine.isFinal) {
        machine.step() match {
          case SResultContinue => ()
          case SResultError(err) => {
            throw err
          }
          case res => {
            throw new RuntimeException(s"Unexpected speedy result $res")
          }
        }
      }
      machine.traceLog.iterator.foreach {
        case (msg, optLoc) =>
          println(s"TRACE ${Pretty.prettyLoc(optLoc).render(80)}: $msg")
      }
      machine.toSValue match {
        case SVariant(_, "Free", v) => {
          v match {
            case SVariant(_, "Submit", v) => {
              v match {
                case SRecord(_, _, vals) if vals.size == 2 => {
                  val party = vals.get(0) match {
                    case p @ SParty(_) => p
                    case v => throw new ConverterException(s"Expected party but got $v")
                  }
                  val freeAp = vals.get(1)
                  val commands = Converter.toCommands(compiledPackages, freeAp) match {
                    case Left(s) => throw new ConverterException(s)
                    case Right(r) => r
                  }
                  val request = toSubmitRequest(client.ledgerId, party, commands)
                  val f =
                    client.commandServiceClient.submitAndWaitForTransactionTree(request)
                  f.flatMap(transactionTree => {
                    val events =
                      transactionTree.getTransaction.rootEventIds.map(evId =>
                        transactionTree.getTransaction.eventsById(evId))
                    val filled =
                      Converter.fillCommandResults(compiledPackages, freeAp, events) match {
                        case Left(s) => throw new ConverterException(s)
                        case Right(r) => r
                      }
                    machine.ctrl = Speedy.CtrlExpr(filled)
                    go()
                  })
                }
                case _ => throw new RuntimeException(s"Expected record with 2 fields but got $v")
              }
            }
            case SVariant(_, "Query", v) => {
              v match {
                case SRecord(_, _, vals) if vals.size == 3 => {
                  val party = vals.get(0).asInstanceOf[SParty].value
                  val tplId = toIdentifier(vals.get(1).asInstanceOf[SRecord])
                  val continue = vals.get(2)
                  val filter = TransactionFilter(
                    List((party, Filters(Some(InclusiveFilters(Seq(tplId)))))).toMap)
                  val anyTemplateTyCon =
                    Identifier(
                      stdlibPackageId,
                      QualifiedName(
                        DottedName.assertFromString("DA.Internal.LF"),
                        DottedName.assertFromString("AnyTemplate")))
                  def record(ty: Identifier, fields: (String, SValue)*): SValue = {
                    val fieldNames = Name.Array(fields.map({
                      case (n, _) => Name.assertFromString(n)
                    }): _*)
                    val args =
                      new util.ArrayList[SValue](fields.map({ case (_, v) => v }).asJava)
                    SRecord(ty, fieldNames, args)
                  }
                  def fromCreated(created: CreatedEvent) = {
                    val arg = SValue.fromValue(
                      ValueValidator.validateRecord(created.getCreateArguments).right.get)
                    val tyCon = arg.asInstanceOf[SRecord].id
                    record(anyTemplateTyCon, ("getAnyTemplate", SAny(TTyCon(tyCon), arg)))
                  }
                  val acsResponses = client.activeContractSetClient
                    .getActiveContracts(filter, verbose = true)
                    .runWith(Sink.seq)
                  acsResponses.flatMap(acsPages => {
                    val res =
                      acsPages.flatMap(page => page.activeContracts).map(fromCreated)
                    machine.ctrl = Speedy.CtrlExpr(
                      SEApp(SEValue(continue), Array(SEValue(SList(FrontStack(res))))))
                    go()
                  })
                }
                case _ => throw new RuntimeException(s"Expected record with 3 fields but got $v")
              }
            }
            case SVariant(_, "AllocParty", v) => {
              v match {
                case SRecord(_, _, vals) if vals.size == 2 => {
                  val displayName = vals.get(0).asInstanceOf[SText].value
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
