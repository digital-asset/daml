// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.testing.parser

import com.digitalasset.daml.lf.data.Ref.Name
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.lfpackage.Ast._
import com.digitalasset.daml.lf.testing.parser.Parsers._
import com.digitalasset.daml.lf.testing.parser.Token._
import com.digitalasset.daml.lf.testing.parser.TypeParser._

@SuppressWarnings(Array("org.wartremover.warts.AnyVal"))
private[parser] object ExprParser {

  lazy val expr0: Parser[Expr] =
    literal ^^ EPrimLit |
      primCon ^^ EPrimCon |
      eList |
      eOption |
      eRecCon |
      eRecProj |
      eRecUpd |
      eVariantCon |
      eTupleCon |
      eTupleUpd |
      eTupleProj |
      eAbs |
      eTyAbs |
      eLet |
      contractId |
      fullIdentifier ^^ EVal |
      (id ^? builtinFunctions) ^^ EBuiltin |
      (id ^? builtinFunctions) ^^ EBuiltin |
      caseOf |
      scenario ^^ EScenario |
      update ^^ EUpdate |
      id ^^ EVar |
      `(` ~> expr <~ `)`

  private lazy val literal: Parsers.Parser[PrimLit] =
    acceptMatch[PrimLit]("Number", { case Number(l) => PLInt64(l) }) |
      acceptMatch("Decimal", { case Decimal(d) => PLDecimal(d) }) |
      acceptMatch("Text", { case Text(s) => PLText(s) }) |
      acceptMatch("Timestamp", { case Timestamp(l) => PLTimestamp(l) }) |
      acceptMatch("Date", { case Date(l) => PLDate(l) }) |
      acceptMatch("Party", {
        case SimpleString(s) if Ref.Party.fromString(s).isRight =>
          PLParty(Ref.Party.assertFromString(s))
      })

  private lazy val primCon =
    Id("True") ^^^ PCTrue |
      Id("False") ^^^ PCFalse |
      `(` ~ `)` ^^^ PCUnit

  private lazy val contractId =
    accept("ContractId", { case ContractId(cid) => cid }) ~ (`@` ~> fullIdentifier) ^^ {
      case cid ~ t => EContractId(cid, t)
    }

  private sealed trait EAppAgr extends Product with Serializable
  private final case class EAppExprArg(e: Expr) extends EAppAgr
  private final case class EAppTypArg(e: Type) extends EAppAgr

  private lazy val eAppAgr: Parser[EAppAgr] =
    argTyp ^^ EAppTypArg |
      expr0 ^^ EAppExprArg

  lazy val expr: Parser[Expr] = {
    expr0 ~ rep(eAppAgr) ^^ {
      case e0 ~ args =>
        (e0 /: args) {
          case (acc, EAppExprArg(e)) => EApp(acc, e)
          case (acc, EAppTypArg(t)) => ETyApp(acc, t)
        }
    }
  }

  private lazy val fieldInit: Parser[(Name, Expr)] = id ~ (`=` ~> expr) ^^ {
    case fName ~ value => fName -> value
  }

  private lazy val fieldInits: Parser[ImmArray[(Name, Expr)]] =
    repsep(fieldInit, `,`) ^^ ImmArray.apply

  private lazy val typeArgs = rep(argTyp) ^^ (ImmArray(_))

  private lazy val typeConApp = fullIdentifier ~ typeArgs ^^ {
    case tName ~ types => TypeConApp(tName, types)
  }

  private lazy val eList = eNil | eCons

  private lazy val eNil = `nil` ~>! argTyp ^^ ENil

  private lazy val eCons = `cons` ~>! argTyp ~ (`[` ~> repsep(expr, `,`) <~ `]`) ~ expr0 ^^ {
    case t ~ front ~ tail => ECons(t, ImmArray(front), tail)
  }

  private lazy val eOption = eNone | eSome

  private lazy val eNone = `none` ~>! argTyp ^^ ENone

  private lazy val eSome = `some` ~>! argTyp ~ expr0 ^^ {
    case t ~ e => ESome(t, e)
  }

  private lazy val eRecCon: Parser[Expr] =
    typeConApp ~ (`{` ~> fieldInits <~ `}`) ^^ {
      case tConApp ~ fields => ERecCon(tConApp, fields)
    }

  private lazy val eRecProj: Parser[Expr] =
    typeConApp ~ (`{` ~> id <~ `}`) ~! expr0 ^^ {
      case tConApp ~ fName ~ record =>
        ERecProj(tConApp, fName, record)
    }

  private lazy val eRecUpd: Parser[Expr] =
    typeConApp ~ (`{` ~> expr ~ (`with` ~>! fieldInit <~ `}`)) ^^ {
      case tConApp ~ (record ~ ((fName, fValue))) =>
        ERecUpd(tConApp, fName, record, fValue)
    }

  private lazy val eVariantCon: Parser[Expr] =
    fullIdentifier ~ (`:` ~> id) ~ rep(argTyp) ~! expr0 ^^ {
      case tName ~ vName ~ argsTyp ~ arg =>
        EVariantCon(TypeConApp(tName, ImmArray(argsTyp)), vName, arg)
    }

  private lazy val eTupleCon: Parser[Expr] =
    `<` ~> fieldInits <~ `>` ^^ ETupleCon

  private lazy val eTupleProj: Parser[Expr] =
    (`(` ~> expr <~ `)` ~ `.`) ~! id ^^ {
      case tuple ~ fName => ETupleProj(fName, tuple)
    }

  private lazy val eTupleUpd: Parser[Expr] =
    `<` ~> expr ~ (`with` ~>! fieldInit) <~ `>` ^^ {
      case tuple ~ ((fName, value)) => ETupleUpd(fName, tuple, value)
    }

  private[parser] lazy val varBinder: Parser[(Name, Type)] =
    `(` ~> id ~ (`:` ~> typ <~ `)`) ^^ { case name ~ t => name -> t }

  private lazy val eAbs: Parser[Expr] =
    `\\` ~>! rep1(varBinder) ~ (`->` ~> expr) ^^ {
      case binders ~ body => (binders :\ body)(EAbs(_, _, None))
    }

  private lazy val eTyAbs: Parser[Expr] =
    `/\\` ~>! rep1(typeBinder) ~ (`.` ~> expr) ^^ {
      case binders ~ body => (binders :\ body)(ETyAbs)
    }

  private lazy val bindings: Parser[ImmArray[Binding]] =
    rep1sep(binding(`<-`), `;`) <~! `in` ^^ (s => ImmArray(s))

  private def binding(sep: Token): Parser[Binding] =
    id ~ (`:` ~> typ) ~ (sep ~> expr) ^^ {
      case vName ~ t ~ value => Binding(Some(vName), t, value)
    }

  private lazy val eLet: Parser[Expr] =
    `let` ~>! binding(`=`) ~ (`in` ~> expr) ^^ {
      case b ~ body => ELet(b, body)
    }

  private lazy val pattern: Parser[CasePat] =
    primCon ^^ CPPrimCon |
      `nil` ^^^ CPNil |
      `cons` ~>! id ~ id ^^ { case x1 ~ x2 => CPCons(x1, x2) } |
      `none` ^^^ CPNone |
      `some` ~>! id ^^ CPSome |
      (fullIdentifier <~ `:`) ~ id ~ id ^^ {
        case tyCon ~ vName ~ x =>
          CPVariant(tyCon, vName, x)
      } |
      Token.`_` ^^^ CPDefault

  private lazy val alternative: Parser[CaseAlt] =
    pattern ~! (`->` ~>! expr) ^^ {
      case p ~ e => CaseAlt(p, e)
    }

  private lazy val caseOf: Parser[Expr] =
    `case` ~>! expr ~ (`of` ~> repsep(alternative, `|`)) ^^ {
      case scrut ~ alts => ECase(scrut, ImmArray(alts))
    }

  private val builtinFunctions = Map(
    "TRACE" -> BTrace,
    "ADD_DECIMAL" -> BAddDecimal,
    "SUB_DECIMAL" -> BSubDecimal,
    "MUL_DECIMAL" -> BMulDecimal,
    "DIV_DECIMAL" -> BDivDecimal,
    "ROUND_DECIMAL" -> BRoundDecimal,
    "ADD_INT64" -> BAddInt64,
    "SUB_INT64" -> BSubInt64,
    "MUL_INT64" -> BMulInt64,
    "DIV_INT64" -> BDivInt64,
    "MOD_INT64" -> BModInt64,
    "EXP_INT64" -> BExpInt64,
    "INT64_TO_DECIMAL" -> BInt64ToDecimal,
    "DECIMAL_TO_INT64" -> BDecimalToInt64,
    "DATE_TO_UNIX_DAYS" -> BDateToUnixDays,
    "UNIX_DAYS_TO_DATE" -> BUnixDaysToDate,
    "TIMESTAMP_TO_UNIX_MICROSECONDS" -> BTimestampToUnixMicroseconds,
    "UNIX_MICROSECONDS_TO_TIMESTAMP" -> BUnixMicrosecondsToTimestamp,
    "FOLDL" -> BFoldl,
    "FOLDR" -> BFoldr,
    "MAP_EMPTY" -> BMapEmpty,
    "MAP_INSERT" -> BMapInsert,
    "MAP_LOOKUP" -> BMapLookup,
    "MAP_DELETE" -> BMapDelete,
    "MAP_TO_LIST" -> BMapToList,
    "MAP_SIZE" -> BMapSize,
    "EXPLODE_TEXT" -> BExplodeText,
    "IMPLODE_TEXT" -> BImplodeText,
    "APPEND_TEXT" -> BAppendText,
    "SHA256_TEXT" -> BSHA256Text,
    "TO_TEXT_INT64" -> BToTextInt64,
    "TO_TEXT_DECIMAL" -> BToTextDecimal,
    "TO_TEXT_TEXT" -> BToTextText,
    "TO_TEXT_TIMESTAMP" -> BToTextTimestamp,
    "TO_TEXT_PARTY" -> BToTextParty,
    "TO_TEXT_DATE" -> BToTextDate,
    "TO_QUOTED_TEXT_PARTY" -> BToQuotedTextParty,
    "FROM_TEXT_PARTY" -> BFromTextParty,
    "ERROR" -> BError,
    "LESS_INT64" -> BLessInt64,
    "LESS_DECIMAL" -> BLessDecimal,
    "LESS_TEXT" -> BLessText,
    "LESS_TIMESTAMP" -> BLessTimestamp,
    "LESS_DATE" -> BLessDate,
    "LESS_EQ_INT64" -> BLessEqInt64,
    "LESS_EQ_DECIMAL" -> BLessEqDecimal,
    "LESS_EQ_TEXT" -> BLessEqText,
    "LESS_EQ_TIMESTAMP" -> BLessEqTimestamp,
    "LESS_EQ_DATE" -> BLessEqDate,
    "GREATER_INT64" -> BGreaterInt64,
    "GREATER_DECIMAL" -> BGreaterDecimal,
    "GREATER_TEXT" -> BGreaterText,
    "GREATER_TIMESTAMP" -> BGreaterTimestamp,
    "GREATER_DATE" -> BGreaterDate,
    "GREATER_EQ_INT64" -> BGreaterEqInt64,
    "GREATER_EQ_DECIMAL" -> BGreaterEqDecimal,
    "GREATER_EQ_TEXT" -> BGreaterEqText,
    "GREATER_EQ_TIMESTAMP" -> BGreaterEqTimestamp,
    "GREATER_EQ_DATE" -> BGreaterEqDate,
    "EQUAL_INT64" -> BEqualInt64,
    "EQUAL_DECIMAL" -> BEqualDecimal,
    "EQUAL_TEXT" -> BEqualText,
    "EQUAL_TIMESTAMP" -> BEqualTimestamp,
    "EQUAL_DATE" -> BEqualDate,
    "EQUAL_PARTY" -> BEqualParty,
    "EQUAL_BOOL" -> BEqualBool,
    "EQUAL_LIST" -> BEqualList,
    "EQUAL_CONTRACT_ID" -> BEqualContractId
  )

  /* Scenarios */

  private lazy val scenarioPure: Parser[Scenario] =
    Id("spure") ~>! argTyp ~ expr0 ^^ {
      case t ~ e => ScenarioPure(t, e)
    }

  private lazy val scenarioBlock: Parser[Scenario] =
    `sbind` ~>! bindings ~ expr ^^ {
      case bs ~ body => ScenarioBlock(bs, body)
    }

  private lazy val scenarioCommit: Parser[Scenario] =
    Id("commit") ~>! argTyp ~ expr0 ~ expr0 ^^ {
      case t ~ actor ~ upd => ScenarioCommit(actor, upd, t)
    }

  private lazy val scenarioMustFailAt: Parser[Scenario] =
    Id("must_fail_at") ~>! argTyp ~ expr0 ~ expr0 ^^ {
      case t ~ actor ~ upd => ScenarioMustFailAt(actor, upd, t)
    }

  private lazy val scenarioPass: Parser[Scenario] =
    Id("pass") ~>! expr0 ^^ { case offest => ScenarioPass(offest) }

  private lazy val scenarioGetTime: Parser[Scenario] =
    Id("sget_time") ^^^ ScenarioGetTime

  private lazy val scenarioGetParty: Parser[Scenario] =
    Id("sget_party") ~>! expr0 ^^ { case nameE => ScenarioGetParty(nameE) }

  private lazy val scenarioEmbedExpr: Parser[Scenario] =
    Id("sembed_expr") ~>! argTyp ~ expr0 ^^ {
      case t ~ e => ScenarioEmbedExpr(t, e)
    }

  private lazy val scenario: Parser[Scenario] =
    scenarioPure |
      scenarioBlock |
      scenarioCommit |
      scenarioMustFailAt |
      scenarioPass |
      scenarioGetTime |
      scenarioGetParty |
      scenarioEmbedExpr

  /* Updates */

  private lazy val updatePure =
    Id("upure") ~>! argTyp ~ expr0 ^^ {
      case t ~ e => UpdatePure(t, e)
    }

  private lazy val updateBlock =
    `ubind` ~>! bindings ~ expr ^^ {
      case bs ~ body => UpdateBlock(bs, body)
    }

  private lazy val updateCreate =
    `create` ~! `@` ~> fullIdentifier ~ expr0 ^^ {
      case t ~ e => UpdateCreate(t, e)
    }

  private lazy val updateFetch =
    `fetch` ~! `@` ~> fullIdentifier ~ expr0 ^^ {
      case t ~ e => UpdateFetch(t, e)
    }

  private lazy val updateExercise =
    `exercise` ~! `@` ~> fullIdentifier ~ id ~ expr0 ~ expr0 ~ expr0 ^^ {
      case t ~ choice ~ cid ~ actor ~ arg => UpdateExercise(t, choice, cid, actor, arg)
    }

  private lazy val updateFetchByKey =
    `fetch_by_key` ~! `@` ~> fullIdentifier ~ expr ^^ {
      case t ~ eKey => UpdateFetchByKey(RetrieveByKey(t, eKey))
    }

  private lazy val updateLookupByKey =
    `lookup_by_key` ~! `@` ~> fullIdentifier ~ expr ^^ {
      case t ~ eKey => UpdateLookupByKey(RetrieveByKey(t, eKey))
    }

  private lazy val updateGetTime =
    Id("uget_time") ^^^ UpdateGetTime

  private lazy val updateEmbedExpr =
    Id("uembed_expr") ~> argTyp ~ expr0 ^^ {
      case t ~ e => UpdateEmbedExpr(t, e)
    }

  private lazy val update: Parser[Update] =
    updatePure |
      updateBlock |
      updateCreate |
      updateFetch |
      updateExercise |
      updateFetchByKey |
      updateLookupByKey |
      updateGetTime |
      updateEmbedExpr

}
