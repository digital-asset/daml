// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.testing.parser

import com.digitalasset.daml.lf.data.Ref.Name
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.testing.parser.Parsers._
import com.digitalasset.daml.lf.testing.parser.Token._

@SuppressWarnings(Array("org.wartremover.warts.AnyVal"))
private[parser] class ExprParser[P](parserParameters: ParserParameters[P]) {

  import ExprParser._

  private[parser] val typeParser: TypeParser[P] = new TypeParser(parserParameters)
  import typeParser._

  lazy val expr0: Parser[Expr] =
    literal ^^ EPrimLit |
      primCon ^^ EPrimCon |
      eList |
      eOption |
      eRecCon |
      eRecProj |
      eRecUpd |
      eVariantOrEnumCon |
      eStructCon |
      eStructUpd |
      eStructProj |
      eAbs |
      eTyAbs |
      eLet |
      eToAny |
      eFromAny |
      eToTextTypeConName |
      fullIdentifier ^^ EVal |
      (id ^? builtinFunctions) ^^ EBuiltin |
      (id ^? builtinFunctions) ^^ EBuiltin |
      caseOf |
      scenario ^^ EScenario |
      update ^^ EUpdate |
      id ^^ EVar |
      `(` ~> expr <~ `)`

  lazy val exprs: Parser[List[Expr]] = rep(expr0)

  private lazy val literal: Parsers.Parser[PrimLit] =
    acceptMatch[PrimLit]("Number", { case Number(l) => PLInt64(l) }) |
      acceptMatch("Numeric", { case Numeric(d) => PLNumeric(d) }) |
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

  private lazy val eVariantOrEnumCon: Parser[Expr] =
    fullIdentifier ~ (`:` ~> id) ~ rep(argTyp) ~ opt(expr0) ^^ {
      case tName ~ vName ~ argsTyp ~ Some(arg) =>
        EVariantCon(TypeConApp(tName, ImmArray(argsTyp)), vName, arg)
      case _ ~ _ ~ argsTyp ~ None if argsTyp.nonEmpty =>
        throw new java.lang.Error("enum type do not take type parameters")
      case tName ~ vName ~ _ ~ None =>
        EEnumCon(tName, vName)
    }

  private lazy val eStructCon: Parser[Expr] =
    `<` ~> fieldInits <~ `>` ^^ EStructCon

  private lazy val eStructProj: Parser[Expr] =
    (`(` ~> expr <~ `)` ~ `.`) ~! id ^^ {
      case struct ~ fName => EStructProj(fName, struct)
    }

  private lazy val eStructUpd: Parser[Expr] =
    `<` ~> expr ~ (`with` ~>! fieldInit) <~ `>` ^^ {
      case struct ~ ((fName, value)) => EStructUpd(fName, struct, value)
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

  private lazy val eToAny: Parser[Expr] =
    `to_any` ~>! argTyp ~ expr0 ^^ {
      case ty ~ e => EToAny(ty, e)
    }

  private lazy val eFromAny: Parser[Expr] =
    `from_any` ~>! argTyp ~ expr0 ^^ {
      case ty ~ e => EFromAny(ty, e)
    }

  private lazy val eToTextTypeConName: Parser[Expr] =
    `type_rep` ~>! argTyp ^^ ETypeRep

  private lazy val pattern: Parser[CasePat] =
    primCon ^^ CPPrimCon |
      `nil` ^^^ CPNil |
      `cons` ~>! id ~ id ^^ { case x1 ~ x2 => CPCons(x1, x2) } |
      `none` ^^^ CPNone |
      `some` ~>! id ^^ CPSome |
      (fullIdentifier <~ `:`) ~ id ~ opt(id) ^^ {
        case tyCon ~ vName ~ Some(x) =>
          CPVariant(tyCon, vName, x)
        case tyCon ~ vName ~ None =>
          CPEnum(tyCon, vName)
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
    "ADD_NUMERIC" -> BAddNumeric,
    "SUB_NUMERIC" -> BSubNumeric,
    "MUL_NUMERIC" -> BMulNumeric,
    "DIV_NUMERIC" -> BDivNumeric,
    "ROUND_NUMERIC" -> BRoundNumeric,
    "CAST_NUMERIC" -> BCastNumeric,
    "SHIFT_NUMERIC" -> BShiftNumeric,
    "ADD_INT64" -> BAddInt64,
    "SUB_INT64" -> BSubInt64,
    "MUL_INT64" -> BMulInt64,
    "DIV_INT64" -> BDivInt64,
    "MOD_INT64" -> BModInt64,
    "EXP_INT64" -> BExpInt64,
    "INT64_TO_NUMERIC" -> BInt64ToNumeric,
    "NUMERIC_TO_INT64" -> BNumericToInt64,
    "DATE_TO_UNIX_DAYS" -> BDateToUnixDays,
    "UNIX_DAYS_TO_DATE" -> BUnixDaysToDate,
    "TIMESTAMP_TO_UNIX_MICROSECONDS" -> BTimestampToUnixMicroseconds,
    "UNIX_MICROSECONDS_TO_TIMESTAMP" -> BUnixMicrosecondsToTimestamp,
    "FOLDL" -> BFoldl,
    "FOLDR" -> BFoldr,
    "TEXTMAP_EMPTY" -> BTextMapEmpty,
    "TEXTMAP_INSERT" -> BTextMapInsert,
    "TEXTMAP_LOOKUP" -> BTextMapLookup,
    "TEXTMAP_DELETE" -> BTextMapDelete,
    "TEXTMAP_TO_LIST" -> BTextMapToList,
    "TEXTMAP_SIZE" -> BTextMapSize,
    "GENMAP_EMPTY" -> BGenMapEmpty,
    "GENMAP_INSERT" -> BGenMapInsert,
    "GENMAP_LOOKUP" -> BGenMapLookup,
    "GENMAP_DELETE" -> BGenMapDelete,
    "GENMAP_KEYS" -> BGenMapKeys,
    "GENMAP_VALUES" -> BGenMapValues,
    "GENMAP_SIZE" -> BGenMapSize,
    "EXPLODE_TEXT" -> BExplodeText,
    "IMPLODE_TEXT" -> BImplodeText,
    "APPEND_TEXT" -> BAppendText,
    "SHA256_TEXT" -> BSHA256Text,
    "TO_TEXT_INT64" -> BToTextInt64,
    "TO_TEXT_NUMERIC" -> BToTextNumeric,
    "TO_TEXT_TEXT" -> BToTextText,
    "TO_TEXT_TIMESTAMP" -> BToTextTimestamp,
    "TO_TEXT_PARTY" -> BToTextParty,
    "TO_TEXT_DATE" -> BToTextDate,
    "TO_QUOTED_TEXT_PARTY" -> BToQuotedTextParty,
    "TEXT_FROM_CODE_POINTS" -> BToTextCodePoints,
    "FROM_TEXT_PARTY" -> BFromTextParty,
    "FROM_TEXT_INT64" -> BFromTextInt64,
    "FROM_TEXT_NUMERIC" -> BFromTextNumeric,
    "TEXT_TO_CODE_POINTS" -> BFromTextCodePoints,
    "ERROR" -> BError,
    "LESS_INT64" -> BLessInt64,
    "LESS_NUMERIC" -> BLessNumeric,
    "LESS_TEXT" -> BLessText,
    "LESS_TIMESTAMP" -> BLessTimestamp,
    "LESS_DATE" -> BLessDate,
    "LESS_EQ_INT64" -> BLessEqInt64,
    "LESS_EQ_NUMERIC" -> BLessEqNumeric,
    "LESS_EQ_TEXT" -> BLessEqText,
    "LESS_EQ_TIMESTAMP" -> BLessEqTimestamp,
    "LESS_EQ_DATE" -> BLessEqDate,
    "GREATER_INT64" -> BGreaterInt64,
    "GREATER_NUMERIC" -> BGreaterNumeric,
    "GREATER_TEXT" -> BGreaterText,
    "GREATER_TIMESTAMP" -> BGreaterTimestamp,
    "GREATER_DATE" -> BGreaterDate,
    "GREATER_EQ_INT64" -> BGreaterEqInt64,
    "GREATER_EQ_NUMERIC" -> BGreaterEqNumeric,
    "GREATER_EQ_TEXT" -> BGreaterEqText,
    "GREATER_EQ_TIMESTAMP" -> BGreaterEqTimestamp,
    "GREATER_EQ_DATE" -> BGreaterEqDate,
    "EQUAL_NUMERIC" -> BEqualNumeric,
    "EQUAL_LIST" -> BEqualList,
    "EQUAL_CONTRACT_ID" -> BEqualContractId,
    "EQUAL" -> BEqual,
    "COERCE_CONTRACT_ID" -> BCoerceContractId,
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
    `exercise` ~! `@` ~> fullIdentifier ~ id ~ expr0 ~ expr0 ^^ {
      case t ~ choice ~ cid ~ arg => UpdateExercise(t, choice, cid, None, arg)
    }

  private lazy val updateExerciseWithActors =
    `exercise_with_actors` ~! `@` ~> fullIdentifier ~ id ~ expr0 ~ expr0 ~ expr0 ^^ {
      case t ~ choice ~ cid ~ actor ~ arg => UpdateExercise(t, choice, cid, Some(actor), arg)
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
      updateExerciseWithActors |
      updateFetchByKey |
      updateLookupByKey |
      updateGetTime |
      updateEmbedExpr

}

object ExprParser {

  private sealed trait EAppAgr extends Product with Serializable
  private final case class EAppExprArg(e: Expr) extends EAppAgr
  private final case class EAppTypArg(e: Type) extends EAppAgr

}
