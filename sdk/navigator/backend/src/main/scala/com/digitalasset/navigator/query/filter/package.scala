// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.query

import com.daml.lf.value.json.ApiValueImplicits._
import com.daml.lf.value.{Value => V}
import com.daml.navigator.dotnot._
import com.daml.navigator.model._
import scalaz.Tag
import scalaz.syntax.tag._

import scala.annotation.nowarn
import scala.util.{Failure, Success, Try}

package object filter {

  object checkContained extends ((String, String) => Boolean) {

    override def apply(v1: String, v2: String): Boolean =
      v1.toLowerCase contains v2.toLowerCase

    override def toString(): String =
      "target.contains(value)"
  }

  object checkOptionalContained extends ((Option[String], String) => Boolean) {

    override def apply(v1: Option[String], v2: String): Boolean =
      v1.exists(_.toLowerCase contains v2.toLowerCase)

    override def toString(): String =
      "target.exists(_.contains(value))"
  }

  def checkParameter(
      rootParam: DamlLfType,
      cursor: PropertyCursor,
      expectedValue: String,
      ps: DamlLfTypeLookup,
  ): Either[DotNotFailure, Boolean] = {

    @annotation.tailrec
    def loop(parameter: DamlLfType, cursor: PropertyCursor): Either[DotNotFailure, Boolean] =
      parameter match {
        case tc: DamlLfTypeCon =>
          val nextOrResult =
            (ps(tc.name.identifier).map(tc.instantiate(_)), cursor.next) match {
              case (Some(DamlLfRecord(fields)), Some(nextCursor)) =>
                fields
                  .collectFirst { case (nextCursor.current, fType) =>
                    fType -> nextCursor
                  }
                  .toLeft(false)
              case (Some(DamlLfVariant(fields)), Some(nextCursor)) =>
                fields
                  .collectFirst { case (nextCursor.current, fType) =>
                    fType -> nextCursor
                  }
                  .toLeft(false)
              case (Some(DamlLfEnum(constructors)), _) =>
                Right(constructors.exists(checkContained(_, expectedValue)))
              case (None, _) | (_, None) =>
                Right(false)
            }

          nextOrResult match {
            case Right(r) => Right(r)
            case Left((typ, nextCursor)) => loop(typ, nextCursor)
          }

        case DamlLfTypeVar(name) => Right(checkContained(name, expectedValue))
        case DamlLfTypePrim(DamlLfPrimType.Bool, _) => Right(checkContained("bool", expectedValue))
        case DamlLfTypeNumeric(_) => Right(checkContained("decimal", expectedValue))
        case DamlLfTypePrim(DamlLfPrimType.Int64, _) =>
          Right(checkContained("int64", expectedValue))
        case DamlLfTypePrim(DamlLfPrimType.Date, _) => Right(checkContained("date", expectedValue))
        case DamlLfTypePrim(DamlLfPrimType.Text, _) => Right(checkContained("text", expectedValue))
        case DamlLfTypePrim(DamlLfPrimType.Party, _) =>
          Right(checkContained("party", expectedValue))
        case DamlLfTypePrim(DamlLfPrimType.Timestamp, _) =>
          Right(checkContained("timestamp", expectedValue))
        case DamlLfTypePrim(DamlLfPrimType.Unit, _) => Right(checkContained("unit", expectedValue))
        case DamlLfTypePrim(DamlLfPrimType.List, _) => Right(checkContained("list", expectedValue))
        case DamlLfTypePrim(DamlLfPrimType.ContractId, _) =>
          Right(checkContained("contractid", expectedValue))
        case DamlLfTypePrim(DamlLfPrimType.Optional, _) =>
          Right(checkContained("optional", expectedValue))
        case DamlLfTypePrim(DamlLfPrimType.TextMap, _) =>
          Right(checkContained("textmap", expectedValue))
        case DamlLfTypePrim(DamlLfPrimType.GenMap, _) =>
          Right(checkContained("genmap", expectedValue))

      }

    loop(rootParam, cursor.prev.get)
  }

  def checkOptionalValue(
      rootArgument: Option[ApiValue],
      cursor: PropertyCursor,
      expectedValue: String,
      ps: DamlLfTypeLookup,
  ): Either[DotNotFailure, Boolean] =
    rootArgument.fold[Either[DotNotFailure, Boolean]](Right(false))(
      checkValue(_, cursor, expectedValue, ps)
    )

  @nowarn("msg=parameter ps .* is never used") // conform to opaque's signature
  def checkValue(
      rootArgument: ApiValue,
      cursor: PropertyCursor,
      expectedValue: String,
      ps: DamlLfTypeLookup,
  ): Either[DotNotFailure, Boolean] = {

    @annotation.tailrec
    def loop(argument: ApiValue, cursor: PropertyCursor): Either[DotNotFailure, Boolean] =
      argument match {
        case V.ValueContractId(value) =>
          cursor.ensureLast("ContractId")(checkContained(value.coid, expectedValue))
        case V.ValueInt64(value) =>
          cursor.ensureLast("Int64")(checkContained(value.toString, expectedValue))
        case V.ValueNumeric(value) =>
          cursor.ensureLast("Numeric")(checkContained(value.toUnscaledString, expectedValue))
        case V.ValueText(value) =>
          cursor.ensureLast("Text")(checkContained(value, expectedValue))
        case V.ValueParty(value) =>
          cursor.ensureLast("Party")(checkContained(value, expectedValue))
        case V.ValueBool(value) =>
          cursor.ensureLast("Bool")(checkContained(value.toString, expectedValue))
        case V.ValueUnit =>
          cursor.ensureLast("Unit")(expectedValue == "")
        case t: V.ValueTimestamp =>
          cursor.ensureLast("Timestamp")(checkContained(t.toIso8601, expectedValue))
        case t: V.ValueDate =>
          cursor.ensureLast("Date")(checkContained(t.toIso8601, expectedValue))
        case V.ValueRecord(_, fields) =>
          cursor.next match {
            case None => Right(false)
            case Some(nextCursor) =>
              val current: String = nextCursor.current
              fields.toSeq.collectFirst { case (Some(`current`), value) => value } match {
                case Some(nextField) => loop(nextField, nextCursor)
                case None => Right(false)
              }
          }
        case V.ValueVariant(_, constructor, value) =>
          cursor.next match {
            case None => Right(false)
            case Some(nextCursor) =>
              nextCursor.current match {
                case "__constructor" => Right(checkContained(constructor, expectedValue))
                case "__value" => loop(value, nextCursor)
                case `constructor` => loop(value, nextCursor)
                case _ => Right(false)
              }
          }
        case V.ValueEnum(_, constructor) =>
          cursor.next match {
            case None => Right(false)
            case Some(nextCursor) =>
              nextCursor.current match {
                case "__constructor" => Right(checkContained(constructor, expectedValue))
                case _ => Right(false)
              }
          }
        case V.ValueList(elements) =>
          cursor.next match {
            case None => Right(false)
            case Some(nextCursor) =>
              Try(nextCursor.current.toInt) match {
                case Success(index) => loop(elements.slowApply(index), nextCursor)
                case Failure(_) =>
                  Left(TypeCoercionFailure("list index", "int", cursor, cursor.current))
              }
          }

        case V.ValueOptional(optValue) =>
          (cursor.next, optValue) match {
            case (None, None) => Right(expectedValue == "None")
            case (None, Some(_)) => Right(expectedValue == "Some")
            case (Some(nextCursor), Some(value)) if nextCursor.current == "Some" =>
              loop(value, nextCursor)
            case (Some(nextCursor), None) if nextCursor.current == "None" => Right(true)
            case (Some(_), _) => Right(false)
          }
        case V.ValueTextMap(textMap) =>
          cursor.next match {
            case None => Right(false)
            case Some(nextCursor) =>
              textMap.toImmArray.toSeq.collectFirst {
                case (k, v) if k == nextCursor.current => v
              } match {
                case None => Right(false)
                case Some(v) => loop(v, nextCursor)
              }
          }
        case V.ValueGenMap(entries) =>
          cursor.next match {
            case None =>
              Right(false)
            case Some(nextCursor) =>
              Try(nextCursor.current.toInt) match {
                case Success(index) =>
                  nextCursor.next match {
                    case Some(nextNextCursor) if nextNextCursor.current == "key" =>
                      loop(entries(index)._1, nextNextCursor)
                    case Some(nextNextCursor) if nextNextCursor.current == "value" =>
                      loop(entries(index)._2, nextNextCursor)
                    case Some(_) => Left(UnknownProperty("genmap", nextCursor, expectedValue))
                    case None =>
                      Right(false)
                  }
                case Failure(_) =>
                  Left(TypeCoercionFailure("GenMap index", "int", cursor, cursor.current))
              }

          }
      }

    loop(rootArgument, cursor.prev.get)
  }

  lazy val parameterFilter =
    opaque[DamlLfType, Boolean, DamlLfTypeLookup]("parameter")((t, c, e, p) =>
      checkParameter(t, c, e, p)
    )

  lazy val parameterIdFilter =
    opaque[DamlLfIdentifier, Boolean, DamlLfTypeLookup]("parameter")((id, c, e, p) =>
      checkParameter(DamlLfTypeCon(DamlLfTypeConName(id), DamlLfImmArraySeq()), c, e, p)
    )

  lazy val argumentFilter =
    opaque[ApiValue, Boolean, DamlLfTypeLookup]("argument")(checkValue)

  lazy val keyFilter =
    opaque[Option[ApiValue], Boolean, DamlLfTypeLookup]("key")(checkOptionalValue)

  lazy val templateFilter =
    root[Template, Boolean, DamlLfTypeLookup]("template")
      .onLeaf("id")
      .onValue("*")
      .const(true)
      .onAnyValue
      .perform[String]((contract, id) => checkContained(contract.idString, id))
      .onLeaf("topLevelDecl")
      .onValue("*")
      .const(true)
      .onAnyValue
      .perform[String]((template, topLevelDecl) =>
        checkContained(template.topLevelDecl, topLevelDecl)
      )
      .onBranch("parameter", _.id, parameterIdFilter)
      .onBranch("choices", _.choices, choicesFilter)
  //  .onStar(check all fields)

  lazy val contractFilter =
    root[Contract, Boolean, DamlLfTypeLookup]("contract")
      .onLeaf("id")
      .onValue("*")
      .const(true)
      .onAnyValue
      .perform[String]((contract, id) => checkContained(contract.id.unwrap, id.toLowerCase))
      .onBranch("template", _.template, templateFilter)
      .onBranch("argument", _.argument, argumentFilter)
      .onBranch("key", _.key, keyFilter)
      .onLeaf("agreementText")
      .onValue("*")
      .const(true)
      .onAnyValue
      .perform[String]((contract, agree) => checkOptionalContained(contract.agreementText, agree))
      .onTree
      .onLeaf("signatories")
      .onValue("*")
      .const(true)
      .onAnyValue
      .perform[String]((contract, signatory) =>
        contract.signatories.map(Tag.unwrap).exists(checkContained(_, signatory))
      )
      .onTree
      .onLeaf("observers")
      .onValue("*")
      .const(true)
      .onAnyValue
      .perform[String]((contract, observer) =>
        contract.observers.map(Tag.unwrap).exists(checkContained(_, observer))
      )
      .onTree
  //  .onStar(check all fields)

  lazy val choicesFilter =
    root[Seq[Choice], Boolean, DamlLfTypeLookup]("choices")
      .onElements[Choice](choice => Tag.unwrap(choice.name), choiceFilter)

  lazy val choiceFilter =
    root[Choice, Boolean, DamlLfTypeLookup]("choice")
      .onLeaf("name")
      .onValue("*")
      .const(true)
      .onAnyValue
      .perform[String]((choice, name) => checkContained(Tag.unwrap(choice.name), name.toLowerCase))
      .onLeaf("consuming")
      .onAnyValue
      .perform[Boolean]((choice, consuming) => choice.consuming == consuming)
      .onBranch("parameter", _.parameter, parameterFilter)
      //  .onStar(check all fields)
      .onTree

  lazy val commandFilter =
    root[Command, Boolean, DamlLfTypeLookup]("command")
      .onLeaf("id")
      .onValue("*")
      .const(true)
      .onAnyValue
      .perform[String]((command, id) => checkContained(command.id.unwrap, id.toLowerCase))
      .onLeaf("index")
      .onValue("*")
      .const(true)
      .onAnyValue
      .perform[String]((command, index) =>
        checkContained(command.index.toString, index.toLowerCase)
      )
      .onLeaf("platformTime")
      .onValue("*")
      .const(true)
      .onAnyValue
      .perform[String]((command, time) =>
        checkContained(command.platformTime.toString, time.toLowerCase)
      )
      .onLeaf("workflowId")
      .onValue("*")
      .const(true)
      .onAnyValue
      .perform[String]((command, id) => checkContained(command.workflowId.unwrap, id.toLowerCase))
      .onTree
  //  .onStar(check all fields)
}
