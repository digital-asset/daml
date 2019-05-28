// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.query

import com.digitalasset.navigator.dotnot._
import com.digitalasset.navigator.model._
import scalaz.Tag
import scalaz.syntax.tag._

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
      ps: DamlLfTypeLookup): Either[DotNotFailure, Boolean] = {

    @annotation.tailrec
    def loop(
        parameter: DamlLfType,
        cursor: PropertyCursor,
        ps: DamlLfTypeLookup): Either[DotNotFailure, Boolean] =
      parameter match {
        case tc: DamlLfTypeCon =>
          val next = for {
            ddt <- ps(tc.name.identifier)
            nextCursor <- cursor.next
            //nextField   <- tc.instantiate(ddt) match {
            nextField <- damlLfInstantiate(tc, ddt) match {
              case DamlLfRecord(fields) => fields.find(f => f._1 == nextCursor.current)
              case DamlLfVariant(fields) => fields.find(f => f._1 == nextCursor.current)
            }
          } yield {
            (nextField._2, nextCursor)
          }
          next match {
            case Some((nextType, nextCursor)) => loop(nextType, nextCursor, ps)
            case None => Right(false)
          }

        case DamlLfTypeVar(name) => Right(checkContained(name, expectedValue))
        case DamlLfTypePrim(DamlLfPrimType.Bool, _) => Right(checkContained("bool", expectedValue))
        case DamlLfTypePrim(DamlLfPrimType.Decimal, _) =>
          Right(checkContained("decimal", expectedValue))
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
        case DamlLfTypePrim(DamlLfPrimType.Map, _) =>
          Right(checkContained("map", expectedValue))
      }

    loop(rootParam, cursor.prev.get, ps)
  }

  def checkArgument(
      rootArgument: ApiValue,
      cursor: PropertyCursor,
      expectedValue: String,
      ps: DamlLfTypeLookup): Either[DotNotFailure, Boolean] = {
    def listIndex(cursor: PropertyCursor): Either[DotNotFailure, Int] = {
      try {
        Right(cursor.current.toInt)
      } catch {
        case e: Exception => Left(TypeCoercionFailure("list index", "int", cursor, cursor.current))
      }
    }

    @annotation.tailrec
    def loop(argument: ApiValue, cursor: PropertyCursor): Either[DotNotFailure, Boolean] =
      argument match {
        case ApiRecord(_, fields) =>
          cursor.next match {
            case None => Right(false)
            case Some(nextCursor) =>
              fields.find(f => f.label == nextCursor.current) match {
                case Some(nextField) => loop(nextField.value, nextCursor)
                case None => Right(false)
              }
          }
        case ApiVariant(_, constructor, value) =>
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
        case ApiList(elements) =>
          cursor.next match {
            case None => Right(false)
            case Some(nextCursor) =>
              Try(nextCursor.current.toInt) match {
                case Success(index) => loop(elements(index), nextCursor)
                case Failure(e) =>
                  Left(TypeCoercionFailure("list index", "int", cursor, cursor.current))
              }
          }
        case ApiContractId(value) if cursor.isLast => Right(checkContained(value, expectedValue))
        case ApiInt64(value) if cursor.isLast =>
          Right(checkContained(value.toString, expectedValue))
        case ApiDecimal(value) if cursor.isLast => Right(checkContained(value, expectedValue))
        case ApiText(value) if cursor.isLast => Right(checkContained(value, expectedValue))
        case ApiParty(value) if cursor.isLast => Right(checkContained(value, expectedValue))
        case ApiBool(value) if cursor.isLast => Right(checkContained(value.toString, expectedValue))
        case ApiUnit() if cursor.isLast => Right(expectedValue == "")
        case ApiOptional(optValue) =>
          (cursor.next, optValue) match {
            case (None, None) => Right(expectedValue == "None")
            case (None, Some(_)) => Right(expectedValue == "Some")
            case (Some(nextCursor), Some(value)) if nextCursor.current == "Some" =>
              loop(value, nextCursor)
            case (Some(nextCursor), None) if nextCursor.current == "None" => Right(true)
            case (Some(nextCursor), _) => Right(false)
          }
        case t: ApiTimestamp if cursor.isLast => Right(checkContained(t.toIso8601, expectedValue))
        case t: ApiDate if cursor.isLast => Right(checkContained(t.toIso8601, expectedValue))
      }
    loop(rootArgument, cursor.prev.get)
  }

  lazy val parameterFilter =
    opaque[DamlLfType, Boolean, DamlLfTypeLookup]("parameter")((t, c, e, p) =>
      checkParameter(t, c, e, p))

  lazy val parameterIdFilter =
    opaque[DamlLfIdentifier, Boolean, DamlLfTypeLookup]("parameter")((id, c, e, p) =>
      checkParameter(DamlLfTypeCon(DamlLfTypeConName(id), DamlLfImmArraySeq()), c, e, p))

  lazy val argumentFilter =
    opaque[ApiRecord, Boolean, DamlLfTypeLookup]("argument")(checkArgument)

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
        checkContained(template.topLevelDecl, topLevelDecl))
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
      .onLeaf("agreementText")
      .onValue("*")
      .const(true)
      .onAnyValue
      .perform[String]((contract, agree) => checkOptionalContained(contract.agreementText, agree))
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
      .perform[String](
        (command, index) => checkContained(command.index.toString, index.toLowerCase))
      .onLeaf("platformTime")
      .onValue("*")
      .const(true)
      .onAnyValue
      .perform[String]((command, time) =>
        checkContained(command.platformTime.toString, time.toLowerCase))
      .onLeaf("workflowId")
      .onValue("*")
      .const(true)
      .onAnyValue
      .perform[String]((command, id) => checkContained(command.workflowId.unwrap, id.toLowerCase))
      .onTree
  //  .onStar(check all fields)
}
