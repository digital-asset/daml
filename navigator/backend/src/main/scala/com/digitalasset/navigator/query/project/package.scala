// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.query

import com.daml.navigator.dotnot._
import com.daml.navigator.model._
import com.daml.lf.value.{Value => V}
import com.daml.lf.value.json.ApiValueImplicits._
import scalaz.Tag
import scalaz.syntax.tag._

import scala.util.{Failure, Success, Try}

@SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
object project {

  /** ADT of primitives used as result of a projection over contracts or templates */
  sealed abstract class ProjectValue
  final case class StringValue(value: String) extends ProjectValue
  final case class NumberValue(value: BigDecimal) extends ProjectValue
  final case class BooleanValue(value: Boolean) extends ProjectValue

  implicit val projectValueOrdering: Ordering[ProjectValue] = new Ordering[ProjectValue] {
    override def compare(x: ProjectValue, y: ProjectValue): Int =
      x match {
        case StringValue(s) =>
          y match {
            case StringValue(s2) => s compareTo s2
            case _ => 1
          }
        case NumberValue(n) =>
          y match {
            case NumberValue(n2) => n compare n2
            case _: StringValue => -1
            case _: BooleanValue => 1
          }
        case BooleanValue(b) =>
          y match {
            case BooleanValue(b2) => b compareTo b2
            case _ => -1
          }
      }
  }

  def checkParameter(
      rootParam: DamlLfType,
      cursor: PropertyCursor,
      value: String,
      ps: DamlLfTypeLookup): Either[DotNotFailure, ProjectValue] = {

    @annotation.tailrec
    def loop(
        parameter: DamlLfType,
        cursor: PropertyCursor,
        ps: DamlLfTypeLookup): Either[DotNotFailure, ProjectValue] =
      parameter match {
        case tc: DamlLfTypeCon =>
          val next = for {
            ddt <- ps(tc.name.identifier)
              .toRight(UnknownType(tc.name.identifier.toString, cursor, value))
            nextCursor <- cursor.next.toRight(MustNotBeLastPart("DataType", cursor, value))
            //nextField   <- tc.instantiate(ddt) match {
            nextField <- damlLfInstantiate(tc, ddt) match {
              case DamlLfRecord(fields) =>
                fields
                  .find(f => f._1 == nextCursor.current)
                  .toRight(UnknownProperty("record", nextCursor, value))
              case DamlLfVariant(fields) =>
                fields
                  .find(f => f._1 == nextCursor.current)
                  .toRight(UnknownProperty("variant", nextCursor, value))
              case DamlLfEnum(_) =>
                // FixMe (RH) https://github.com/digital-asset/daml/issues/105
                throw new NotImplementedError("Enum types not supported")
            }
          } yield {
            (nextField._2, nextCursor)
          }
          next match {
            case Right((nextType, nextCursor)) => loop(nextType, nextCursor, ps)
            case Left(e) => Left(e)
          }

        case DamlLfTypeVar(name) => Right(StringValue(name))
        case DamlLfTypePrim(DamlLfPrimType.Bool, _) => Right(StringValue("bool"))
        case DamlLfTypeNumeric(_) => Right(StringValue("decimal"))
        case DamlLfTypePrim(DamlLfPrimType.Int64, _) => Right(StringValue("int64"))
        case DamlLfTypePrim(DamlLfPrimType.Date, _) => Right(StringValue("date"))
        case DamlLfTypePrim(DamlLfPrimType.Text, _) => Right(StringValue("text"))
        case DamlLfTypePrim(DamlLfPrimType.Party, _) => Right(StringValue("party"))
        case DamlLfTypePrim(DamlLfPrimType.Timestamp, _) => Right(StringValue("timestamp"))
        case DamlLfTypePrim(DamlLfPrimType.Unit, _) => Right(StringValue("unit"))
        case DamlLfTypePrim(DamlLfPrimType.List, _) => Right(StringValue("list"))
        case DamlLfTypePrim(DamlLfPrimType.ContractId, _) => Right(StringValue("contractid"))
        case DamlLfTypePrim(DamlLfPrimType.Optional, _) => Right(StringValue("optional"))
        case DamlLfTypePrim(DamlLfPrimType.TextMap, _) => Right(StringValue("textmap"))
        case DamlLfTypePrim(DamlLfPrimType.GenMap, _) => Right(StringValue("genmap"))
      }

    loop(rootParam, cursor.prev.get, ps)
  }

  def checkOptionalValue(
      rootArgument: Option[ApiValue],
      cursor: PropertyCursor,
      expectedValue: String,
      ps: DamlLfTypeLookup): Either[DotNotFailure, ProjectValue] =
    rootArgument.fold[Either[DotNotFailure, ProjectValue]](Right(StringValue("")))(
      checkValue(_, cursor, expectedValue, ps))

  def checkValue(
      rootArgument: ApiValue,
      cursor: PropertyCursor,
      expectedValue: String,
      ps: DamlLfTypeLookup): Either[DotNotFailure, ProjectValue] = {

    @annotation.tailrec
    def loop(argument: ApiValue, cursor: PropertyCursor): Either[DotNotFailure, ProjectValue] =
      argument match {
        case V.ValueContractId(value) if cursor.isLast => Right(StringValue(value))
        case V.ValueInt64(value) if cursor.isLast => Right(NumberValue(value))
        case V.ValueNumeric(value) if cursor.isLast => Right(StringValue(value.toUnscaledString))
        case V.ValueText(value) if cursor.isLast => Right(StringValue(value))
        case V.ValueParty(value) if cursor.isLast => Right(StringValue(value))
        case V.ValueBool(value) if cursor.isLast => Right(BooleanValue(value))
        case V.ValueUnit if cursor.isLast => Right(StringValue(""))
        case t: V.ValueTimestamp if cursor.isLast => Right(StringValue(t.toIso8601))
        case t: V.ValueDate if cursor.isLast => Right(StringValue(t.toIso8601))
        case V.ValueRecord(_, fields) =>
          cursor.next match {
            case None => Left(MustNotBeLastPart("record", cursor, expectedValue))
            case Some(nextCursor) =>
              val current: String = nextCursor.current
              fields.toSeq.collectFirst { case (Some(`current`), value) => value } match {
                case Some(nextField) => loop(nextField, nextCursor)
                case None => Left(UnknownProperty("record", nextCursor, expectedValue))
              }
          }
        case V.ValueVariant(_, constructor, value) =>
          cursor.next match {
            case None => Left(MustNotBeLastPart("variant", cursor, expectedValue))
            case Some(nextCursor) =>
              nextCursor.current match {
                case "__constructor" => Right(StringValue(constructor))
                case "__value" => loop(value, nextCursor)
                case `constructor` => loop(value, nextCursor)
                case _ => Left(UnknownProperty("variant", nextCursor, expectedValue))
              }
          }
        case V.ValueEnum(_, constructor) =>
          cursor.next match {
            case None => Left(MustNotBeLastPart("enum", cursor, expectedValue))
            case Some(nextCursor) =>
              nextCursor.current match {
                case "__constructor" => Right(StringValue(constructor))
                case _ => Left(UnknownProperty("enum", nextCursor, expectedValue))
              }
          }
        case V.ValueOptional(optValue) =>
          (cursor.next, optValue) match {
            case (None, None) => Right(StringValue("None"))
            case (None, Some(_)) => Right(StringValue("Some"))
            case (Some(nextCursor), Some(value)) if nextCursor.current == "Some" =>
              loop(value, nextCursor)
            case (Some(nextCursor), None) if nextCursor.current == "None" => Right(StringValue(""))
            case (Some(nextCursor), _) =>
              Left(UnknownProperty("optional", nextCursor, expectedValue))
          }
        case V.ValueList(elements) =>
          cursor.next match {
            case None => Left(MustNotBeLastPart("list", cursor, expectedValue))
            case Some(nextCursor) =>
              Try(nextCursor.current.toInt) match {
                case Success(index) => loop(elements.slowApply(index), nextCursor)
                case Failure(_) =>
                  Left(TypeCoercionFailure("list index", "int", cursor, cursor.current))
              }
          }
        case V.ValueTextMap(textMap) =>
          cursor.next match {
            case None => Left(MustNotBeLastPart("textmap", cursor, expectedValue))
            case Some(nextCursor) =>
              textMap.toImmArray.toSeq.collectFirst {
                case (k, v) if k == nextCursor.current => v
              } match {
                case Some(v) => loop(v, nextCursor)
                case None =>
                  Left(UnknownProperty(nextCursor.current, nextCursor, expectedValue))
              }
          }
        case V.ValueGenMap(entries) =>
          cursor.next match {
            case None =>
              Left(MustNotBeLastPart("genmap", cursor, expectedValue))
            case Some(nextCursor) =>
              Try(nextCursor.current.toInt) match {
                case Success(index) =>
                  nextCursor.next match {
                    case Some(nextNextCursor) if nextNextCursor.current == "key" =>
                      loop(entries(index)._1, nextNextCursor)
                    case Some(nextNextCursor) if nextNextCursor.current == "value" =>
                      loop(entries(index)._2, nextNextCursor)
                    case None =>
                      Left(UnknownProperty(nextCursor.current, nextCursor, expectedValue))
                  }
                case Failure(_) =>
                  Left(TypeCoercionFailure("GenMap index", "int", cursor, cursor.current))
              }
          }
      }
    loop(rootArgument, cursor.prev.get)
  }

  lazy val parameterProject =
    opaque[DamlLfType, ProjectValue, DamlLfTypeLookup]("parameter")((t, c, e, p) =>
      checkParameter(t, c, e, p))

  lazy val parameterIdProject =
    opaque[DamlLfIdentifier, ProjectValue, DamlLfTypeLookup]("parameter")((id, c, e, p) =>
      checkParameter(DamlLfTypeCon(DamlLfTypeConName(id), DamlLfImmArraySeq()), c, e, p))

  lazy val argumentProject =
    opaque[ApiValue, ProjectValue, DamlLfTypeLookup]("argument")(checkValue)

  lazy val keyProject =
    opaque[Option[ApiValue], ProjectValue, DamlLfTypeLookup]("key")(checkOptionalValue)

  lazy val templateProject =
    root[Template, ProjectValue, DamlLfTypeLookup]("template")
      .onLeaf("id")
      .onAnyValue
      .perform[String]((contract, _) => StringValue(contract.idString))
      .onBranch("parameter", _.id, parameterIdProject)
      .onBranch("choices", _.choices, choicesProject)

  lazy val contractProject =
    root[Contract, ProjectValue, DamlLfTypeLookup]("contract")
      .onLeaf("id")
      .onAnyValue
      .perform[String]((contract, _) => StringValue(contract.id.unwrap))
      .onBranch("template", _.template, templateProject)
      .onBranch("argument", _.argument, argumentProject)
      .onBranch("key", _.key, keyProject)
      .onLeaf("agreementText")
      .onAnyValue
      .perform[String]((contract, _) => StringValue(contract.agreementText.getOrElse("")))
      .onLeaf("signatories")
      .onAnyValue
      .perform[String]((contract, _) => StringValue(contract.signatories.mkString))
      .onLeaf("observers")
      .onAnyValue
      .perform[String]((contract, _) => StringValue(contract.observers.mkString))
      .onTree

  lazy val choicesProject =
    root[Seq[Choice], ProjectValue, DamlLfTypeLookup]("choices")
      .onElements[Choice](choice => Tag.unwrap(choice.name), choiceProject)

  lazy val choiceProject =
    root[Choice, ProjectValue, DamlLfTypeLookup]("choice")
      .onLeaf("name")
      .onAnyValue
      .perform[String]((choice, _) => StringValue(Tag.unwrap(choice.name)))
      .onLeaf("consuming")
      .onAnyValue
      .perform[String]((choice, _) => BooleanValue(choice.consuming))
      .onBranch("parameter", _.parameter, parameterProject)
      .onTree

  lazy val commandProject =
    root[Command, ProjectValue, DamlLfTypeLookup]("command")
      .onLeaf("id")
      .onAnyValue
      .perform[String]((command, _) => StringValue(Tag.unwrap(command.id)))
      .onLeaf("index")
      .onAnyValue
      .perform[String]((command, _) => NumberValue(command.index))
      .onLeaf("workflowId")
      .onAnyValue
      .perform[String]((command, _) => StringValue(Tag.unwrap(command.workflowId)))
      .onLeaf("platformTime")
      .onAnyValue
      .perform[String]((command, _) => StringValue(command.platformTime.toString))
      .onTree
}
