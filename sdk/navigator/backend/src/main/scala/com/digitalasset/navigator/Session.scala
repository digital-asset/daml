// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator

import com.daml.navigator.model.PartyState
import scalaz.Tag

import scala.collection.immutable
import spray.json._

import scala.reflect.ClassTag

final case class User(
    id: String,
    party: PartyState,
    // TODO: where is `role` used? frontend has some references, but doesn't seem to impact anything?
    role: Option[String] = None,
    canAdvanceTime: Boolean = true,
)

sealed trait SignInError
case object InvalidCredentials extends SignInError
case object NotConnected extends SignInError
case object Unresponsive extends SignInError
case object Unknown extends SignInError

sealed abstract class Status
final case class Session(user: User) extends Status
final case class SignIn(method: SignInMethod, error: Option[SignInError] = None) extends Status

sealed abstract class SignInMethod
final case class SignInSelect(userIds: Set[String]) extends SignInMethod

case class LoginRequest(userId: String)

/** Maintains session information for browser clients. */
object Session {
  private var sessions: immutable.Map[String, Session] = immutable.Map()

  def current(sessionId: String): Option[Session] = sessions.get(sessionId)

  def open(
      sessionId: String,
      userId: String,
      userRole: Option[String],
      state: PartyState,
  ): Session = {
    val user = Session(User(userId, state, userRole))
    sessions += sessionId -> user
    user
  }

  def close(sessionId: String): Unit = {
    sessions -= sessionId
  }

  // method for debugging and testing purposes
  private[navigator] def clean(): Unit = synchronized {
    this.sessions = immutable.Map()
  }
}

object SessionJsonProtocol extends DefaultJsonProtocol {
  val typeFieldName = "type"
  val userFieldName = "user"
  val usersFieldName = "users"
  val methodFieldName = "method"
  val errorFieldName = "error"
  val idFieldName = "id"
  val roleFieldName = "role"
  val partyFieldName = "party"
  val canAdvanceTimeFieldName = "canAdvanceTime"
  val sessionType = JsString("session")
  val selectType = JsString("select")
  val signInType = JsString("sign-in")

  implicit object partyWriter extends RootJsonWriter[PartyState] {
    override def write(obj: PartyState): JsValue = Tag.unwrap(obj.name).toJson
  }

  implicit object userWriter extends RootJsonWriter[User] {
    override def write(obj: User): JsValue =
      obj.role match {
        case None =>
          JsObject(
            idFieldName -> obj.id.toJson,
            partyFieldName -> obj.party.toJson,
            canAdvanceTimeFieldName -> obj.canAdvanceTime.toJson,
          )
        case Some(role) =>
          JsObject(
            idFieldName -> obj.id.toJson,
            roleFieldName -> role.toJson,
            partyFieldName -> obj.party.toJson,
            canAdvanceTimeFieldName -> obj.canAdvanceTime.toJson,
          )
      }
  }

  implicit object loginRequestFormat extends RootJsonFormat[LoginRequest] {
    override def write(obj: LoginRequest): JsValue =
      JsObject("userId" -> obj.userId.toJson)

    override def read(json: JsValue): LoginRequest = {
      val obj =
        json.asJsObject(errorMsg =
          s"LoginRequest should be an object but ${json.getClass.getCanonicalName} found"
        )
      obj.fields.get("userId") match {
        case Some(JsString(userId)) => LoginRequest(userId)
        case _ =>
          throw DeserializationException(
            s"LoginRequest should contain a field called 'userId' of type String, got $json"
          )
      }
    }
  }

  implicit object sessionWriter extends RootJsonWriter[Session] {
    override def write(obj: Session): JsValue = {
      JsObject(
        typeFieldName -> sessionType,
        userFieldName -> obj.user.toJson,
      )
    }
  }

  implicit object statusWriter extends RootJsonWriter[Status] {
    override def write(obj: Status): JsValue =
      obj match {
        case session: Session => sessionWriter.write(session)
        case signIn: SignIn => signInFormat.write(signIn)
      }
  }

  implicit object signInSelectJsonFormat extends RootJsonFormat[SignInSelect] {

    def fromFields(fields: Map[String, JsValue]): SignInSelect =
      fields.get(usersFieldName) match {
        case None =>
          throw DeserializationException(
            s"Cannot decode an instance of " +
              s"${SignInSelect.getClass.getCanonicalName} because the field $usersFieldName is missing"
          )
        case Some(rawUserIds) =>
          SignInSelect(userIds = rawUserIds.convertTo[Set[String]])
      }

    override def read(json: JsValue): SignInSelect =
      deserialize2(json, selectType)(fromFields)

    override def write(obj: SignInSelect): JsValue =
      JsObject(
        typeFieldName -> selectType,
        usersFieldName -> obj.userIds.toJson,
      )
  }

  implicit object signInMethodFormat extends JsonFormat[SignInMethod] {
    override def read(json: JsValue): SignInMethod =
      deserialize(json) {
        case (`selectType`, fields) =>
          signInSelectJsonFormat.fromFields(fields)
        case _ => throw DeserializationException(s"Unknown sign in method")
      }

    override def write(obj: SignInMethod): JsValue = obj match {
      case select: SignInSelect =>
        select.toJson
    }
  }

  implicit object signInFormat extends RootJsonFormat[SignIn] {
    override def write(obj: SignIn): JsValue =
      obj.error.fold(
        JsObject(
          typeFieldName -> signInType,
          methodFieldName -> obj.method.toJson,
        )
      )(error =>
        JsObject(
          typeFieldName -> signInType,
          methodFieldName -> obj.method.toJson,
          errorFieldName -> JsString(error match {
            case InvalidCredentials => "invalid-credentials"
            case NotConnected => "not-connected"
            case Unresponsive => "unresponsive"
            case Unknown => "unknown-error"
          }),
        )
      )

    def fromFields(fields: Map[String, JsValue]): SignIn = {
      (fields.get(methodFieldName), fields.get(errorFieldName)) match {
        case (None, _) =>
          throw DeserializationException(
            s"Cannot decode an instance of " +
              s"${SignIn.getClass.getCanonicalName} because the field $methodFieldName is missing"
          )
        case (Some(rawMethod), maybeError) =>
          val method = signInMethodFormat.read(rawMethod)
          SignIn(
            method,
            maybeError.map {
              case JsString("invalid-credentials") => InvalidCredentials
              case JsString("not-connected") => NotConnected
              case JsString("unresponsive") => Unresponsive
              case _ => Unknown
            },
          )
      }
    }

    override def read(json: JsValue): SignIn =
      deserialize2(json, signInType)(fromFields)
  }

  /** Convert a json object following our encoding to an instance of `T` by using a function from the 'type' and the
    * fields in the json object to the type `T`. Throws `DeserializationException` if the conversion is not possible.
    *
    * Our encoding of objects follows the rules:
    * - the encoded json must be a `JsObject`
    * - the encoded json must have a field `'type'` with the name of the type encoded. `T` must match it.
    */
  private def deserialize[T](
      json: JsValue
  )(f: (JsString, Map[String, JsValue]) => T)(implicit tag: ClassTag[T]): T = {
    val jsObject =
      json
        .asJsObject(errorMsg =
          s"JSON object required to deserialize an instance of ${tag.runtimeClass.getCanonicalName}"
        )
    val typeValue = fieldOrThrow(jsObject, typeFieldName)(jsValueAsJsString)
    f(typeValue, jsObject.fields)
  }

  /** Similar to [[deserialize]] but checks that the fields 'type' has the expected type.
    */
  private def deserialize2[T](json: JsValue, typeExpected: JsString)(
      f: Map[String, JsValue] => T
  )(implicit tag: ClassTag[T]): T = {
    deserialize[T](json) { case (typeStr, fields) =>
      if (typeExpected != typeStr) {
        throw new DeserializationException(
          s"Cannot deserialize an instance of ${tag.runtimeClass.getCanonicalName} because the wrong type" +
            s"${typeStr.prettyPrint} has been found instead of the expected type ${typeExpected.prettyPrint}"
        )
      } else {
        f(fields)
      }
    }
  }

  /** Tries to retrieve a field from a `jsObject` and convert it to `T` if `f` is given. Throws
    * `DeserializationException` if the `jsObject` doesn't contain the field
    */
  private def fieldOrThrow[T](jsObject: JsObject, fieldName: String)(
      f: JsValue => T
  )(implicit tag: ClassTag[T]): T =
    jsObject.fields
      .get(fieldName)
      .map(f)
      .getOrElse(
        throw DeserializationException(
          s"JSON object with a '$fieldName' field is required to deserialize an " +
            s"instance of ${tag.runtimeClass.getCanonicalName}. jsObject found ${jsObject.compactPrint}"
        )
      )

  /** Convert the jsValue to a `JsString` if jsValue is a `JsString`, otherwise throws `DeserializationException`
    */
  private def jsValueAsJsString(jsValue: JsValue): JsString =
    jsValue match {
      case js: JsString => js
      case _ =>
        throw DeserializationException(
          s"JsString expected, but found ${jsValue.compactPrint} of type " +
            jsValue.getClass.getCanonicalName
        )
    }
}
