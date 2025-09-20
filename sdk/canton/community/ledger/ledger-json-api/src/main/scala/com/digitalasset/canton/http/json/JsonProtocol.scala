// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.daml.struct.spray.StructJsonFormat
import com.digitalasset.canton.http
import com.digitalasset.canton.http.*
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.struct.Struct
import org.apache.pekko.http.scaladsl.model.StatusCode
import scalaz.syntax.tag.*
import scalaz.{@@, NonEmptyList, Tag}
import spray.json.*

import scala.reflect.ClassTag

object JsonProtocol extends JsonProtocolLow {

  private def taggedJsonFormat[A: JsonFormat, T]: JsonFormat[A @@ T] =
    Tag.subst(implicitly[JsonFormat[A]])

  implicit val PartyFormat: JsonFormat[http.Party] =
    taggedJsonFormat

  /** This intuitively pointless extra type is here to give it specificity so this instance will
    * beat CollectionFormats#listFormat. You would normally achieve the conflict resolution by
    * putting this instance in a parent of
    * [[https://javadoc.io/static/io.spray/spray-json_2.12/1.3.5/spray/json/CollectionFormats.html CollectionFormats]],
    * but that kind of extension isn't possible here.
    */
  final class JsonReaderList[A: JsonReader] extends JsonReader[List[A]] {
    override def read(json: JsValue) = json match {
      case JsArray(elements) => elements.iterator.map(_.convertTo[A]).toList
      case _ => deserializationError(s"must be a list, but got $json")
    }
  }

  implicit def `List reader only`[A: JsonReader]: JsonReaderList[A] = new JsonReaderList

  private def jsonFormatFromADT[T: ClassTag](
      fromJs: String => JsObject => T,
      toJs: T => JsValue,
  ): JsonFormat[T] =
    new JsonFormat[T] {
      private val typeDiscriminatorKeyName = "type"

      override def read(json: JsValue): T = {
        val fields = json.asJsObject().fields
        val typeValue = fields.getOrElse(
          typeDiscriminatorKeyName,
          deserializationError(
            s"${implicitly[ClassTag[T]].runtimeClass.getSimpleName} must have a `$typeDiscriminatorKeyName` field"
          ),
        ) match {
          case JsString(value) => value
          case other =>
            deserializationError(
              s"field with key name `$typeDiscriminatorKeyName` must be a JsString, got $other"
            )
        }
        val jsValueWithoutType = JsObject(fields - typeDiscriminatorKeyName)
        fromJs(typeValue)(jsValueWithoutType)
      }
      override def write(obj: T): JsObject = {
        val typeName = obj.getClass.getSimpleName.replace("$", "")
        val jsObj = toJs(obj)
        jsObj.asJsObject.copy(fields =
          jsObj.asJsObject.fields + (typeDiscriminatorKeyName -> JsString(typeName))
        )
      }
    }

  implicit val hexStringFormat: JsonFormat[Ref.HexString] =
    xemapStringJsonFormat(Ref.HexString.fromString)(identity)

  implicit val StatusCodeFormat: RootJsonFormat[StatusCode] =
    new RootJsonFormat[StatusCode] {
      override def read(json: JsValue): StatusCode = json match {
        case JsNumber(x) => StatusCode.int2StatusCode(x.toIntExact)
        case _ => deserializationError(s"Expected JsNumber, got: $json")
      }

      override def write(obj: StatusCode): JsValue = JsNumber(obj.intValue)
    }

  implicit val ResourceInfoDetailFormat: RootJsonFormat[http.ResourceInfoDetail] = jsonFormat2(
    http.ResourceInfoDetail.apply
  )
  implicit val ErrorInfoDetailFormat: RootJsonFormat[http.ErrorInfoDetail] = jsonFormat2(
    http.ErrorInfoDetail.apply
  )

  implicit val durationFormat: JsonFormat[http.RetryInfoDetailDuration] =
    jsonFormat[http.RetryInfoDetailDuration](
      JsonReader.func2Reader(
        (LongJsonFormat.read _)
          .andThen(scala.concurrent.duration.Duration.fromNanos)
          .andThen(it => http.RetryInfoDetailDuration(it: scala.concurrent.duration.Duration))
      ),
      JsonWriter.func2Writer[http.RetryInfoDetailDuration](duration =>
        LongJsonFormat.write(duration.unwrap.toNanos)
      ),
    )

  implicit val RetryInfoDetailFormat: RootJsonFormat[http.RetryInfoDetail] =
    jsonFormat1(http.RetryInfoDetail.apply)

  implicit val RequestInfoDetailFormat: RootJsonFormat[http.RequestInfoDetail] = jsonFormat1(
    http.RequestInfoDetail.apply
  )

  implicit val ErrorDetailsFormat: JsonFormat[http.ErrorDetail] = {
    val resourceInfoDetailTypeName = classOf[ResourceInfoDetail].getSimpleName
    val errorInfoDetailTypeName = classOf[ErrorInfoDetail].getSimpleName
    val retryInfoDetailTypeName = classOf[RetryInfoDetail].getSimpleName
    val requestInfoDetailTypeName = classOf[RequestInfoDetail].getSimpleName

    jsonFormatFromADT(
      {
        case `resourceInfoDetailTypeName` => ResourceInfoDetailFormat.read(_)
        case `errorInfoDetailTypeName` => ErrorInfoDetailFormat.read(_)
        case `retryInfoDetailTypeName` => RetryInfoDetailFormat.read(_)
        case `requestInfoDetailTypeName` => RequestInfoDetailFormat.read(_)
        case typeName => deserializationError(s"Unknown error detail type: $typeName")
      },
      {
        case resourceInfoDetail: ResourceInfoDetail =>
          ResourceInfoDetailFormat.write(resourceInfoDetail)
        case errorInfoDetail: ErrorInfoDetail => ErrorInfoDetailFormat.write(errorInfoDetail)
        case retryInfoDetail: RetryInfoDetail => RetryInfoDetailFormat.write(retryInfoDetail)
        case requestInfoDetail: RequestInfoDetail =>
          RequestInfoDetailFormat.write(requestInfoDetail)
      },
    )
  }

  implicit val LedgerApiErrorFormat: RootJsonFormat[http.LedgerApiError] =
    jsonFormat3(http.LedgerApiError.apply)

  implicit val ErrorResponseFormat: RootJsonFormat[http.ErrorResponse] =
    jsonFormat3(http.ErrorResponse.apply)

  implicit val StructFormat: RootJsonFormat[Struct] = StructJsonFormat

  // xmap with an error case for StringJsonFormat
  def xemapStringJsonFormat[A](readFn: String => Either[String, A])(
      writeFn: A => String
  ): RootJsonFormat[A] = new RootJsonFormat[A] {
    private[this] val base = implicitly[JsonFormat[String]]
    override def write(obj: A): JsValue = base.write(writeFn(obj))
    override def read(json: JsValue): A =
      readFn(base.read(json)).fold(deserializationError(_), identity)
  }
}

sealed abstract class JsonProtocolLow extends DefaultJsonProtocol {
  implicit def NonEmptyListReader[A: JsonReader]: JsonReader[NonEmptyList[A]] = {
    case JsArray(hd +: tl) =>
      NonEmptyList(hd.convertTo[A], tl map (_.convertTo[A]): _*)
    case _ => deserializationError("must be a JSON array with at least 1 element")
  }

  implicit def NonEmptyListWriter[A: JsonWriter]: JsonWriter[NonEmptyList[A]] =
    nela => JsArray(nela.map(_.toJson).list.toVector)
}
