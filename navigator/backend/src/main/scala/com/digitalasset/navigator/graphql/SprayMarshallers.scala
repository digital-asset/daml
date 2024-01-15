// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.graphql

import sangria.marshalling._
import spray.json._

import scala.util.Try

/** Custom marshallers and unmarshallers for dealing with raw JSON values.
  *
  * The declared implicit objects replace the ones you would usually import from `sangria.marshalling.sprayJson`.
  * In fact they are largely identical to those except for the additional support for non-scalar JSON values.
  *
  * The entire object is only relevant as long as we use raw JSON values in the model.
  *
  * Adapted from https://gist.github.com/OlegIlyenko/5b96f4b54f656aac226d3c4bc33fd2a6
  */
@SuppressWarnings(Array("org.wartremover.warts.Serializable"))
object SprayMarshallers {
  implicit object CustomSprayJsonResultMarshaller extends ResultMarshaller {
    type Node = JsValue
    type MapBuilder = ArrayMapBuilder[Node]

    def emptyMapNode(keys: Seq[String]) = new ArrayMapBuilder[Node](keys)
    def addMapNodeElem(
        builder: MapBuilder,
        key: String,
        value: Node,
        optional: Boolean,
    ): ArrayMapBuilder[JsValue] = builder.add(key, value)

    def mapNode(builder: MapBuilder) = JsObject(builder.toMap)
    def mapNode(keyValues: Seq[(String, JsValue)]) = JsObject(keyValues: _*)

    def arrayNode(values: Vector[JsValue]) = JsArray(values)
    def optionalArrayNodeValue(value: Option[JsValue]): JsValue = value match {
      case Some(v) => v
      case None => nullNode
    }

    def scalarNode(value: Any, typeName: String, info: Set[ScalarValueInfo]): JsValue =
      value match {
        case v: String => JsString(v)
        case v: Boolean => JsBoolean(v)
        case v: Int => JsNumber(v)
        case v: Long => JsNumber(v)
        case v: Float => JsNumber(v.toDouble)
        case v: Double => JsNumber(v)
        case v: BigInt => JsNumber(v)
        case v: BigDecimal => JsNumber(v)
        case v: JsValue => v
        case v => throw new IllegalArgumentException("Unsupported scalar value: " + v.toString)
      }

    def enumNode(value: String, typeName: String) = JsString(value)

    def nullNode = JsNull

    def renderCompact(node: JsValue): String = node.compactPrint
    def renderPretty(node: JsValue): String = node.prettyPrint
  }

  implicit object SprayJsonMarshallerForType extends ResultMarshallerForType[JsValue] {
    val marshaller = CustomSprayJsonResultMarshaller
  }

  implicit object CustomSprayJsonInputUnmarshaller extends InputUnmarshaller[JsValue] {
    def getRootMapValue(node: JsValue, key: String): Option[JsValue] =
      node.asInstanceOf[JsObject].fields get key

    def isListNode(node: JsValue): Boolean = node.isInstanceOf[JsArray]
    def getListValue(node: JsValue): Seq[JsValue] = node.asInstanceOf[JsArray].elements

    def isMapNode(node: JsValue): Boolean = node.isInstanceOf[JsObject]
    def getMapValue(node: JsValue, key: String): Option[JsValue] =
      node.asInstanceOf[JsObject].fields get key
    def getMapKeys(node: JsValue): Iterable[String] = node.asInstanceOf[JsObject].fields.keys

    def isDefined(node: JsValue): Boolean = node != JsNull
    def getScalarValue(node: JsValue): Any = node match {
      case JsBoolean(b) => b
      case JsNumber(d) => d.toBigIntExact getOrElse d
      case JsString(s) => s
      case n => n
    }

    def getScalaScalarValue(node: JsValue): Any = getScalarValue(node)

    def isEnumNode(node: JsValue): Boolean = node.isInstanceOf[JsString]

    def isScalarNode(node: JsValue) = true

    def isVariableNode(node: JsValue) = false
    def getVariableName(node: JsValue) =
      throw new IllegalArgumentException("variables are not supported")

    def render(node: JsValue): String = node.compactPrint
  }

  private object CustomSprayJsonToInput extends ToInput[JsValue, JsValue] {
    def toInput(value: JsValue): (JsValue, CustomSprayJsonInputUnmarshaller.type) =
      (value, CustomSprayJsonInputUnmarshaller)
  }

  private object CustomSprayJsonFromInput extends FromInput[JsValue] {
    val marshaller = CustomSprayJsonResultMarshaller
    def fromResult(node: marshaller.Node): JsValue = node
  }

  implicit def sprayJsonToInput[T <: JsValue]: ToInput[T, JsValue] =
    CustomSprayJsonToInput.asInstanceOf[ToInput[T, JsValue]]

  implicit def sprayJsonFromInput[T <: JsValue]: FromInput[T] =
    CustomSprayJsonFromInput.asInstanceOf[FromInput[T]]

  implicit def sprayJsonWriterToInput[T: JsonWriter]: ToInput[T, JsValue] =
    new ToInput[T, JsValue] {
      def toInput(value: T): (JsValue, CustomSprayJsonInputUnmarshaller.type) =
        implicitly[JsonWriter[T]].write(value) -> CustomSprayJsonInputUnmarshaller
    }

  implicit def sprayJsonReaderFromInput[T: JsonReader]: FromInput[T] =
    new FromInput[T] {
      val marshaller = CustomSprayJsonResultMarshaller
      def fromResult(node: marshaller.Node): T =
        try implicitly[JsonReader[T]].read(node)
        catch {
          case e: DeserializationException => throw InputParsingError(Vector(e.msg))
        }
    }

  implicit object CustomSprayJsonInputParser extends InputParser[JsValue] {
    def parse(str: String) = Try(str.parseJson)
  }
}
