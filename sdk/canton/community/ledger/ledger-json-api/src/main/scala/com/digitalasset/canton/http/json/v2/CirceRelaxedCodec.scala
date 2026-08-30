// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import io.circe.Decoder.Result
import io.circe.generic.extras.codec.ConfiguredAsObjectCodec
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import io.circe.{Codec, HCursor, Json, JsonObject}
import shapeless.Lazy

import reflect.runtime.universe
import universe.*

/** This codec automatically fills missing json attributes as long as they are Seq, Option or Map.
  *
  * This emulates to some extent gRPC / proto behaviour, where all fields are optional.
  *
  * We should use:
  *   - deriveRelaxedCodec:
  *     - for gRPC scalaPb generated case classes
  *   - deriveConfiguredCodec:
  *     - for JS mirrors of gRPC classes
  *     - for proto base lib classes (not generated), (they have default value)
  *     - for gRPC scalaPb generated roots of ADT
  *
  * For scalar types that should have default values, you can use the extension
  * deriveRelaxedCodedWithDefaults which takes a map of default values.
  *
  * DO NOT USE deriveRelaxedCodec FOR Protobuf ENUM.
  *
  * You will likely get: 'JSON decoding to CNil should never happen'. In this case, create separate
  * Encoder / Decoder using stringDecoderForEnum / stringEncoderForEnum.
  *
  * Do not forget to also create a Schema for the openapi.yml encoding. Otherwise, your enum will be
  * translated into objects and you will likely end up with inconsistencies between Tapir and the
  * openapi.yml.
  */
object CirceRelaxedCodec {

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  def deriveRelaxedCodec[T: TypeTag](implicit
      c: Lazy[ConfiguredAsObjectCodec[T]]
  ): Codec.AsObject[T] = {
    val codec = deriveConfiguredCodec[T]
    new Codec.AsObject[T] {
      override def encodeObject(value: T): JsonObject = codec.encodeObject(value)

      /* caches a "prefilled" nullable fields for this type */
      private val initiallyEmptyProperties: Map[String, Json] = {
        val tpe = typeOf[T]

        // We just iterate all fields that are case class properties
        val fields = tpe.decls.collect {
          case m: MethodSymbol if m.isCaseAccessor =>
            m.name.toString -> m.returnType
        }
        fields
          .map {
            case (name, aType) if aType <:< universe.typeOf[Seq[Any]] =>
              Some((name, Json.arr()))
            case (name, aType) if aType <:< universe.typeOf[Option[Any]] => Some((name, Json.Null))
            case (name, aType) if universe.typeOf[Boolean] =:= aType => Some((name, Json.False))
            case (name, aType) if universe.typeOf[Int] =:= aType => Some((name, Json.fromInt(0)))
            case (name, aType) if universe.typeOf[Long] =:= aType => Some((name, Json.fromLong(0L)))
            case (name, aType) if universe.typeOf[Double] =:= aType =>
              Some((name, Json.fromDoubleOrNull(0.0)))
            case (name, aType) if universe.typeOf[String] =:= aType =>
              Some((name, Json.fromString("")))
            case (name, aType) if universe.typeOf[Float] =:= aType =>
              Some((name, Json.fromFloatOrNull(0.0f)))
            case (name, aType) if aType <:< universe.typeOf[Map[?, ?]] =>
              Some((name, Json.obj()))
            case (name, aType) if universe.typeOf[com.google.protobuf.ByteString] =:= aType =>
              Some((name, Json.fromString("")))
            case (name, aType) if aType <:< universe.typeOf[scalapb.GeneratedEnum] =>
              Some(
                (
                  name,
                  Json.obj("Unrecognized" -> Json.obj("unrecognizedValue" -> Json.fromInt(0))),
                )
              )
            case (name, aType) if aType <:< universe.typeOf[scalapb.GeneratedOneof] =>
              Some((name, Json.obj("Empty" -> Json.obj())))
            case _ => None
          }
          .collect { case Some((name, v)) =>
            (name, v)
          }
          .toMap
      }

      override def apply(c: HCursor): Result[T] = {
        val updatedJson = c.value.mapObject { jsonObj =>
          val objMap: Map[String, Json] = jsonObj.toMap
          val resultMap = initiallyEmptyProperties ++ objMap
          JsonObject.fromMap(resultMap)
        }
        codec.decodeJson(updatedJson)
      }
    }
  }

  /** derived codec that supports using default scalar values */
  def deriveRelaxedCodecWithDefaults[T: TypeTag](defaults: Map[String, Json])(implicit
      c: Lazy[ConfiguredAsObjectCodec[T]]
  ): Codec.AsObject[T] = {
    val codec = deriveRelaxedCodec[T]
    new Codec.AsObject[T] {
      override def encodeObject(value: T): JsonObject = codec.encodeObject(value)
      override def apply(c: HCursor): Result[T] =
        codec.decodeJson(c.value.mapObject { jsonObj =>
          val objMap = jsonObj.toMap
          val resultMap = defaults ++ objMap
          JsonObject.fromMap(resultMap)
        })
    }

  }

}
