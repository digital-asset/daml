// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import io.circe.Decoder.Result
import io.circe.generic.extras.codec.ConfiguredAsObjectCodec
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import io.circe.{Codec, HCursor, Json, JsonObject}
import shapeless.Lazy

import scala.reflect.runtime.universe.*

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
            m.name.toString -> m.returnType.toString
        }
        fields
          .map {
            case (name, aType) if aType.startsWith("Seq[") => Some((name, Json.arr()))
            case (name, aType) if aType.startsWith("Option[") => Some((name, Json.Null))
            case (name, aType)
                if aType
                  .startsWith("scala.collection.immutable.Map[") || aType.startsWith("Map[") =>
              // The name depends on how the map was defined in a class just Map, or with a full package name,
              // unfortunately normalizing this (along with Seq and Option) made this code complex - so I stayed with a simple solution
              Some((name, Json.obj()))
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
}
