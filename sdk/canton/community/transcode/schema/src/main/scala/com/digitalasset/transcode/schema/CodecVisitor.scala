// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.schema

import com.digitalasset.transcode.schema.CodecVisitor.*

trait CodecVisitor[T]
    extends SchemaVisitor.WithResult[Dictionary[com.digitalasset.transcode.Codec[T]]]:
  final type Type = Codec[T]

  override final def collect(entities: Seq[Template[Type]]): Result =
    Dictionary(
      entities.map(
        _.map(codec =>
          new com.digitalasset.transcode.Codec[T] {
            def fromDynamicValue(dv: DynamicValue): T = codec.fromDynamicValue(dv)(using Map.empty)
            def toDynamicValue(v: T): DynamicValue = codec.toDynamicValue(v)(using Map.empty)
          }
        )
      )
    )

  override final def constructor(
      id: Identifier,
      typeParams: Seq[TypeVarName],
      value: => Type,
  ): Type = new Type {
    private lazy val delegate = value
    override def isOptional(using VarMap[Decoder[T]]): Boolean = delegate.isOptional
    def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[T]]): T =
      delegate.fromDynamicValue(dv)
    def toDynamicValue(v: T)(using VarMap[Decoder[T]]): DynamicValue = delegate.toDynamicValue(v)
  }
  override final def variable(name: TypeVarName): Type = new Type {
    override def isOptional(using varMap: VarMap[Decoder[T]]): Boolean =
      varMap(name).isOptional
    def fromDynamicValue(dv: DynamicValue)(using varMap: VarMap[Encoder[T]]): T =
      varMap(name).fromDynamicValue(dv)
    def toDynamicValue(v: T)(using varMap: VarMap[Decoder[T]]): DynamicValue =
      varMap(name).toDynamicValue(v)
  }
  override final def application(value: Type, typeParams: Seq[TypeVarName], args: Seq[Type]): Type =
    new Type {
      override def isOptional(using VarMap[Decoder[T]]): Boolean =
        value.isOptional(using newDecoderMap)

      def fromDynamicValue(dv: DynamicValue)(using varMap: VarMap[Encoder[T]]): T =
        value.fromDynamicValue(dv)(using newEncoderMap)

      def toDynamicValue(v: T)(using varMap: VarMap[Decoder[T]]): DynamicValue =
        value.toDynamicValue(v)(using newDecoderMap)

      private def newEncoderMap(using varMap: VarMap[Encoder[T]]): VarMap[Encoder[T]] =
        val appliedArgs = args.map(self =>
          new Encoder[T] {
            def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[T]]): T =
              self.fromDynamicValue(dv)(using varMap)
          }
        )
        (typeParams zip appliedArgs).toMap

      private def newDecoderMap(using varMap: VarMap[Decoder[T]]): VarMap[Decoder[T]] =
        val appliedArgs = args.map(self =>
          new Decoder[T] {
            override def isOptional(using VarMap[Decoder[T]]): Boolean =
              self.isOptional(using varMap)
            def toDynamicValue(v: T)(using VarMap[Decoder[T]]): DynamicValue =
              self.toDynamicValue(v)(using varMap)
          }
        )
        (typeParams zip appliedArgs).toMap
    }

  extension [A](array: Array[A])
    inline def getMaybe(ix: Int): A =
      if ix < array.length then array(ix)
      else throw Exception(s"Unexpected case: $ix")
  extension [K, V](map: Map[K, V])
    inline def getMaybe(key: K): V =
      map.getOrElse(key, throw Exception(s"Unexpected case: $key"))

end CodecVisitor

object CodecVisitor:
  trait Codec[A] extends Encoder[A] with Decoder[A]
  trait Encoder[A] {
    def fromDynamicValue(dv: DynamicValue)(using VarMap[Encoder[A]]): A
  }
  trait Decoder[A] {
    def toDynamicValue(v: A)(using VarMap[Decoder[A]]): DynamicValue
    def isOptional(using VarMap[Decoder[A]]): Boolean = false
  }
  type VarMap[A] = Map[TypeVarName, A]
end CodecVisitor
