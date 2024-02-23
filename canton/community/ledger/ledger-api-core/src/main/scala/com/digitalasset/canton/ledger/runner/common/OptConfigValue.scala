// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.runner.common

import com.typesafe.config.{ConfigObject, ConfigValue, ConfigValueFactory}
import pureconfig.error.{ConfigReaderFailures, UnknownKey}
import pureconfig.generic.ProductHint
import pureconfig.{ConfigConvert, ConfigCursor, ConfigObjectCursor, ConfigReader, ConfigWriter}

object OptConfigValue {
  val enabledKey = "enabled"

  /** Reads configuration object of `T` and `enabled` flag to find out if this object has values.
    */
  def optReaderEnabled[T](reader: ConfigReader[T]): ConfigReader[Option[T]] =
    (cursor: ConfigCursor) =>
      for {
        objCur <- cursor.asObjectCursor
        enabledCur <- objCur.atKey(enabledKey)
        enabled <- enabledCur.asBoolean
        value <-
          if (enabled) {
            reader.from(cursor).map(x => Some(x))
          } else {
            Right(None)
          }
      } yield value

  /** Writes object of `T` and adds `enabled` flag for configuration which contains value.
    */
  def optWriterEnabled[T](writer: ConfigWriter[T]): ConfigWriter[Option[T]] = {
    import scala.jdk.CollectionConverters.*
    def toConfigValue(enabled: Boolean) =
      ConfigValueFactory.fromMap(Map(enabledKey -> enabled).asJava)

    (optValue: Option[T]) =>
      optValue match {
        case Some(value) =>
          writer.to(value) match {
            // if serialised object of `T` is `ConfigObject` and
            // has `enabled` inside, it cannot be supported by this writer
            case configObject: ConfigObject if configObject.toConfig.hasPath(enabledKey) =>
              throw new IllegalArgumentException(
                s"Ambiguous configuration, object contains `${enabledKey}` flag"
              )
            case _ =>
              writer.to(value).withFallback(toConfigValue(enabled = true))
          }
        case None => toConfigValue(enabled = false)
      }
  }

  def optConvertEnabled[T](
      reader: ConfigReader[T],
      writer: ConfigWriter[T],
  ): ConfigConvert[Option[T]] =
    ConfigConvert.apply(optReaderEnabled(reader), optWriterEnabled(writer))

  def optConvertEnabled[T](convert: ConfigConvert[T]): ConfigConvert[Option[T]] =
    optConvertEnabled(convert, convert)

  class OptProductHint[T](allowUnknownKeys: Boolean) extends ProductHint[T] {
    val hint = ProductHint[T](allowUnknownKeys = allowUnknownKeys)

    override def from(cursor: ConfigObjectCursor, fieldName: String): ProductHint.Action =
      hint.from(cursor, fieldName)

    override def bottom(
        cursor: ConfigObjectCursor,
        usedFields: Set[String],
    ): Option[ConfigReaderFailures] = if (allowUnknownKeys)
      None
    else {
      val unknownKeys = cursor.map.toList.collect {
        case (k, keyCur) if !usedFields.contains(k) && k != enabledKey =>
          keyCur.failureFor(UnknownKey(k))
      }
      unknownKeys match {
        case h :: t => Some(ConfigReaderFailures(h, t*))
        case Nil => None
      }
    }

    override def to(value: Option[ConfigValue], fieldName: String): Option[(String, ConfigValue)] =
      hint.to(value, fieldName)
  }

  def optProductHint[T](allowUnknownKeys: Boolean): OptProductHint[T] = new OptProductHint[T](
    allowUnknownKeys = allowUnknownKeys
  )

}
