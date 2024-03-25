// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import java.nio.charset.StandardCharsets
import scala.util.matching.Regex

object ResourceAnnotationValidator {

  // NOTE: These constraints are based on constraints K8s uses for their annotations and labels
  private val NamePattern = "([a-zA-Z0-9]+[a-zA-Z0-9-]*)?[a-zA-Z0-9]+"
  private val KeySegmentRegex: Regex = "^([a-zA-Z0-9]+[a-zA-Z0-9.\\-_]*)?[a-zA-Z0-9]+$".r
  private val DnsSubdomainRegex: Regex = ("^(" + NamePattern + "[.])*" + NamePattern + "$").r
  val MaxAnnotationsSizeInKiloBytes: Int = 256
  private val MaxAnnotationsSizeInBytes: Int = MaxAnnotationsSizeInKiloBytes * 1024

  sealed trait MetadataAnnotationsError {
    def reason: String
  }
  case object AnnotationsSizeExceededError extends MetadataAnnotationsError {
    override val reason =
      s"Max annotations size of ${MaxAnnotationsSizeInKiloBytes}kb has been exceeded"
  }
  final case class InvalidAnnotationsKeyError(override val reason: String)
      extends MetadataAnnotationsError
  final case class EmptyAnnotationsValueError(private val key: String)
      extends MetadataAnnotationsError {
    override val reason = s"The value of an annotation is empty for key: '${shorten(key)}'"
  }

  /** @return a Left(actualSizeInBytes) in case of a failed validation
    */
  def isWithinMaxAnnotationsByteSize(annotations: Map[String, String]): Boolean = {
    val totalSizeInBytes = annotations.iterator.foldLeft(0L) { case (size, (key, value)) =>
      val keySize = key.getBytes(StandardCharsets.UTF_8).length
      val valSize = value.getBytes(StandardCharsets.UTF_8).length
      size + keySize + valSize
    }
    totalSizeInBytes <= MaxAnnotationsSizeInBytes
  }

  def validateAnnotationsFromApiRequest(
      annotations: Map[String, String],
      allowEmptyValues: Boolean,
  ): Either[MetadataAnnotationsError, Unit] = {
    val nonEmptyValued = annotations.view.filter { case (_, value) => value.nonEmpty }.toMap
    for {
      _ <-
        Either.cond(
          isWithinMaxAnnotationsByteSize(nonEmptyValued),
          (),
          AnnotationsSizeExceededError,
        )
      _ <- validateAnnotationKeys(annotations)
      _ <- if (allowEmptyValues) Right(()) else validateAnnotationsValues(annotations)
    } yield ()
  }

  private def validateAnnotationsValues(
      annotations: Map[String, String]
  ): Either[MetadataAnnotationsError, Unit] = {
    annotations.view.iterator.foldLeft(Right(()): Either[MetadataAnnotationsError, Unit]) {
      case (acc, (key, value)) =>
        for {
          _ <- acc
          _ <- if (value.isEmpty) Left(EmptyAnnotationsValueError(key = key)) else Right(())
        } yield ()
    }
  }

  private def validateAnnotationKeys(
      annotations: Map[String, String]
  ): Either[MetadataAnnotationsError, Unit] = {
    annotations.keys.iterator.foldLeft(Right(()): Either[MetadataAnnotationsError, Unit]) {
      (acc, key) =>
        for {
          _ <- acc
          _ <- isValidKey(key)
        } yield ()
    }
  }

  private def isValidKey(key: String): Either[MetadataAnnotationsError, Unit] = {
    key.split('/') match {
      case Array(name) => isValidKeyNameSegment(name)
      case Array(prefix, name) =>
        for {
          _ <- isValidKeyPrefixSegment(prefix)
          _ <- isValidKeyNameSegment(name)
        } yield ()
      case _ =>
        Left(
          InvalidAnnotationsKeyError(
            s"Key '${shorten(key)}' contains more than one forward slash ('/') character"
          )
        )
    }
  }

  private def isValidKeyPrefixSegment(
      prefixSegment: String
  ): Either[InvalidAnnotationsKeyError, Unit] = {
    if (prefixSegment.length > 253) {
      Left(
        InvalidAnnotationsKeyError(
          s"Key prefix segment '${shorten(prefixSegment)}' exceeds maximum length of 253 characters"
        )
      )
    } else {
      if (DnsSubdomainRegex.matches(prefixSegment)) {
        Right(())
      } else {
        Left(
          InvalidAnnotationsKeyError(
            s"Key prefix segment '${shorten(prefixSegment)}' has invalid syntax"
          )
        )
      }
    }
  }

  private def isValidKeyNameSegment(
      nameSegment: String
  ): Either[InvalidAnnotationsKeyError, Unit] = {
    if (nameSegment.length > 63) {
      Left(
        InvalidAnnotationsKeyError(
          s"Key name segment '${shorten(nameSegment)}' exceeds maximum length of 63 characters"
        )
      )
    } else {
      if (KeySegmentRegex.matches(nameSegment)) {
        Right(())
      } else {
        Left(
          InvalidAnnotationsKeyError(
            s"Key name segment '${shorten(nameSegment)}' has invalid syntax"
          )
        )
      }
    }
  }

  private def shorten(s: String): String =
    if (s.length > 53) { s.take(50) + "..." }
    else s

}
