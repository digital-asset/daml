// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.model.converter

case object Converter {

  /** Returns the sequence of values, or the first error encountered */
  def sequence[T](xs: Seq[Either[ConversionError, T]]): Either[ConversionError, List[T]] =
    xs.foldRight(Right(Nil): Either[ConversionError, List[T]]) { (e, acc) =>
      for (xs <- acc.right; x <- e.right) yield x :: xs
    }

  def sequenceMap[K, T](
      xs: Map[K, Either[ConversionError, T]]): Either[ConversionError, Map[K, T]] =
    xs.foldRight(Right(Map.empty): Either[ConversionError, Map[K, T]]) { (e, acc) =>
      for (xs <- acc.right; x <- e._2.right) yield xs + (e._1 -> x)
    }

  /** Returns the value of a required protobuf3 field, or RequiredFieldDoesNotExistError if it doesn't exist. */
  def checkExists[T](
      fieldName: String,
      maybeElement: Option[T]
  ): Either[ConversionError, T] =
    maybeElement match {
      case Some(element) => Right(element)
      case None => Left(RequiredFieldDoesNotExistError(fieldName))
    }

  def checkExists[T](
      maybeElement: Option[T],
      error: ConversionError
  ): Either[ConversionError, T] =
    maybeElement match {
      case Some(element) => Right(element)
      case None => Left(error)
    }
}
