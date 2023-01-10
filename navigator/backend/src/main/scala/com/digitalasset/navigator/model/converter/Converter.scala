// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.model.converter

case object Converter {

  /** Returns the sequence of values, or the last error encountered */
  def sequence[E, T](xs: Seq[Either[E, T]]): Either[E, List[T]] =
    xs.foldRight(Right(Nil): Either[E, List[T]]) { (e, acc) =>
      for (xs <- acc; x <- e) yield x :: xs
    }

  /** Returns the value of a required protobuf3 field, or RequiredFieldDoesNotExistError if it doesn't exist. */
  def checkExists[T](
      fieldName: String,
      maybeElement: Option[T],
  ): Either[ConversionError, T] =
    maybeElement match {
      case Some(element) => Right(element)
      case None => Left(RequiredFieldDoesNotExistError(fieldName))
    }

  def checkExists[T](
      maybeElement: Option[T],
      error: ConversionError,
  ): Either[ConversionError, T] =
    maybeElement match {
      case Some(element) => Right(element)
      case None => Left(error)
    }
}
