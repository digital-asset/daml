// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.model.converter

case object Converter {

  /** Returns the sequence of values, or the last error encountered */
  def sequence[E, T](xs: Seq[Either[E, T]]): Either[E, List[T]] =
    xs.foldRight(Right(Nil): Either[E, List[T]]) { (e, acc) =>
      for (xs <- acc.right; x <- e.right) yield x :: xs
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
