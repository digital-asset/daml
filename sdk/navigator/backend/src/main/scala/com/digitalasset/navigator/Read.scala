// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/** A reading failure */
final case class ReadFailed(from: String)

/** A simple `Read` typeclass */
sealed trait Read[To] {

  /** Convert `from` to a value of type `To`
    *
    * @return `Right` wrapping an instance of `To` in case of success in reading, a `Left` wrapping a `ReadFailed`
    *        otherwise
    */
  def from(from: String): Either[ReadFailed, To]
}

object Read {
  def apply[To](implicit readTo: Read[To]): Read[To] = readTo

  /** Utility function equivalent to calling `new Read` but more convenient */
  def fromFunction[To](f: String => Either[ReadFailed, To]): Read[To] =
    new Read[To] {
      override def from(from: String): Either[ReadFailed, To] = f(from)
    }

  /** A failure while reading that wraps the failure inside a `Left` */
  def fail[To](implicit classTag: ClassTag[To]): Either[ReadFailed, To] =
    Left(ReadFailed(classTag.runtimeClass.getSimpleName))

  /** Catches the exceptions thrown while read a value of type `To` from a `String`
    */
  def fromUnsafeFunction[To](f: String => To)(implicit classTag: ClassTag[To]): Read[To] =
    fromFunction[To] { str =>
      Try(f(str)) match {
        case Success(str) => Right(str)
        case Failure(_) => Read.fail[To](classTag)
      }
    }

  implicit val readString: Read[String] = Read.fromFunction[String](str => Right(str))
  implicit val readBoolean: Read[Boolean] = Read.fromUnsafeFunction[Boolean](_.toBoolean)
  implicit val readInt: Read[Int] = Read.fromUnsafeFunction[Int](_.toInt)
  implicit val readFloat: Read[Float] = Read.fromUnsafeFunction[Float](_.toFloat)
  implicit val readDouble: Read[Double] = Read.fromUnsafeFunction[Double](_.toDouble)
}
