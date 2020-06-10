package com.daml.platform.store.dao.events

import com.daml.platform.store.SimpleSqlAsVectorOf._
import anorm.{ResultSetParser, Row, RowParser, SimpleSql}
import scalaz.{-\/, Free, Functor, \/-}

import java.sql.Connection

object SqlSequence {

  /** A sequence of `SimpleSql`s, terminating in A. */
  type T[A] = Free[Elt, A]

  final case class Elt[+A](run: Connection => A)

  object Elt {
    implicit final class syntax[A](private val self: T[A]) extends AnyVal {
      def executeSql(implicit conn: Connection): A = {
        @annotation.tailrec
        def go(self: T[A]): A =
          self.resume match {
            case -\/(elt) => go(elt.run(conn))
            case \/-(a) => a
          }
        go(self)
      }
    }

    implicit val covariant: Functor[Elt] = new Functor[Elt] {
      override def map[A, B](fa: Elt[A])(f: A => B) = fa copy (run = fa.run andThen f)
    }
  }

  def apply[A](s: SimpleSql[_], p: ResultSetParser[A]): T[A] =
    Free liftF Elt(implicit conn => s as p)

  def vector[A](s: SimpleSql[Row], p: RowParser[A]): T[Vector[A]] =
    Free liftF Elt(implicit conn => s asVectorOf p)

  def point[A](a: A): T[A] = Free point a
}
