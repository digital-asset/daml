package lf.verified
package utils

import stainless.annotation._
import stainless.lang._
import scala.annotation.{Annotation}

object Unreachable {


  @opaque
  def apply(): Nothing = {
    require(false)
    ???
  }
}

object Trivial {
  def apply(): Unit = ()
}

@ignore
class nopaque extends Annotation

@ignore
class alias extends Annotation

object Either{

  @pure
  def cond[A, B](test: Boolean, right: B, left: A): Either[A, B] = {
    if (test) Right[A, B](right) else Left[A, B](left)
  }.ensuring(
    (res: Either[A, B]) => res.isInstanceOf[Right[A, B]] == test
  )
}

extension[T](o: Option[T]) {
  def filterNot(p: T => Boolean): Option[T] =
    o match {
      case Some(v) if !p(v) => o
      case _ => None[T]()
    }
}
