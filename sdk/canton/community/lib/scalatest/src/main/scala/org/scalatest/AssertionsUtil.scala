package org.scalatest

import org.scalactic.source

import scala.language.experimental.macros

trait AssertionsUtil extends Assertions {

  /** Generalizes [[Assertions.assertTypeError]] in that the type error message can be inspected.
    *
    * [[Assertions.assertTypeError]](code) is equivalent to [[assertOnTypeError]](code)(_ =>
    * succeed).
    */
  def assertOnTypeError(code: String)(assertion: String => Assertion)(implicit
      pos: source.Position
  ): Assertion = macro AssertionsUtilMacros.assertOnTypeErrorImpl
}

object AssertionsUtil extends AssertionsUtil
