package org.scalatest

import org.scalactic.source
import org.scalatest.CompileMacro.{containsAnyValNullStatement, getCodeStringFromCodeExpression}
import org.scalatest.exceptions.{StackDepthException, TestFailedException}

import scala.annotation.nowarn
import scala.reflect.macros.{Context, ParseException, TypecheckException}

object AssertionsUtilMacros {
  // Generalized version of org.scalatest.CompileMacro.assertTypeErrorImpl. Modifications are marked with MODIFIED
  @nowarn("msg=dead code following this construct")
  @nowarn("cat=deprecation")
  @nowarn("msg=unused value of type")
  def assertOnTypeErrorImpl(c: Context)(code: c.Expr[String])(
      // MODIFIED: This parameter is new
      assertion: c.Expr[String => Assertion]
  )(
      pos: c.Expr[source.Position]
  ): c.Expr[Assertion] = {
    import c.universe.*

    // extract code snippet
    val codeStr = getCodeStringFromCodeExpression(c)(
      // MODIFIED: Using the appropriate name here
      "assertOnTypeError",
      code,
    )

    try {
      val tree = c.parse("{ " + codeStr + " }")
      if (!containsAnyValNullStatement(c)(List(tree))) {
        c.typecheck(tree) // parse and type check code snippet
        // If reach here, type check passes, let's generate code to throw TestFailedException
        val messageExpr = c.literal(Resources.expectedTypeErrorButGotNone(codeStr))
        reify {
          throw new TestFailedException(
            (_: StackDepthException) => Some(messageExpr.splice),
            None,
            pos.splice,
          )
        }
      } else {

        reify {
          // statement such as val i: Int = null, compile fails as expected, generate code to return Succeeded
          Succeeded
        }
      }
    } catch {
      case e: TypecheckException =>
        // MODIFIED: Evaluate the given assertion on the exception's message instead of always succeeding
        val errorMessage = c.literal(e.msg)
        reify {
          assertion.splice(errorMessage.splice)
        }
      case e: ParseException =>
        // parse error, generate code to throw TestFailedException
        val messageExpr =
          c.literal(Resources.expectedTypeErrorButGotParseError(e.getMessage, codeStr))
        reify {
          throw new TestFailedException(
            (_: StackDepthException) => Some(messageExpr.splice),
            None,
            pos.splice,
          )
        }
    }
  }

}
