package slick.jdbc.canton

import slick.dbio.{Effect, NoStream}
import slick.jdbc.{
  GetResult,
  PositionedParameters,
  PositionedResult,
  SetParameter,
  StatementInvoker,
  StreamingInvokerAction
}
import slick.sql.{SqlAction, SqlStreamingAction}

import java.sql.PreparedStatement
import scala.language.experimental.macros
import scala.language.implicitConversions
import scala.reflect.macros.blackbox

/** Fork of slick's sql interpolation.
  *
  * `sqlu` now returns an action indicating a write effect.
  * `sql` now returns a forked SQL action builder where `as` results in an action with read effect and
  * `asUpdate` in one with a write effect.
  */
class ActionBasedSQLInterpolation(val s: StringContext) extends AnyVal {
  import slick.jdbc.ActionBasedSQLInterpolation.sqluImpl
  import ActionBasedSQLInterpolation.sqlImpl

  def sql(param: Any*): SQLActionBuilder = macro sqlImpl

  def sqlu(param: Any*): SqlAction[Int, NoStream, Effect.Write] = macro sqluImpl
}

object ActionBasedSQLInterpolation {

  object Implicits {
    implicit def actionBasedSQLInterpolationCanton(s: StringContext): ActionBasedSQLInterpolation =
      new ActionBasedSQLInterpolation(s)
  }

  def sqlImpl(ctxt: blackbox.Context)(param: ctxt.Expr[Any]*): ctxt.Expr[SQLActionBuilder] = {
    import ctxt.universe._
    reify {
      val builder = slick.jdbc.ActionBasedSQLInterpolation.sqlImpl(ctxt)(param.toList: _*).splice
      SQLActionBuilder(builder.queryParts, builder.unitPConv)
    }
  }

}

final case class SQLActionBuilder(queryParts: Seq[Any], unitPConv: SetParameter[Unit]) {
  private def asInternal[R, E <: Effect](implicit rconv: GetResult[R]): SqlStreamingAction[Vector[R], R, E] = {
    val query =
      if (queryParts.length == 1 && queryParts(0).isInstanceOf[String]) queryParts(0).asInstanceOf[String]
      else queryParts.iterator.map(String.valueOf).mkString
    new StreamingInvokerAction[Vector[R], R, E] {
      def statements = List(query)
      protected[this] def createInvoker(statements: Iterable[String]) = new StatementInvoker[R] {
        val getStatement                                    = statements.head
        protected def setParam(st: PreparedStatement)       = unitPConv((), new PositionedParameters(st))
        protected def extractValue(rs: PositionedResult): R = rconv(rs)
      }
      protected[this] def createBuilder = Vector.newBuilder[R]
    }
  }

  def as[R](implicit rconv: GetResult[R]): SqlStreamingAction[Vector[R], R, Effect.Read] = asInternal[R, Effect.Read]
  def asUpdate: SqlStreamingAction[Vector[Int], Int, Effect.Write]#ResultAction[Int, NoStream, Effect.Write] =
    asInternal[Int, Effect.Write](GetResult.GetUpdateValue).head

  def stripMargin(marginChar: Char): SQLActionBuilder =
    copy(queryParts.map(_.asInstanceOf[String].stripMargin(marginChar)))
  def stripMargin: SQLActionBuilder = copy(queryParts.map(_.asInstanceOf[String].stripMargin))
}
