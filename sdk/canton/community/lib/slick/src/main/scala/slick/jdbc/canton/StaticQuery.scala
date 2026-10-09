package slick.jdbc.canton

import slick.dbio.{Effect, NoStream}
import slick.jdbc.{
  GetResult,
  Invoker,
  PositionedParameters,
  PositionedResult,
  SetParameter,
  StatementInvoker,
  StreamingInvokerAction,
}
import slick.sql.SqlStreamingAction

import java.sql.PreparedStatement
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

/** Fork of slick's sql interpolation.
  *
  * `sqlu` now returns an action indicating a write effect.
  * `sql` now returns a forked SQL action builder where `as` results in an action with read effect and
  * `asUpdate` in one with a write effect.
  */
class ActionBasedSQLInterpolation(val s: StringContext) extends AnyVal {

  /** Build a SQLActionBuilder via string interpolation */
  def sql(params: TypedParameter[?]*): SQLActionBuilder = SQLActionBuilder.parse(s.parts, params)

  /** Build an Action for an UPDATE statement via string interpolation */
  def sqlu(
            params: TypedParameter[?]*
          ): SqlStreamingAction[Vector[Int], Int, Effect.Write]#ResultAction[Int, NoStream, Effect.Write] =
    sql(params *).asUpdate
}

object ActionBasedSQLInterpolation {

  object Implicits {
    implicit def actionBasedSQLInterpolationCanton(s: StringContext): ActionBasedSQLInterpolation =
      new ActionBasedSQLInterpolation(s)
  }
}

class TypedParameter[T](val param: T, val setParameter: SetParameter[T]) {
  def applied: SetParameter[Unit] = setParameter.applied(param)
}

object TypedParameter {
  implicit def typedParameter[T](param: T)(implicit
      setParameter: SetParameter[T]
  ): TypedParameter[T] =
    new TypedParameter[T](param, setParameter)
}

object SQLActionBuilder {
  def parse(strings: Seq[String], typedParams: Seq[TypedParameter[?]]): SQLActionBuilder =
    if (strings.sizeIs == 1)
      SQLActionBuilder(strings.head, SetParameter.SetUnit)
    else {
      val b = new mutable.StringBuilder
      val remaining = new ArrayBuffer[SetParameter[Unit]]
      typedParams.zip(strings.iterator.to(Iterable)).foreach { zipped =>
        val p = zipped._1.param
        var literal = false

        def decode(s: String): String =
          if (s.endsWith("##")) decode(s.substring(0, s.length - 2)) + "#"
          else if (s.endsWith("#")) {
            literal = true
            s.substring(0, s.length - 1)
          } else
            s

        b.append(decode(zipped._2))
        if (literal) b.append(p.toString)
        else {
          b.append('?')
          remaining += zipped._1.applied
        }
      }
      b.append(strings.last)
      SQLActionBuilder(b.toString, (u, pp) => remaining.foreach(_(u, pp)))
    }
}

case class SQLActionBuilder(sql: String, setParameter: SetParameter[Unit]) {

  private def asInternal[R, E <: Effect](implicit
      getResult: GetResult[R]
  ): SqlStreamingAction[Vector[R], R, E] =
    new StreamingInvokerAction[Vector[R], R, Effect] {
      def statements: Iterable[String] = List(sql)

      protected[this] def createInvoker(statements: Iterable[String]): Invoker[R] =
        new StatementInvoker[R] {
          val getStatement = statements.head

          protected def setParam(st: PreparedStatement): Unit =
            setParameter((), new PositionedParameters(st))

          protected def extractValue(rs: PositionedResult): R = getResult(rs)
        }

      protected[this] def createBuilder: collection.mutable.Builder[R, Vector[R]] =
        Vector.newBuilder[R]
    }

  def as[R](implicit getResult: GetResult[R]): SqlStreamingAction[Vector[R], R, Effect.Read] =
    asInternal[R, Effect.Read]

  def asUpdate: SqlStreamingAction[Vector[Int], Int, Effect.Write]#ResultAction[
    Int,
    NoStream,
    Effect.Write,
  ] =
    asInternal[Int, Effect.Write](GetResult.GetUpdateValue).head

  def concat(b: SQLActionBuilder): SQLActionBuilder =
    SQLActionBuilder(
      sql + b.sql,
      (p, pp) => {
        setParameter(p, pp)
        b.setParameter(p, pp)
      },
    )
}
