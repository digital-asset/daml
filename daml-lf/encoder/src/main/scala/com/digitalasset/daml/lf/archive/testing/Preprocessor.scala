package com.daml.lf.archive.testing

import java.nio.file.Path

import com.daml.lf.language.LanguageVersion

import scala.Ordering.Implicits.infixOrderingOps

/*

  A very simple cpp-like preprocessor that allow conditional
  The preprocessor assumes that all lines that star with `#` is a preprocess directives.
  The preprocessor accepts only four directives:
  - `#if TARGET <sign> 1.<minor>`
  - `#elif TARGET <sign> 1.<minor>`
  - `#else`
  - `#endif`
  where
   - <sign> stands for a one of the comparison operators (`<`, `<=`, `>`, `>=`)
   - <minor> stands for a non empty sequence of digit

 */
private[testing] object Preprocessor {

  val versions = LanguageVersion.Major.V1.acceptedVersions
    .map(ver => ver.toProtoIdentifier -> LanguageVersion(LanguageVersion.Major.V1, ver))
    .toMap

  def preprocess(file: Path, lines: Iterator[String], target: LanguageVersion): String = {

    def cond(idx: Int, sign: String, minor: String): Boolean = {
      val ver =
        versions.getOrElse(minor, throw Error(file, idx + 1, s"LF version 1.$minor not supported"))
      sign match {
        case "<" => target < ver
        case "<=" => target <= ver
        case ">" => target > ver
        case ">=" => target >= ver
      }
    }

    var state: State = RootState
    val builder = new StringBuilder

    lines.zipWithIndex.foreach { case (line, idx) =>
      if (line.startsWith("#")) {
        builder ++= "// "
        line match {

          case If(sign, minor) =>
            val newInclusion = state match {
              case RootState | BranchingState(_, Inclusion.Yes, _) =>
                Inclusion(cond(idx, sign, minor))
              case _ =>
                Inclusion.Never
            }
            state = BranchingState(ifPart = true, newInclusion, state)

          case Elif(sign, minor) =>
            state match {
              case BranchingState(true, inclusion, stack) =>
                val newInclusion = inclusion match {
                  case Inclusion.No => Inclusion(cond(idx, sign, minor))
                  case _ => Inclusion.Never
                }
                state = BranchingState(ifPart = true, newInclusion, stack)
              case _ =>
                throw Error(file, idx + 1, "#elif does not match a #if/elif")
            }

          case Else() =>
            state match {
              case BranchingState(true, inclusion, stack) =>
                val newInclusion = inclusion match {
                  case Inclusion.No => Inclusion.Yes
                  case _ => Inclusion.Never
                }
                state = BranchingState(ifPart = false, newInclusion, stack)
              case _ =>
                throw Error(file, idx + 1, "#else does not match a #if/elif")
            }

          case EndIf() =>
            state match {
              case BranchingState(_, _, stack) =>
                state = stack
              case _ =>
                throw Error(file, idx + 1, "#endif does not match a #if/else/elif")
            }

          case s =>
            throw Error(file, idx + 1, s"macro parse error: $s")
        }
      } else {
        state match {
          case BranchingState(_, Inclusion.No | Inclusion.Never, _) =>
            builder ++= "// "
          case _ =>
        }
      }

      builder ++= line
      builder += '\n'
    }

    if (state != RootState)
      throw Error(file, -1, "#if/else/elif not close")

    builder.result()
  }

  private[this] sealed abstract class Inclusion extends Product with Serializable

  private[this] object Inclusion {

    case object No extends Inclusion

    case object Yes extends Inclusion

    case object Never extends Inclusion

    def apply(boolean: Boolean): Inclusion =
      if (boolean) Yes else No
  }

  private[this] val If = """#if\s+TARGET\s+([<>]=?)\s+1\.(\d+)\s*""".r
  private[this] val Elif = """#elif\s+TARGET\s+([<>]=?)\s+1\.(\d+)\s*""".r
  private[this] val Else = """#else\s*""".r
  private[this] val EndIf = """#endif\s*""".r

  case class Error(file: Path, line: Int, error: String) extends RuntimeException {
    override def getMessage: String =
      s"Preprocessor Error at $file:$line: $error"
  }

  private sealed abstract class State
  private case object RootState extends State
  private final case class BranchingState(ifPart: Boolean, inclusion: Inclusion, parentState: State)
      extends State

}
