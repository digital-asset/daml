// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package xbc

import org.objectweb.asm._
import org.objectweb.asm.Opcodes._

object Compiler { // compiler from Lang to ByteCode

  import Lang._

  type Value = Long

  val className = "ClassForCompiledLangExpression"

  def compile(program: Program): ByteCode = {

    val cw: ClassWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES)
    cw.visit(V1_8, ACC_PUBLIC, className, null, "java/lang/Object", null)

    def compileMethod(mv: MethodVisitor, body: Exp): Unit = {

      def compileBinOp(binOp: BinOp): Unit = {
        binOp match {
          case MulOp => mv.visitInsn(LMUL)
          case AddOp => mv.visitInsn(LADD)
          case SubOp => mv.visitInsn(LSUB)
          case CmpOp => mv.visitInsn(LCMP)
        }
      }

      def compileExp(exp: Exp): Unit = {
        exp match {

          case Num(long) =>
            mv.visitLdcInsn(long)

          case Builtin(binOp, e1, e2) =>
            compileExp(e1)
            compileExp(e2)
            compileBinOp(binOp)

          case IfNeg(e1, e2, e3) =>
            val labElse = new Label()
            val labJoin = new Label()
            compileExp(e1)
            mv.visitJumpInsn(IFGE, labElse)
            compileExp(e2)
            mv.visitJumpInsn(GOTO, labJoin)
            mv.visitLabel(labElse)
            compileExp(e3)
            mv.visitLabel(labJoin)

          case Arg(i) =>
            // compute assuming we are a static method, and all args are long
            val pos = 2 * i
            mv.visitIntInsn(LLOAD, pos)

          case FnCall(fnName, args) =>
            val subMethodName = "my_" + fnName
            args.foreach { arg =>
              compileExp(arg)
            }
            val arity = args.length
            val argTypes = List.fill(arity)('J').mkString
            mv.visitMethodInsn(
              INVOKESTATIC,
              className,
              subMethodName,
              s"($argTypes)J",
              false,
            )
        }
      }
      compileExp(body)
      mv.visitInsn(LRETURN)
      mv.visitMaxs(-1, -1)
    }

    def compileDef(fnName: FnName, arity: Int, body: Exp): Unit = {
      val subMethodName = "my_" + fnName
      val argTypes = List.fill(arity)('J').mkString
      val mv: MethodVisitor =
        cw.visitMethod(
          ACC_PUBLIC | ACC_STATIC,
          subMethodName,
          s"($argTypes)J",
          null,
          null,
        )
      compileMethod(mv, body)
    }

    val entryMethodName = "startHere"

    def compileMain(main: Exp): Unit = {
      val mv: MethodVisitor =
        cw.visitMethod(ACC_PUBLIC | ACC_STATIC, entryMethodName, "()J", null, null)
      compileMethod(mv, main)
    }

    program match {
      case Program(defs, main) =>
        defs.foreach { case ((name, arity), body) =>
          compileDef(name, arity, body)
        }
        compileMain(main)
    }

    val bytes = cw.toByteArray()
    new ByteCode(className, entryMethodName, bytes)

  }

}
