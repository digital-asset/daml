// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package xbc

import org.objectweb.asm._
import org.objectweb.asm.Opcodes._

object Bucket {
  def myLong2String(x: Long): String = {
    s"myLong2String[$x]"
  }
  def mySubtract(x: Long, y: Long): Long = {
    x - y
  }
}

object Play { // Play with bytecode generation using Asm

  def makeCodeToPrintMessage(message: String): ByteCode = {

    val className = "PlayGen"
    val methodName = "doit"

    val cw: ClassWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES)
    cw.visit(V1_8, ACC_PUBLIC, className, null, "java/lang/Object", null)

    {
      val sub: MethodVisitor =
        cw.visitMethod(ACC_PUBLIC | ACC_STATIC, "mySquare", "(J)J", null, null)

      sub.visitIntInsn(LLOAD, 0)
      sub.visitIntInsn(LLOAD, 0)
      sub.visitInsn(LMUL)
      sub.visitInsn(LRETURN)
      sub.visitMaxs(-1, -1)
    }

    val mv: MethodVisitor =
      cw.visitMethod(ACC_PUBLIC | ACC_STATIC, methodName, "(JJ)J", null, null)

    // print(message)
    mv.visitFieldInsn(GETSTATIC, "java/lang/System", "out", "Ljava/io/PrintStream;")
    mv.visitLdcInsn(message)
    mv.visitMethodInsn(
      INVOKEVIRTUAL,
      "java/io/PrintStream",
      "println",
      "(Ljava/lang/String;)V",
      false,
    )

    /*
       print(myLong2String(mySquare(100) + mySubtract( ...?:... ,  1)))
     */
    mv.visitFieldInsn(GETSTATIC, "java/lang/System", "out", "Ljava/io/PrintStream;")
    mv.visitFieldInsn(GETSTATIC, "xbc/Bucket$", "MODULE$", "Lxbc/Bucket$;")

    mv.visitLdcInsn(100L)
    mv.visitMethodInsn(INVOKESTATIC, className, "mySquare", "(J)J", false)

    mv.visitFieldInsn(GETSTATIC, "xbc/Bucket$", "MODULE$", "Lxbc/Bucket$;")

    /*
         (firstLongArg < secondLongArg)
         ? (secondLongArg - firstLongArg)
         : (firstLongArg - secondLongArg)
     */
    val labElse = new Label()
    val labJoin = new Label()
    mv.visitIntInsn(LLOAD, 0)
    mv.visitIntInsn(LLOAD, 2)
    mv.visitInsn(LCMP)
    mv.visitJumpInsn(IFGE, labElse)
    mv.visitIntInsn(LLOAD, 2)
    mv.visitIntInsn(LLOAD, 0)
    mv.visitInsn(LSUB)
    mv.visitJumpInsn(GOTO, labJoin)
    mv.visitLabel(labElse)
    mv.visitIntInsn(LLOAD, 0)
    mv.visitIntInsn(LLOAD, 2)
    mv.visitInsn(LSUB)
    mv.visitLabel(labJoin)

    mv.visitLdcInsn(1L)
    mv.visitMethodInsn(INVOKEVIRTUAL, "xbc/Bucket$", "mySubtract", "(JJ)J", false)
    mv.visitInsn(LADD)
    mv.visitMethodInsn(
      INVOKEVIRTUAL,
      "xbc/Bucket$",
      "myLong2String",
      "(J)Ljava/lang/String;",
      false,
    )
    mv.visitMethodInsn(
      INVOKEVIRTUAL,
      "java/io/PrintStream",
      "println",
      "(Ljava/lang/String;)V",
      false,
    )

    // return(42)
    mv.visitLdcInsn(42L)
    mv.visitInsn(LRETURN)

    mv.visitMaxs(-1, -1)

    val bytes = cw.toByteArray()
    new ByteCode(className, methodName, bytes)
  }

}
