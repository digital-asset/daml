// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package xbc

import java.io.FileOutputStream

final class ByteCode(className: String, methodName: String, bytes: Array[Byte]) {

  def dump() = {
    val base = "/tmp/"
    val path = base + className + ".class"
    val f = new FileOutputStream(path)
    //println(s"generating $path, #${bytes.length} bytes")
    f.write(bytes)
  }

  // load and invoke some (perhaps dynamically generated) bytecode
  case object MyClassLoader extends ClassLoader {
    override def findClass(name: String): Class[_] = {
      defineClass(name, bytes, 0, bytes.length)
    }
  }

  def run0(): Long = { // run with 0 args; return long
    val aClass = MyClassLoader.loadClass(className)
    val method = aClass.getMethod(methodName)
    method.invoke(null).asInstanceOf[Long]
  }

  def run2(arg1: Long, arg2: Long): Long = { // run with 2 long args; return long
    val aClass = MyClassLoader.loadClass(className)
    val longType: Class[_] = classOf[Long]
    val method = aClass.getMethod(methodName, longType, longType)
    val res: Object = method.invoke(null, arg1, arg2)
    res.asInstanceOf[Long]
  }

}
