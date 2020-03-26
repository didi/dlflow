package com.didi.dm.dmflow.base

import scala.reflect.ClassTag

trait Reflective extends Serializable {
  def clazz: Class[_ <: Reflective] = this.getClass

  def clazzName: String = getClassName(clazz)

  def simpleClazzName: String = getSimpleClassName(clazz)

  def create[T](className: String): T = Class.forName(className).newInstance().asInstanceOf[T]

  def getClassName(inClazz: Class[_]): String = inClazz.getName

  def getSimpleClassName(inClazz: Class[_]): String = inClazz.getSimpleName

  def getClassOf[T: ClassTag]: Class[_] = implicitly[ClassTag[T]].runtimeClass

}
