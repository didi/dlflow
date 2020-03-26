package com.didi.dm.dmflow.param

import java.nio.file.{Files, Paths}

import com.didi.dm.dmflow.pojo.DTO.BoolRet

trait RequiredArguments {

  @throws(classOf[IllegalArgumentException])
  protected def checkRequiredArguments(): BoolRet

}

object DMParamValidators {

  def alwaysTrue[T]: T => Boolean = { _: T => true }

  def nonEmpty4opt: Option[String] => Boolean = { value: Option[String] => {
    value match {
      case None => true
      case Some(value: String) => value != null && value.trim.length() > 0
    }
  }
  }

  def nonEmpty: String => Boolean = { value: String => value != null && value.trim.length() > 0 }

  def localFileExists: String => Boolean = { pt: String => Files.exists(Paths.get(pt)) }

}