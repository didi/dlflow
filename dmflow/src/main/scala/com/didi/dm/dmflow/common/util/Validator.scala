package com.didi.dm.dmflow.common.util

import com.didi.dm.dmflow.pojo.DTO.BoolRet


object Validator {

  @throws(classOf[IllegalArgumentException])
  def assert(func: => BoolRet): Unit = {
    val result = func
    require(result.ret, result.msg)
  }

}
