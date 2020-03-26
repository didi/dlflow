package com.didi.dm.dmflow.base

import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.writePretty
import org.json4s.{Formats, NoTypeHints}

trait IBean extends Serializable {


  override def toString: String = {

    val typeName = this.getClass.getTypeName

    val map = this.getClass.getDeclaredFields.map { v =>
      v.setAccessible(true)
      (v.getName, if (v.get(this) != null) v.get(this).toString else "")
    }.toMap

    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

    writePretty(Map("type" -> typeName, "params" -> map))
  }
}
