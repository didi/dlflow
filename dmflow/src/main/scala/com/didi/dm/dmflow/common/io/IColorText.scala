package com.didi.dm.dmflow.common.io

trait IColorText {

  val ANSI_RESET = "\u001B[0m"
  val ANSI_BLACK = "\u001B[30m"
  val ANSI_RED = "\u001B[31m"
  val ANSI_GREEN = "\u001B[32m"
  val ANSI_YELLOW = "\u001B[33m"
  val ANSI_BLUE = "\u001B[34m"
  val ANSI_PURPLE = "\u001B[35m"
  val ANSI_CYAN = "\u001B[36m"
  val ANSI_WHITE = "\u001B[37m"

  def Black(text: String): String = {
    ANSI_BLACK + text + ANSI_RESET
  }

  def Red(text: String): String = {
    ANSI_RED + text + ANSI_RESET
  }

  def Green(text: String): String = {
    ANSI_GREEN + text + ANSI_RESET
  }

  def Yellow(text: String): String = {
    ANSI_YELLOW + text + ANSI_RESET
  }

  def Blue(text: String): String = {
    ANSI_BLUE + text + ANSI_RESET
  }

  def Purple(text: String): String = {
    ANSI_PURPLE + text + ANSI_RESET
  }

  def Cyan(text: String): String = {
    ANSI_CYAN + text + ANSI_RESET
  }

  def White(text: String): String = {
    ANSI_WHITE + text + ANSI_RESET
  }

}
