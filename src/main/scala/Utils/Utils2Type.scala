package Utils

object Utils2Type {
  def toDouble(str: String): Double = {
    try {
      str.toDouble
    } catch {
      case _:Exception => 0.0
    }
  }

  def toInt(str: String): Int = {
    try (
      str.toInt
      ) catch {
      case _:Exception => 0
    }
  }

}

