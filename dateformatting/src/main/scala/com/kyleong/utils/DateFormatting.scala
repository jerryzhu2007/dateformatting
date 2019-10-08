package com.kyleong.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf


class DateFormatting extends Serializable {

  //2017-06-16  2016.04.01  2017年4月5日 2019/07/22
  val p1 = """\d{4}[-|.|/|年]\d{1,2}[-|.|/|月]\d{1,2}""".r
  //16 June 2017  22-07-2019  12/08/2016  2 октября 2017  3.1.2019  05/feb/2018  243; datums 31 maijs 2019
  val p2 = """\d{1,2}[\s|-|/|.][\S]+[\s|-|/|.]\d{4}""".r
  //August 12, 2019
  val p3 = """[\S]+[\s]\d{1,2}[\s|,]+\d{4}""".r
  //6. března 2015  22 de Setembro de 2016
  val p4 = """\d{1,2}[\S|\s]+[\s|-]\d{4}""".r
  //Jul 2016
  val p5 = """[\S|\s]+[\s|-]\d{4}""".r
  //2016. okt. 18.  2017Nian 1Yue 15Ri
  val p6 = """\d{4}[\S|\s]+[\s|-]\d{1,2}""".r
  //20121103112058
  val p7 = """\d{8,14}""".r
  //todo:6  years ago  včera  59  minutes ago  1  day ago  2014å¹´09æ08æ¥
  val p8 = """\d{4}[\S|\s]+[\s|-]\d{1,2}""".r
  private def dateCleaning(content: String): List[String] = {
    content match {
      case c if (p1 findFirstIn c).nonEmpty => ("P1")::(p1 findFirstIn c).get.split("[-|.|/|年|月]").toList
      case c if (p2 findFirstIn c).nonEmpty => ("P2")::(p2 findFirstIn c).get.split("[ |-|/|.]").toList.reverse
      case c if (p3 findFirstIn c).nonEmpty => {
        val s = (p3 findFirstIn c).get.split("[ ]").toList
        ("P3")::s.last::s.dropRight(1)
      }
      case c if (p4 findFirstIn c).nonEmpty => ("P4")::(p4 findFirstIn c).get.split("[ |-]").toList.reverse
      case c if (p5 findFirstIn c).nonEmpty => ("P5")::("01"::(p5 findFirstIn c).get.split("[ |-]").toList).reverse
      case c if (p6 findFirstIn c).nonEmpty => ("P6")::(p6 findFirstIn c).get.split("[ |-]").toList
      case c if (p7 findFirstIn c).nonEmpty => {
        val s = (p7 findFirstIn c).get
        List("P7", s.substring(0,4), s.substring(4,6), s.substring(6,8))
      }
      case _ => ("NotMatch")::content.split(" ").toList
    }
  }

  private def stripSpecialCharacters(str: String): String = {
    var output = ""
    if (str != null) {
      output = str.replaceAll("[ .,;{}()\n\t=]", "")
      // get digital part of a string, example: 2017Nian 1Yue 15Ri ---> 2017 1 15
      output = ("""\d{1,4}\S{3,4}""".r findFirstIn output) match {
        case Some(x) => x.replaceAll("[^(0-9)]", "")
        case None => output
      }
    }
    output
  }

  private def stripSpecialList( input: List[String] ) = {
    var output = List[String]()
    input.map(i => if (i != "" && i != "de") output = i::output)
    output.reverse
  }

  def dateFormatting(str: String):String = {
    val getDate = dateCleaning(str)
    val trimSpecial = getDate.map(i => stripSpecialCharacters(i))
    stripSpecialList(trimSpecial).mkString("-")
  }

  val dateFormattingUDF = udf(dateFormatting _)
}
