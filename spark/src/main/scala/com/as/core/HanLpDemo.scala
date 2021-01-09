package com.as.core

import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable

object HanLpDemo {


  def main(args: Array[String]): Unit = {
    val words = "[南京市长江大桥垮了]"
    val terms: util.List[Term] = HanLP.segment(words)

    import scala.collection.JavaConverters._
    val result: mutable.Buffer[String] = terms.asScala.map(_.word.replaceAll("\\[|\\]","")).filter(StringUtils.isNotBlank(_))
    result.foreach(println)
  }


}
