package com.recsystem.common

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Created by lee on 2016/4/20.
 */
class HashingTFWithTerm extends HashingTF{
   def transforms(document: Iterable[_]): (Vector,mutable.HashMap[Int, String]) = {
    val termArray = mutable.HashMap.empty[Int, String]
    val termFrequencies = mutable.HashMap.empty[Int, Double]
    document.foreach { term =>
      val i = indexOf(term)
      termFrequencies.put(i, termFrequencies.getOrElse(i, 0.0) + 1.0)
      termArray.put(i, term.toString)
    }
    (Vectors.sparse(numFeatures, termFrequencies.toSeq),termArray)
  }

   def transforms[D <: Iterable[_]](dataset: RDD[D]): RDD[(Vector,mutable.HashMap[Int, String])] = {
    dataset.map(this.transforms)
  }
}
