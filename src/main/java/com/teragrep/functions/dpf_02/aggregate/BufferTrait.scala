package com.teragrep.functions.dpf_02.aggregate

import org.apache.spark.sql.Row

trait BufferTrait {
  def zero: BufferTrait

  def reduce(input: Row): BufferTrait

  def merge(another: BufferTrait): BufferTrait

  def finish(): Row

  def toList: java.util.List[Row]

  def count(): Int
}
