package com.teragrep.functions.dpf_02.aggregate

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row}

import scala.reflect.classTag

class RowArrayAggregator(schema: StructType) extends Aggregator[Row, RowBuffer, Row] with Serializable {

  private val arrSchema: ArrayType = DataTypes.createArrayType(schema)

  private val schemaArrs: StructType = StructType(Seq(StructField("arrayOfInput", arrSchema, nullable = false)))

  override def zero: RowBuffer = new RowBuffer: RowBuffer

  override def reduce(buf: RowBuffer, input: Row): RowBuffer = {
    buf.reduce(input)
  }

  override def merge(b1: RowBuffer, b2: RowBuffer): RowBuffer = {
    b1.merge(b2)
  }

  override def finish(reduction: RowBuffer): Row = {
    reduction.finish()
  }

  override def bufferEncoder: Encoder[RowBuffer] = Encoders.kryo(classTag[RowBuffer])
  override def outputEncoder: Encoder[Row] = RowEncoder.apply(schemaArrs).resolveAndBind()
}
