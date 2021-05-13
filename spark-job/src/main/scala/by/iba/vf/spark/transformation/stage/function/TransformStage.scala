/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package by.iba.vf.spark.transformation.stage.function

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.OperationType
import by.iba.vf.spark.transformation.stage.Stage
import by.iba.vf.spark.transformation.stage.StageBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

private[function] final class TransformStage(val id: String, selectStmt: String) extends Stage {
  override val operation: OperationType.Value = OperationType.TRANSFORM
  override val inputsRequired: Int = 1
  override val builder: StageBuilder = TransformStageBuilder

  override protected def process(input: Map[String, DataFrame])(implicit spark: SparkSession): Option[DataFrame] =
    input.values.headOption.map(transform(_, spark))

  def transform(df: DataFrame, spark: SparkSession): DataFrame = {
    val tmpTable = "tmpTable"
    val sql = s"select $selectStmt from $tmpTable"

    df.createOrReplaceTempView(tmpTable)
    spark.sql(sql)
  }
}

object TransformStageBuilder extends StageBuilder {
  private val FieldStatement = "statement"

  override protected def validate(config: Map[String, String]): Boolean =
    config.get(fieldOperation).contains(OperationType.TRANSFORM.toString) && config.contains(FieldStatement)

  override protected def convert(config: Node): Stage =
    new TransformStage(config.id, config.value(FieldStatement))
}
