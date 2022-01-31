/*
 * Copyright (c) 2021 IBA Group, a.s. All rights reserved.
 *
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
package by.iba.vf.spark.transformation.stage.write

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.JdbcStageBuilder
import by.iba.vf.spark.transformation.stage.Stage
import by.iba.vf.spark.transformation.stage.StageBuilder
import by.iba.vf.spark.transformation.stage.WriteStageBuilder
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField

private[write] final class JdbcWriteStage(
    override val id: String,
    schemaTable: String,
    truststorePath: Option[String],
    saveMode: Option[String],
    jdbcConfig: Map[String, String]
) extends WriteStage(id, JdbcWriteStageBuilder.jdbcStorage) {

  override val builder: StageBuilder = JdbcWriteStageBuilder

  override def write(df: DataFrame)(implicit spark: SparkSession): Unit = {
    implicit val sc: SparkContext = spark.sparkContext

    truststorePath.foreach(sc.addFile)

    val alteredTypes = df.schema.fields.foldLeft(None: Option[String]) { case (prev, field) =>
      field match {
        case StructField(name: String, _: StringType, _, _) =>
          val p = prev.map(_ + ",").getOrElse("")
          Some(s"$p${name} VARCHAR(255)")
        case _ => prev
      }
    }

    val config = jdbcConfig +
      (JDBCOptions.JDBC_TABLE_NAME -> schemaTable) ++
      alteredTypes.map(fields => Map("createTableColumnTypes" -> fields)).getOrElse(Map.empty)

    val dfWriter = getDfWriter(df, saveMode)
    dfWriter.format(JdbcWriteStageBuilder.jdbcStorage).options(config).save()
  }
}

object JdbcWriteStageBuilder extends JdbcStageBuilder with WriteStageBuilder {
  override def validateStorage(config: Map[String, String]): Boolean =
    config.get(fieldStorageId).exists(s => drivers.contains(s.toLowerCase))

  override protected def validateWrite(config: Map[String, String]): Boolean =
    validateJdbc(config)

  override protected def convert(config: Node): Stage = {
    val (schemaTable, map) = jdbcParams(config)
    val truststorePathOption = if (config.value.contains(fieldCertData)) Some(truststorePath) else None
    val saveMode = config.value.get(fieldWriteMode)
    new JdbcWriteStage(config.id, schemaTable, truststorePathOption, saveMode, map)
  }
}
