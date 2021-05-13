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
package by.iba.vf.spark.transformation

import by.iba.vf.spark.transformation.config.ProcessConfiguration
import by.iba.vf.spark.transformation.plan.ExecutionPlanBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

private[transformation] object TransformationJob extends Logger {

  private def run(implicit spark: SparkSession): Unit = {
    val ep = ExecutionPlanBuilder.buildExecutionPlan(ProcessConfiguration.read)
    val ed = new ExecutionData(ep.dependencyMap)
    ep.printExecutionPlan()
    ep.plan.foreach { iteration =>
      val results: Map[String, DataFrame] =
        iteration.flatMap { stage =>
          logger.info("Current layer: {}", ed.getCurrentLayer)
          logger.info("Processing stage: {}", stage.id)
          logger.info("Stage input: {}", ep.dependencyMap.getOrElse(stage.id, Nil))
          stage.execute(ed.getStageInputData(stage.id)).map { r =>
            logger.info("Resulting DataFrame schema:\n{}", r.schema.treeString)
            stage.id -> r
          }
        }.toMap

      ed.saveStageResults(results)
    }
  }

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()

    try {
      run
    } finally {
      spark.stop()
    }
  }
}
