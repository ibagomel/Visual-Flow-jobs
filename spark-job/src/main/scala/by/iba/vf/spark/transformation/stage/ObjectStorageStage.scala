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
package by.iba.vf.spark.transformation.stage

import by.iba.vf.spark.transformation.config.Node
import org.apache.spark.sql.SparkSession

protected trait ObjectStorageConfig {
  final val cosStorage = "cos"
  final protected val objectStorage = "ObjectStorage"
  final protected val s3Storage = "s3"
  final protected val service = "service"

  final protected val fieldAccessKey = "accessKey"
  final protected val fieldBucket = "bucket"
  final protected val fieldEndpoint = "endpoint"
  final protected val fieldFormat = "format"
  final protected val fieldPath = "path"
  final protected val fieldWriteMode = "writeMode"
  final protected val fieldSecretKey = "secretKey"
  final protected val fieldStorage = "storage"
  def validate(config: Map[String, String]): Boolean = {
    config.contains(fieldAccessKey) && config.contains(fieldSecretKey) && config.contains(fieldBucket) && config
      .contains(fieldPath) && config.contains(fieldFormat)
  }
}

protected abstract class BaseStorageConfig(config: Node) extends ObjectStorageConfig{
  final val format: String = config.value(fieldFormat)
  final val saveMode: Option[String] = config.value.get(fieldWriteMode)
  final protected val id: String = config.id
  final protected val accessKey: String = config.value(fieldAccessKey)
  final protected val secretKey: String = config.value(fieldSecretKey)
  final protected val bucket: String = config.value(fieldBucket)
  final protected val path: String = config.value(fieldPath)

  def setConfig(spark: SparkSession): Unit
  def connectPath: String
}

class COSConfig(config: Node) extends BaseStorageConfig (config){
  private val endpoint = config.value(fieldEndpoint)
  override def setConfig(spark: SparkSession): Unit = {
    spark.conf.set("fs.stocator.scheme.list", cosStorage)
    spark.conf.set(s"fs.$cosStorage.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
    spark.conf.set(s"fs.stocator.$cosStorage.impl", s"com.ibm.stocator.fs.$cosStorage.COSAPIClient")
    spark.conf.set(s"fs.stocator.$cosStorage.scheme", cosStorage)
    spark.conf.set(s"fs.$cosStorage.$service.endpoint", endpoint)
    spark.conf.set(s"fs.$cosStorage.$service.access.key", accessKey)
    spark.conf.set(s"fs.$cosStorage.$service.secret.key", secretKey)
  }

  override def connectPath: String = s"$cosStorage://$bucket.$service/$path"
}
object COSConfig extends ObjectStorageConfig {
  override def validate(config: Map[String, String]): Boolean =
    super.validate(config) &&
      config.contains(fieldEndpoint) &&
      config.get(fieldStorage).contains(cosStorage)
}

class S3Config(config: Node)  extends BaseStorageConfig (config){
  override def setConfig(spark: SparkSession): Unit = {
    spark.conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    spark.conf.set("fs.s3a.access.key", accessKey)
    spark.conf.set("fs.s3a.secret.key", secretKey)
  }

  override def connectPath: String = s"s3a://$bucket/$path"
}
object S3Config extends ObjectStorageConfig {
  override def validate(config: Map[String, String]): Boolean =
    super.validate(config) &&
      config.get(fieldStorage).contains(s3Storage)
}
