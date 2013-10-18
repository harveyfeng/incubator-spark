/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.dstream

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, DStream, Time}

private[streaming]
class NaryTransformedDStream[T: ClassManifest, U: ClassManifest] (
    parents: Seq[DStream[T]]
  ) extends DStream[U](parents.head.ssc) {

  if (parents.map(_.ssc).distinct.size > 1) {
    throw new IllegalArgumentException("Array of parents have different StreamingContexts")
  }

  if (parents.map(_.slideDuration).distinct.size > 1) {
    throw new IllegalArgumentException("Array of parents have different slide times")
  }

  private var _transformFuncOpt: Option[(Seq[RDD[T]], Time) => RDD[U]] = None

  def this(
      parents: Seq[DStream[T]],
      transformFunc: (Seq[RDD[T]], Time) => RDD[U]) {
    this(parents)
    setTransformFunc(transformFunc)
  }

  def setTransformFunc(transformFunc: (Seq[RDD[T]], Time) => RDD[U]) {
    _transformFuncOpt = Some(transformFunc)
  }

  def getTransformFunc = _transformFuncOpt

  override def dependencies = parents.toList

  override def slideDuration: Duration = parents.head.slideDuration

  override def compute(validTime: Time): Option[RDD[U]] = {
    assert(_transformFuncOpt.isDefined,
      "NaryTransformedDStream's transformFunc should be set before computation begins.")
    val rdds = parents.flatMap(_.getOrCompute(validTime))
    if (rdds.size > 0) {
      _transformFuncOpt.map(func => func(rdds, validTime))
    } else {
      None
    }
  }
}
