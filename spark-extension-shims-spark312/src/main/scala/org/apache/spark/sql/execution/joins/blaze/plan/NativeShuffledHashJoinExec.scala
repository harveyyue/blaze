/*
 * Copyright 2022 The Blaze Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.joins.blaze.plan

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.blaze.plan.{BuildLeft, BuildRight, BuildSide, NativeShuffledHashJoinBase}
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec

case class NativeShuffledHashJoinExec(
    override val left: SparkPlan,
    override val right: SparkPlan,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide)
    extends NativeShuffledHashJoinBase(left, right, leftKeys, rightKeys, joinType, buildSide) {

  override val (output, outputPartitioning, outputOrdering) = {
    val sparkBuildSide = buildSide match {
      case BuildLeft => org.apache.spark.sql.catalyst.optimizer.BuildLeft
      case BuildRight => org.apache.spark.sql.catalyst.optimizer.BuildRight
    }
    val shj =
      ShuffledHashJoinExec(leftKeys, rightKeys, joinType, sparkBuildSide, None, left, right)
    (shj.output, shj.outputPartitioning, shj.outputOrdering)
  }

  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(left = newChildren(0), right = newChildren(1))
}
