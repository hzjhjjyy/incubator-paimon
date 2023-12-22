/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.paimon.spark.sql

import org.apache.paimon.CoreOptions
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.catalyst.analysis.Update

import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}

class UpdateTableTest extends PaimonSparkTestBase {

  test(s"test update append only table") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING)
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22')")

    assertThatThrownBy(() => spark.sql("UPDATE T SET name = 'a_new' WHERE id = 1"))
      .hasMessageContaining("Only support to update table with primary keys.")
  }

  CoreOptions.MergeEngine.values().foreach {
    mergeEngine =>
      {
        test(s"test update with merge engine $mergeEngine") {
          val options = if ("first-row".equals(mergeEngine.toString)) {
            s"'primary-key' = 'id', 'merge-engine' = '$mergeEngine', 'changelog-producer' = 'lookup'"
          } else {
            s"'primary-key' = 'id', 'merge-engine' = '$mergeEngine'"
          }
          spark.sql(s"""
                       |CREATE TABLE T (id INT, name STRING, dt STRING)
                       |TBLPROPERTIES ($options)
                       |""".stripMargin)

          spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22')")

          if (Update.supportedMergeEngine.contains(mergeEngine)) {
            spark.sql("UPDATE T SET name = 'a_new' WHERE id = 1")
            val rows = spark.sql("SELECT * FROM T ORDER BY id").collectAsList()
            assertThat(rows.toString).isEqualTo("[[1,a_new,11], [2,b,22]]")
          } else
            assertThatThrownBy(() => spark.sql("UPDATE T SET name = 'a_new' WHERE id = 1"))
              .isInstanceOf(classOf[UnsupportedOperationException])
        }
      }
  }

  test(s"test update with primary key") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING)
                 |TBLPROPERTIES ('primary-key' = 'id', 'merge-engine' = 'deduplicate')
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22'), (3, 'c', '33')")

    assertThatThrownBy(() => spark.sql("UPDATE T SET id = 11 WHERE name = 'a'"))
      .hasMessageContaining("Can't update the primary key column.")
  }

  test(s"test update with no where") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING)
                 |TBLPROPERTIES ('primary-key' = 'id, dt', 'merge-engine' = 'deduplicate')
                 |PARTITIONED BY (id)
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22'), (3, 'c', '33')")

    spark.sql("UPDATE T SET name = 'a_new'")
    val rows = spark.sql("SELECT * FROM T ORDER BY id").collectAsList()
    assertThat(rows.toString).isEqualTo("[[1,a_new,11], [2,a_new,22], [3,a_new,33]]")
  }

  test(s"test update with alias") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING)
                 |TBLPROPERTIES ('primary-key' = 'id, dt', 'merge-engine' = 'deduplicate')
                 |PARTITIONED BY (id)
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22'), (3, 'c', '33')")

    spark.sql("UPDATE T AS t SET t.name = 'a_new' where id = 1")
    val rows = spark.sql("SELECT * FROM T ORDER BY id").collectAsList()
    assertThat(rows.toString).isEqualTo("[[1,a_new,11], [2,b,22], [3,c,33]]")
  }

  test(s"test update with alias assignment") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, c1 INT, c2 INT)
                 |TBLPROPERTIES ('primary-key' = 'id', 'merge-engine' = 'deduplicate')
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 1, 11), (2, 2, 22), (3, 3, 33)")

    spark.sql("UPDATE T set c1 = c1 + 1, c2 = c2 + 1 where id = 1")
    val rows = spark.sql("SELECT * FROM T ORDER BY id").collectAsList()
    assertThat(rows.toString).isEqualTo("[[1,2,12], [2,2,22], [3,3,33]]")
  }

  test(s"test update with in condition and not in condition") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING)
                 |TBLPROPERTIES ('primary-key' = 'id, dt', 'merge-engine' = 'deduplicate')
                 |PARTITIONED BY (dt)
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22'), (3, 'c', '33')")

    spark.sql("UPDATE T set name = 'in_new' WHERE id IN (1)")
    val rows1 = spark.sql("SELECT * FROM T ORDER BY id").collectAsList()
    assertThat(rows1.toString).isEqualTo("[[1,in_new,11], [2,b,22], [3,c,33]]")

    spark.sql("UPDATE T set name = 'not_in_new' WHERE id NOT IN (2)")
    val rows2 = spark.sql("SELECT * FROM T ORDER BY id").collectAsList()
    assertThat(rows2.toString).isEqualTo("[[1,not_in_new,11], [2,b,22], [3,not_in_new,33]]")
  }

  test(s"test update with in subquery") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING)
                 |TBLPROPERTIES ('primary-key' = 'id, dt', 'merge-engine' = 'deduplicate')
                 |PARTITIONED BY (dt)
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22'), (3, 'c', '33')")

    import testImplicits._
    val df = Seq(1, 2).toDF("id")
    df.createOrReplaceTempView("updated_ids")
    spark.sql("UPDATE T set name = 'in_new' WHERE id IN (SELECT * FROM updated_ids)")
    val rows = spark.sql("SELECT * FROM T ORDER BY id").collectAsList()
    assertThat(rows.toString).isEqualTo("[[1,in_new,11], [2,in_new,22], [3,c,33]]")
  }

  test(s"test update with self subquery") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING)
                 |TBLPROPERTIES ('primary-key' = 'id, dt', 'merge-engine' = 'deduplicate')
                 |PARTITIONED BY (dt)
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22'), (3, 'c', '33')")

    spark.sql("UPDATE T set name = 'in_new' WHERE id IN (SELECT id + 1 FROM T)")
    val rows = spark.sql("SELECT * FROM T ORDER BY id").collectAsList()
    assertThat(rows.toString).isEqualTo("[[1,a,11], [2,in_new,22], [3,in_new,33]]")
  }

  test(s"test update with various column references") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, c1 INT, c2 INT, dt STRING)
                 |TBLPROPERTIES ('primary-key' = 'id, dt', 'merge-engine' = 'deduplicate')
                 |PARTITIONED BY (dt)
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 1, 10, '11'), (2, 2, 20, '22'), (3, 3, 300, '33')")

    spark.sql("UPDATE T SET c1 = c2 + 1, c2 = 1000")
    val rows = spark.sql("SELECT * FROM T ORDER BY id").collectAsList()
    assertThat(rows.toString).isEqualTo("[[1,11,1000,11], [2,21,1000,22], [3,301,1000,33]]")
  }

  test(s"test update with struct column") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, s STRUCT<c1: INT, c2: STRING>, dt STRING)
                 |TBLPROPERTIES ('primary-key' = 'id, dt', 'merge-engine' = 'deduplicate')
                 |PARTITIONED BY (dt)
                 |""".stripMargin)

    spark.sql(
      "INSERT INTO T VALUES (1, struct(1, 'a'), '11'), (2, struct(2, 'b'), '22'), (3, struct(3, 'c'), '33')")

    spark.sql("UPDATE T SET s.c2 = 'a_new' WHERE s.c1 = 1")
    val rows = spark.sql("SELECT * FROM T ORDER BY id").collectAsList()
    assertThat(rows.toString).isEqualTo("[[1,[1,a_new],11], [2,[2,b],22], [3,[3,c],33]]")
  }

  test(s"test update with map column") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, m MAP<INT, STRING>, dt STRING)
                 |TBLPROPERTIES ('primary-key' = 'id, dt', 'merge-engine' = 'deduplicate')
                 |PARTITIONED BY (dt)
                 |""".stripMargin)

    spark.sql(
      "INSERT INTO T VALUES (1, map(1, 'a'), '11'), (2, map(2, 'b'), '22'), (3, map(3, 'c'), '33')")

    assertThatThrownBy(() => spark.sql("UPDATE T SET m.key = 11 WHERE id = 1"))
      .hasMessageContaining("Unsupported update expression")

    spark.sql("UPDATE T SET m = map(11, 'a_new') WHERE id = 1")
    val rows = spark.sql("SELECT * FROM T ORDER BY id").collectAsList()
    assertThat(rows.toString).isEqualTo(
      "[[1,Map(11 -> a_new),11], [2,Map(2 -> b),22], [3,Map(3 -> c),33]]")
  }

  test(s"test update with conflicted column") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, s STRUCT<c1: INT, c2: STRING>, dt STRING)
                 |TBLPROPERTIES ('primary-key' = 'id, dt', 'merge-engine' = 'deduplicate')
                 |PARTITIONED BY (dt)
                 |""".stripMargin)

    spark.sql(
      "INSERT INTO T VALUES (1, struct(1, 'a'), '11'), (2, struct(2, 'b'), '22'), (3, struct(3, 'c'), '33')")

    assertThatThrownBy(
      () => spark.sql("UPDATE T SET s.c2 = 'a_new', s = struct(11, 'a_new') WHERE s.c1 = 1"))
      .hasMessageContaining("Conflicting update/insert on attrs: s.c2, s")
  }
}
