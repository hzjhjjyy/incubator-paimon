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

package org.apache.paimon.flink.action.cdc.mongodb;

import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionFactory;
import org.apache.paimon.flink.action.MultipleParameterToolAdapter;

import java.util.Optional;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.EXCLUDING_TABLES;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.INCLUDING_TABLES;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.MONGODB_CONF;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.TABLE_PREFIX;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.TABLE_SUFFIX;

/** Factory to create {@link MongoDBSyncDatabaseAction}. */
public class MongoDBSyncDatabaseActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "mongodb_sync_database";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    public Optional<Action> create(MultipleParameterToolAdapter params) {
        checkRequiredArgument(params, MONGODB_CONF);

        MongoDBSyncDatabaseAction action =
                new MongoDBSyncDatabaseAction(
                        getRequiredValue(params, WAREHOUSE),
                        getRequiredValue(params, DATABASE),
                        optionalConfigMap(params, CATALOG_CONF),
                        optionalConfigMap(params, MONGODB_CONF));

        action.withTablePrefix(params.get(TABLE_PREFIX))
                .withTableSuffix(params.get(TABLE_SUFFIX))
                .includingTables(params.get(INCLUDING_TABLES))
                .excludingTables(params.get(EXCLUDING_TABLES))
                .withTableConfig(optionalConfigMap(params, TABLE_CONF));

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"mongodb_sync_database\" creates a streaming job "
                        + "with a Flink MongoDB CDC source and multiple Paimon table sinks "
                        + "to synchronize a whole MongoDB database into one Paimon database.\n"
                        + "Only MongoDB tables with a primary key that includes `_id` will be taken into consideration."
                        + "Any MongoDB tables created after the commencement of the task will automatically be included.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  mongodb_sync_database --warehouse <warehouse_path> --database <database_name> "
                        + "[--table_prefix <paimon_table_prefix>] "
                        + "[--table_suffix <paimon_table_suffix>] "
                        + "[--including_tables <mongodb_table_name|name_regular_expr>] "
                        + "[--excluding_tables <mongodb_table_name|name_regular_expr>] "
                        + "[--mongodb_conf <mongodb_cdc_source_conf> [--mongodb_conf <mongodb_cdc_source_conf> ...]] "
                        + "[--catalog_conf <paimon_catalog_conf> [--catalog_conf <paimon_catalog_conf> ...]] "
                        + "[--table_conf <paimon_table_sink_conf> [--table_conf <paimon_table_sink_conf> ...]]");
        System.out.println();

        System.out.println(
                "--table_prefix is the prefix of all Paimon tables to be synchronized. For example, if you want all "
                        + "synchronized tables to have \"ods_\" as prefix, you can specify `--table_prefix ods_`.");
        System.out.println("The usage of --table_suffix is same as `--table_prefix`");
        System.out.println();

        System.out.println(
                "--including_tables is used to specify which source tables are to be synchronized. "
                        + "You must use '|' to separate multiple tables. Regular expression is supported.");
        System.out.println(
                "--excluding_tables is used to specify which source tables are not to be synchronized. "
                        + "The usage is same as --including_tables.");
        System.out.println(
                "--excluding_tables has higher priority than --including_tables if you specified both.");
        System.out.println();

        System.out.println("MongoDB CDC source conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "'hosts', 'username', 'password' and 'database' "
                        + "are required configurations, others are optional. "
                        + "Note that 'database' should be the exact name "
                        + "of the MongoDB database you want to synchronize. "
                        + "It can't be a regular expression.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mongodb-cdc.html#connector-options");
        System.out.println();

        System.out.println("Paimon catalog and table sink conf syntax:");
        System.out.println("  key=value");
        System.out.println("All Paimon sink table will be applied the same set of configurations.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://paimon.apache.org/docs/master/maintenance/configurations/");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  mongodb_sync_database \\\n"
                        + "    --warehouse hdfs:///path/to/warehouse \\\n"
                        + "    --database test_db \\\n"
                        + "    --mongodb_conf hosts=127.0.0.1:27017 \\\n"
                        + "    --mongodb_conf username=root \\\n"
                        + "    --mongodb_conf password=123456 \\\n"
                        + "    --mongodb_conf database=source_db \\\n"
                        + "    --catalog_conf metastore=hive \\\n"
                        + "    --catalog_conf uri=thrift://hive-metastore:9083 \\\n"
                        + "    --table_conf bucket=4 \\\n"
                        + "    --table_conf changelog-producer=input \\\n"
                        + "    --table_conf sink.parallelism=4");
    }
}
