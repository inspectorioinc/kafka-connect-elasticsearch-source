/*
 * Copyright © 2018 Dario Balinzo (dariobalinzo@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.dariobalinzo;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Collections;
import java.util.Map;

public class ElasticSourceConnectorConfig extends AbstractConfig {

    //TODO add the possibility to specify multiple hosts
    public final static String ES_SCHEME_CONF = "es.scheme";
    private final static String ES_SCHEME_DOC = "Elasticsearch scheme (default: http)";
    private final static String ES_SCHEME_DISPLAY = "Elasticsearch scheme";
    private static final String ES_SCHEME_DEFAULT = "http";

    public final static String ES_HOST_CONF = "es.host";
    private final static String ES_HOST_DOC = "ElasticSearch host";
    private final static String ES_HOST_DISPLAY = "Elastic host";

    public final static String ES_PORT_CONF = "es.port";
    private final static String ES_PORT_DOC = "ElasticSearch port";
    private final static String ES_PORT_DISPLAY = "ElasticSearch port";

    public final static String ES_USER_CONF = "es.user";
    private final static String ES_USER_DOC = "Elasticsearch username";
    private final static String ES_USER_DISPLAY = "Elasticsearch username";

    public final static String ES_PWD_CONF = "es.password";
    private final static String ES_PWD_DOC = "Elasticsearch password";
    private final static String ES_PWD_DISPLAY = "Elasticsearch password";

    public static final String CONNECTION_ATTEMPTS_CONFIG = "connection.attempts";
    private static final String CONNECTION_ATTEMPTS_DOC
            = "Maximum number of attempts to retrieve a valid Elasticsearch connection.";
    private static final String CONNECTION_ATTEMPTS_DISPLAY = "Elasticsearch connection attempts";
    private static final String CONNECTION_ATTEMPTS_DEFAULT = "3";

    public static final String CONNECTION_BACKOFF_CONFIG = "connection.backoff.ms";
    private static final String CONNECTION_BACKOFF_DOC
            = "Backoff time in milliseconds between connection attempts.";
    private static final String CONNECTION_BACKOFF_DISPLAY
            = "Elastic connection backoff in milliseconds";
    private static final String CONNECTION_BACKOFF_DEFAULT = "10000";

    public static final String POLL_INTERVAL_MS_CONFIG = "poll.interval.ms";
    private static final String POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new data in "
            + "each index.";
    private static final String POLL_INTERVAL_MS_DEFAULT = "5000";
    private static final String POLL_INTERVAL_MS_DISPLAY = "Poll Interval (ms)";

    public static final String BATCH_MAX_ROWS_CONFIG = "batch.max.rows";
    private static final String BATCH_MAX_ROWS_DOC =
            "Maximum number of documents to include in a single batch when polling for new data.";
    private static final String BATCH_MAX_ROWS_DEFAULT = "10000";
    private static final String BATCH_MAX_ROWS_DISPLAY = "Max Documents Per Batch";

    private static final String MODE_UNSPECIFIED = "";
    private static final String MODE_BULK = "bulk";
    private static final String MODE_TIMESTAMP = "timestamp";
    private static final String MODE_INCREMENTING = "incrementing";
    private static final String MODE_TIMESTAMP_INCREMENTING = "timestamp+incrementing";

    public static final String INCREMENTING_FIELD_NAME_CONFIG = "incrementing.field.name";
    private static final String INCREMENTING_FIELD_NAME_DOC =
            "The name of the strictly incrementing field to use to detect new records.";
    private static final String INCREMENTING_FIELD_NAME_DEFAULT = "";
    private static final String INCREMENTING_FIELD_NAME_DISPLAY = "Incrementing Field Name";

    public static final String INDEX_PREFIX_CONFIG = "index.prefix";
    private static final String INDEX_PREFIX_DOC = "List of indices to include in copying.";
    private static final String INDEX_PREFIX_DEFAULT = "";
    private static final String INDEX_PREFIX_DISPLAY = "Indices prefix Whitelist";

    public static final String IS_INDEX_ALIAS_PREFIX_CONFIG = "is.index.alias.prefix";
    private static final String IS_INDEX_ALIAS_PREFIX_DOC = "Is query indices aliases to include in copying.";
    private static final String IS_INDEX_ALIAS_PREFIX_DEFAULT = "";
    private static final String IS_INDEX_ALIAS_PREFIX_DISPLAY = "Is prefix of indices aliases Whitelist";

    public static final String  MAPPING_TYPE_PREFIX_CONFIG = "index.mapping.type";
    private static final String MAPPING_TYPE_PREFIX_DOC = "List of mapping type to include in copying.";
    private static final String MAPPING_TYPE_PREFIX_DEFAULT = "";
    private static final String MAPPING_TYPE_PREFIX_DISPLAY = "Mapping type prefix Whitelist";

    public static final String  WHITELIST_FIELDS_CONFIG = "whitelist.fields";
    private static final String WHITELIST_FIELDS_DOC = "List of whitelist fields accept.";
    private static final String WHITELIST_FIELDS_DEFAULT = "";
    private static final String WHITELIST_FIELDS_DISPLAY = "Mapping fields Whitelist";

    public static final String  CAST_STRING_FIELDS_CONFIG = "cast.string.fields";
    private static final String CAST_STRING_FIELDS_DOC = "List of array, struct, map fields cast to string";
    private static final String CAST_STRING_FIELDS_DEFAULT = "";
    private static final String CAST_STRING_FIELDS_DISPLAY = "List fields cast to string";


    public static final String TOPIC_PREFIX_CONFIG = "topic.prefix";
    private static final String TOPIC_PREFIX_DOC =
            "Prefix to prepend to index names to generate the name of the Kafka topic to publish data";
    private static final String TOPIC_PREFIX_DISPLAY = "Topic Prefix";

    private static final String DATABASE_GROUP = "Elasticsearch";
    private static final String MODE_GROUP = "Mode";
    private static final String CONNECTOR_GROUP = "Connector";

    private static final String MODE_CONFIG = "mode";
    private static final String MODE_DOC = "";
    private static final String MODE_DISPLAY = "Index Incrementing field";

    public static final String INDICES_CONFIG = "es.indices";


    public static final ConfigDef CONFIG_DEF = baseConfigDef();


    protected static ConfigDef baseConfigDef() {
        ConfigDef config = new ConfigDef();
        addDatabaseOptions(config);
        addModeOptions(config);
        addConnectorOptions(config);
        return config;
    }

    private static void addDatabaseOptions(ConfigDef config) {
        int orderInGroup = 0;
        config.define(
                ES_SCHEME_CONF,
                Type.STRING,
                ES_SCHEME_DEFAULT,
                Importance.HIGH,
                ES_SCHEME_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                ES_SCHEME_DISPLAY,
                Collections.singletonList(INDEX_PREFIX_CONFIG)
        ).define(
                ES_HOST_CONF,
                Type.STRING,
                Importance.HIGH,
                ES_HOST_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                ES_HOST_DISPLAY,
                Collections.singletonList(INDEX_PREFIX_CONFIG)
        ).define(
                ES_PORT_CONF,
                Type.STRING,
                Importance.HIGH,
                ES_PORT_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                ES_PORT_DISPLAY,
                Collections.singletonList(INDEX_PREFIX_CONFIG)
        ).define(
                ES_USER_CONF,
                Type.STRING,
                null,
                Importance.HIGH,
                ES_USER_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                ES_USER_DISPLAY
        ).define(
                ES_PWD_CONF,
                Type.STRING,
                null,
                Importance.HIGH,
                ES_PWD_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                ES_PWD_DISPLAY
        ).define(
                CONNECTION_ATTEMPTS_CONFIG,
                Type.STRING,
                CONNECTION_ATTEMPTS_DEFAULT,
                Importance.LOW,
                CONNECTION_ATTEMPTS_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                CONNECTION_ATTEMPTS_DISPLAY
        ).define(
                CONNECTION_BACKOFF_CONFIG,
                Type.STRING,
                CONNECTION_BACKOFF_DEFAULT,
                Importance.LOW,
                CONNECTION_BACKOFF_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                CONNECTION_BACKOFF_DISPLAY
        ).define(
                INDEX_PREFIX_CONFIG,
                Type.STRING,
                INDEX_PREFIX_DEFAULT,
                Importance.MEDIUM,
                INDEX_PREFIX_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                INDEX_PREFIX_DISPLAY
        ).define(
                IS_INDEX_ALIAS_PREFIX_CONFIG,
                Type.STRING,
                IS_INDEX_ALIAS_PREFIX_DEFAULT,
                Importance.MEDIUM,
                IS_INDEX_ALIAS_PREFIX_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                IS_INDEX_ALIAS_PREFIX_DISPLAY
        ).define(
                MAPPING_TYPE_PREFIX_CONFIG,
                Type.STRING,
                MAPPING_TYPE_PREFIX_DEFAULT,
                Importance.MEDIUM,
                MAPPING_TYPE_PREFIX_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                MAPPING_TYPE_PREFIX_DISPLAY
        ).define(
                WHITELIST_FIELDS_CONFIG,
                Type.STRING,
                WHITELIST_FIELDS_DEFAULT,
                Importance.MEDIUM,
                WHITELIST_FIELDS_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                WHITELIST_FIELDS_DISPLAY
        ).define(
                CAST_STRING_FIELDS_CONFIG,
                Type.STRING,
                CAST_STRING_FIELDS_DEFAULT,
                Importance.MEDIUM,
                CAST_STRING_FIELDS_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                CAST_STRING_FIELDS_DISPLAY
        );
    }

    private static void addModeOptions(ConfigDef config) {
        int orderInGroup = 0;
        config.define(
                MODE_CONFIG,
                Type.STRING,
                MODE_UNSPECIFIED,
                ConfigDef.ValidString.in(
                        MODE_UNSPECIFIED,
                        MODE_BULK,
                        MODE_TIMESTAMP,
                        MODE_INCREMENTING,
                        MODE_TIMESTAMP_INCREMENTING
                ),
                Importance.HIGH,
                MODE_DOC,
                MODE_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                MODE_DISPLAY,
                Collections.singletonList(
                        INCREMENTING_FIELD_NAME_CONFIG
                )
        ).define(
                INCREMENTING_FIELD_NAME_CONFIG,
                Type.STRING,
                INCREMENTING_FIELD_NAME_DEFAULT,
                Importance.MEDIUM,
                INCREMENTING_FIELD_NAME_DOC,
                MODE_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                INCREMENTING_FIELD_NAME_DISPLAY
        );
    }

    private static void addConnectorOptions(ConfigDef config) {
        int orderInGroup = 0;
        config.define(
                POLL_INTERVAL_MS_CONFIG,
                Type.STRING,
                POLL_INTERVAL_MS_DEFAULT,
                Importance.HIGH,
                POLL_INTERVAL_MS_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.SHORT,
                POLL_INTERVAL_MS_DISPLAY
        ).define(
                BATCH_MAX_ROWS_CONFIG,
                Type.STRING,
                BATCH_MAX_ROWS_DEFAULT,
                Importance.LOW,
                BATCH_MAX_ROWS_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.SHORT,
                BATCH_MAX_ROWS_DISPLAY
        ).define(
                TOPIC_PREFIX_CONFIG,
                Type.STRING,
                Importance.HIGH,
                TOPIC_PREFIX_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                TOPIC_PREFIX_DISPLAY
        );
    }

    public ElasticSourceConnectorConfig(Map<String, String> properties) {

        super(CONFIG_DEF, properties);

    }

    protected ElasticSourceConnectorConfig(ConfigDef subclassConfigDef, Map<String, String> props) {
        super(subclassConfigDef, props);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG_DEF.toEnrichedRst());
    }
}
