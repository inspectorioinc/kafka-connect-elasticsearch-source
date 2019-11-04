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

package com.github.dariobalinzo.task;

import com.github.dariobalinzo.ElasticSourceConnectorConfig;
import com.github.dariobalinzo.schema.SchemaConverter;
import com.github.dariobalinzo.schema.StructConverter;
import com.github.dariobalinzo.utils.ElasticConnection;
import com.github.dariobalinzo.utils.Utils;
import com.github.dariobalinzo.utils.Version;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.elasticsearch.action.search.*;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

public class ElasticSourceTask extends SourceTask {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSourceTask.class);
    private static final String INDEX = "index";
    private static final String POSITION = "position";

    private ElasticSourceTaskConfig config;
    private ElasticConnection es;

    private AtomicBoolean stopping = new AtomicBoolean(false);
    private List<String> indices;
    private List<String> mappingTypes;
    private List<String> whitelistFields;
    private List<String> castStringFields;
    private long connectionRetryBackoff;
    private int maxConnectionAttempts;
    private String topic;
    private String incrementingField;
    private int size;
    private int pollingMs;
    private Map<String,String> last = new HashMap<>();
    private Map<String,Integer> sent = new HashMap<>();

    public ElasticSourceTask() {

    }

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(Map<String, String> properties) {
        try {
            config = new ElasticSourceTaskConfig(properties);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start ElasticSourceTask due to configuration error", e);
        }

        initEsConnection();

        indices = Arrays.asList(config.getString(ElasticSourceTaskConfig.INDICES_CONFIG).split(","));
        if (indices.isEmpty()) {
            throw new ConnectException("Invalid configuration: each ElasticSourceTask must have at "
                    + "least one index assigned to it");
        }

        topic = config.getString(ElasticSourceConnectorConfig.TOPIC_PREFIX_CONFIG);
        incrementingField = config.getString(ElasticSourceConnectorConfig.INCREMENTING_FIELD_NAME_CONFIG);
        size = Integer.parseInt(config.getString(ElasticSourceConnectorConfig.BATCH_MAX_ROWS_CONFIG));
        pollingMs = Integer.parseInt(config.getString(ElasticSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG));
        mappingTypes = Arrays.asList(config.getString(ElasticSourceTaskConfig.MAPPING_TYPE_PREFIX_CONFIG).split(","));
        whitelistFields = Utils.getArrayList(config.getString(ElasticSourceTaskConfig.WHITELIST_FIELDS_CONFIG));
        castStringFields = Utils.getArrayList(config.getString(ElasticSourceTaskConfig.CAST_STRING_FIELDS_CONFIG));
    }

    private void initEsConnection() {

        final String esScheme = config.getString(ElasticSourceConnectorConfig.ES_SCHEME_CONF);
        final String esHost = config.getString(ElasticSourceConnectorConfig.ES_HOST_CONF);
        final int esPort = Integer.parseInt(config.getString(ElasticSourceConnectorConfig.ES_PORT_CONF));

        final String esUser = config.getString(ElasticSourceConnectorConfig.ES_USER_CONF);
        final String esPwd = config.getString(ElasticSourceConnectorConfig.ES_PWD_CONF);

        maxConnectionAttempts = Integer.parseInt(config.getString(
                ElasticSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG
        ));
        connectionRetryBackoff = Long.parseLong(config.getString(
                ElasticSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG
        ));

        if (esUser == null || esUser.isEmpty()) {
            es = new ElasticConnection(
                    esHost,
                    esPort,
                    esScheme,
                    maxConnectionAttempts,
                    connectionRetryBackoff
            );
        } else {
            es = new ElasticConnection(
                    esHost,
                    esPort,
                    esScheme,
                    esUser,
                    esPwd,
                    maxConnectionAttempts,
                    connectionRetryBackoff
            );

        }

    }


    //will be called by connect with a different thread than the stop thread
    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        List<SourceRecord> results = new ArrayList<>();
        indices.forEach(
                index -> {
                    if (!stopping.get()) {
                        logger.info("fetching from {}", index);
                        String lastValue = fetchLastOffset(index);
                        logger.info("found last value {}", lastValue);
                        if (lastValue != null) {
                            executeScroll(index, lastValue, results);
                        }
                        logger.info("index {} total messages: {} ",index,sent.get(index));
                    }
                }
        );
        if (results.isEmpty()) {
            logger.info("no data found, sleeping for {} ms",pollingMs);
            Thread.sleep(pollingMs);
        }
        return results;
    }

    private String fetchLastOffset(String index) {


        //first we check in cache memory the last value
        if (last.get(index)!= null) {
            return last.get(index);
        }

        //if cache is empty we check the framework
        Map<String, Object> offset = context.offsetStorageReader().offset(Collections.singletonMap(INDEX, index));
        if (offset != null) {
            return (String) offset.get(POSITION);
        } else {
            //first execution, no last value
            //fetching the lower level directly to the elastic index (if it is not empty)
            SearchRequest searchRequest = new SearchRequest(index);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(
                    matchAllQuery()
            ).sort(incrementingField, SortOrder.ASC);
            searchSourceBuilder.size(1); // only one record
            searchRequest.source(searchSourceBuilder);

            if (mappingTypes.size() > 0) {
                searchRequest.types(mappingTypes.toArray(new String[mappingTypes.size()]));
            }

            SearchResponse searchResponse = null;
            try {
                for (int i = 0; i < maxConnectionAttempts; ++i) {
                    try {
                        searchResponse = es.getClient().search(searchRequest);
                        break;
                    } catch (IOException e) {
                        logger.error("error in scroll");
                        Thread.sleep(connectionRetryBackoff);
                    }
                }
                if (searchResponse == null) {
                    throw new RuntimeException("connection failed");
                }
                SearchHits hits = searchResponse.getHits();
                int totalShards = searchResponse.getTotalShards();
                int successfulShards = searchResponse.getSuccessfulShards();

                logger.info("total shard {}, successuful: {}", totalShards, successfulShards);

                int failedShards = searchResponse.getFailedShards();
                for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
                    // failures should be handled here
                    logger.error("failed {}", failure);
                }
                if (failedShards > 0) {
                    throw new RuntimeException("failed shard in search");
                }

                SearchHit[] searchHits = hits.getHits();
                //here only one record
                for (SearchHit hit : searchHits) {
                    // do something with the SearchHit
                    return hit.getSourceAsMap().get(incrementingField).toString();

                }

            } catch (Exception e) {
                logger.error("error fetching min value", e);
                return null;
            }
        }
        return null;

    }

    private void executeScroll(String index, String lastValue, List<SourceRecord> results) {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.scroll(TimeValue.timeValueMinutes(1L));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                rangeQuery(incrementingField)
                        .from(lastValue, last.get(index)== null)
        ).sort(incrementingField, SortOrder.ASC); //TODO configure custom query
        searchSourceBuilder.size(1000);
        searchRequest.source(searchSourceBuilder);

        if (mappingTypes.size() > 0) {
            searchRequest.types(mappingTypes.toArray(new String[mappingTypes.size()]));
        }
        SearchResponse searchResponse = null;
        String scrollId = null;
        try {
            for (int i = 0; i < maxConnectionAttempts; ++i) {
                try {
                    searchResponse = es.getClient().search(searchRequest);
                    break;
                } catch (IOException e) {
                    logger.error("error in scroll");
                    Thread.sleep(connectionRetryBackoff);
                }
            }
            if (searchResponse == null) {
                throw new RuntimeException("connection failed");
            }
            scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = parseSearchResult(index, lastValue, results, searchResponse, scrollId);

            while (!stopping.get() && searchHits != null && searchHits.length > 0 && results.size() < size) {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(TimeValue.timeValueMinutes(1L));
                searchResponse = es.getClient().searchScroll(scrollRequest);
                scrollId = searchResponse.getScrollId();
                searchHits = parseSearchResult(index, lastValue, results, searchResponse, scrollId);
            }
        } catch (Throwable t) {
            logger.error("error", t);
        } finally {
            closeScrollQuietly(scrollId);
        }


    }

    private SearchHit[] parseSearchResult(String index, String lastValue, List<SourceRecord> results, SearchResponse searchResponse, Object scrollId) {

        if (results.size() > size) {
            return null; //nothing to do: limit reached
        }

        SearchHits hits = searchResponse.getHits();
        int totalShards = searchResponse.getTotalShards();
        int successfulShards = searchResponse.getSuccessfulShards();

        logger.info("total shard {}, successuful: {}", totalShards, successfulShards);
        logger.info("retrived {}, scroll id : {}", hits, scrollId);

        int failedShards = searchResponse.getFailedShards();
        for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
            // failures should be handled here
            logger.error("failed {}", failure);
        }
        if (failedShards > 0) {
            throw new RuntimeException("failed shard in search");
        }

        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            // do something with the SearchHit
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            Map sourcePartition = Collections.singletonMap(INDEX, index);
            Map sourceOffset = Collections.singletonMap(POSITION, sourceAsMap.get(incrementingField).toString());
            Schema schema = SchemaConverter.convertElasticMapping2AvroSchema(sourceAsMap, index, whitelistFields, castStringFields);
            Struct struct = StructConverter.convertElasticDocument2AvroStruct(sourceAsMap, schema, whitelistFields, castStringFields);

            //document key
            String key = String.join("_", hit.getIndex(), hit.getType(), hit.getId());

            SourceRecord sourceRecord = new SourceRecord(
                    sourcePartition,
                    sourceOffset,
                    topic + index,
                    //KEY
                    Schema.STRING_SCHEMA,
                    key,
                    //VALUE
                    schema,
                    struct);
            results.add(sourceRecord);

            last.put(index,sourceAsMap.get(incrementingField).toString());
            sent.merge(index, 1, Integer::sum);
        }
        return searchHits;
    }

    private void closeScrollQuietly(String scrollId) {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = null;
        try {
            clearScrollResponse = es.getClient().clearScroll(clearScrollRequest);
        } catch (IOException e) {
            logger.error("error in clear scroll", e);
        }
        boolean succeeded = clearScrollResponse != null && clearScrollResponse.isSucceeded();
        logger.info("scroll {} cleared: {}", scrollId, succeeded);
    }

    //will be called by connect with a different thread than poll thread
    public void stop() {

        if (stopping != null) {
            stopping.set(true);
        }

        if (es != null) {
            es.closeQuietly();
        }

    }

    //utility method for testing
    public void setupTest(List<String> index) {

        final String esHost = "localhost";
        final int esPort = 9200;
        final String scheme = "http";

        maxConnectionAttempts = 3;
        connectionRetryBackoff = 1000;
        es = new ElasticConnection(
                esHost,
                esPort,
                scheme,
                maxConnectionAttempts,
                connectionRetryBackoff
        );

        indices = index;
        if (indices.isEmpty()) {
            throw new ConnectException("Invalid configuration: each ElasticSourceTask must have at "
                    + "least one index assigned to it");
        }

        topic = "connect_";
        incrementingField = "@timestamp";
        size = 10000;
        pollingMs = 1000;
    }
}
