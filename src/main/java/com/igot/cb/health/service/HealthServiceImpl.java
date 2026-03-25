package com.igot.cb.health.service;


import com.igot.cb.cache.RedisCacheMgr;
import com.igot.cb.cassandra.CassandraOperation;
import com.igot.cb.cassandra.exceptions.CustomException;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.model.ApiResponse;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class HealthServiceImpl implements HealthService {


    @Autowired
    CassandraOperation cassandraOperation;

    @Autowired
    RedisCacheMgr redisCacheMgr;

    @Autowired
    EsUtilService esClientService;

    @Autowired
    AdminClient adminClient;

    private Logger log = LoggerFactory.getLogger(getClass().getName());

    @Override
    public ApiResponse checkHealthStatus(String requestId) throws Exception {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_HEALTH_CHECK);
        Map<String, Object> responseObj = new HashMap<>();
        response.getParams().setMsgId(requestId);
        response.getParams().setResMsgId(requestId);
        try {

            List<Map<String, Object>> healthResults = new ArrayList<>();
            cassandraHealthStatus(healthResults);
            elasticsearchHealthStatus(healthResults);
            redisHealthStatus(healthResults);
            kafkaHealthStatus(healthResults);

            responseObj.put(Constants.CHECKS, healthResults);
            responseObj.put(Constants.NAME,Constants.ALL_HEALTH_CHECK);
            responseObj.put(Constants.HEALTHY, Constants.TRUE_1);
            response.put(Constants.RESPONSE, responseObj);

        } catch (Exception e) {
            log.error("Failed to process health check. Exception: ", e);
            response.put(Constants.HEALTHY, false);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    public void cassandraHealthStatus(List<Map<String, Object>> response) throws Exception {
        Map<String, Object> result = ProjectUtil.createDefaultMapResponse(Constants.CASSANDRA_DB, null, null);
        try {
            List<Map<String, Object>> cassandraQueryResponse = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD, Constants.TABLE_SYSTEM_SETTINGS,  null, null,null);
            if (cassandraQueryResponse.isEmpty()) {
                setErrorDetails( result, new CustomException(Constants.CASSANDRA_DB +" Down", "Cassandra query returned empty result",
                        HttpStatus.SERVICE_UNAVAILABLE));
            }
        } catch (Exception e) {
            setErrorDetails( result, new CustomException(Constants.CASSANDRA_DB +" Down", e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR));
        }
        response.add(result);
    }

    private void redisHealthStatus(List<Map<String, Object>> response) {

        Map<String, Object> result = ProjectUtil.createDefaultMapResponse(Constants.REDIS_CACHE,null,null);
        boolean isHealthy = true;

        try{
            isHealthy = redisCacheMgr.isRedisHealthy();

            if (!isHealthy) {
                setErrorDetails( result, new CustomException(Constants.REDIS_CACHE +" Down", "Redis is unhealthy",
                        HttpStatus.SERVICE_UNAVAILABLE));
            }
        }catch (Exception e) {
            setErrorDetails( result, new CustomException(Constants.REDIS_CACHE +" Down", e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR));
        }

        response.add(result);
    }

    private void elasticsearchHealthStatus(List<Map<String, Object>> response) {

        Map<String, Object> result = ProjectUtil.createDefaultMapResponse(Constants.ELASTIC_SEARCH,null,null);
        boolean isHealthy = true;
        try {
            isHealthy = esClientService.isElasticsearchHealthy();

            if (!isHealthy) {

                setErrorDetails( result, new CustomException(Constants.ELASTIC_SEARCH +" Down", "Elasticsearch service is unhealthy",
                        HttpStatus.SERVICE_UNAVAILABLE));
            }

        }catch (Exception e) {
            setErrorDetails( result, new CustomException(Constants.ELASTIC_SEARCH +" Down", e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR));
        }
        response.add(result);
    }

    private void kafkaHealthStatus(List<Map<String, Object>> response) {
        Map<String, Object> result = ProjectUtil.createDefaultMapResponse(Constants.KAFKA_SERVICE,null,null);
        try {
            DescribeClusterResult clusterResult = adminClient.describeCluster();
            clusterResult.nodes().get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            setErrorDetails( result, new CustomException(Constants.KAFKA_SERVICE +" Down", e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR));
        }
        response.add(result);
    }


    private void setErrorDetails(Map<String, Object> response, CustomException e) {

        response.put(Constants.HEALTHY,Constants.FALSE);
        response.put(Constants.ERR, e.getHttpStatusCode().value());
        response.put(Constants.ERROR_MESSAGE, e.getMessage()!=null ? e.getMessage() : e.getHttpStatusCode().getReasonPhrase());
    }

}

