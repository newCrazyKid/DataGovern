package com.asiainfo.ctc.datagovern.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.asiainfo.ctc.datagovern.util.HttpAuth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.RestTemplate;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;

/**
 * created by xiejialin on 20190704
 */
@Controller
public class DataGovern {
    @Autowired
    private RestTemplate restTemplate;

    private static final Logger logger = LoggerFactory.getLogger(DataGovern.class);

    @ResponseBody
    @RequestMapping(value = "/datagovern", method = {RequestMethod.GET, RequestMethod.POST}, produces = "application/json;charset=utf-8")
    public String getTableInfo(){
        HttpEntity<String> httpEntity = HttpAuth.getHttpEntity();

        String hivedbUrl = "http://10.62.228.14:21000/api/atlas/v2/search/basic?typeName=hive_db";
        ResponseEntity<String> dbResponse = restTemplate.exchange(hivedbUrl, HttpMethod.GET, httpEntity, String.class);
        String hivetableUrl = "http://10.62.228.14:21000/api/atlas/v2/search/basic?typeName=hive_table";
        ResponseEntity<String> tableResponse = restTemplate.exchange(hivetableUrl, HttpMethod.GET, httpEntity, String.class);

        if (dbResponse == null || tableResponse == null){
            logger.info("访问Atlas出错！");
            return "error";
        }
        String dbResult = dbResponse.getBody();
        String tableResult = tableResponse.getBody();

        JSONObject dbJson = JSON.parseObject(dbResult);
        String dbEntities = dbJson.getString("entities");
        JSONObject tableJson= JSON.parseObject(tableResult);
        String tableEntities = tableJson.getString("entities");

        JSONArray dbEntitiesArr = JSON.parseArray(dbEntities);
        int dbCount = dbEntitiesArr.size();
        System.out.println("数据库个数：" + dbCount);
        String entityUrl = "http://10.62.228.14:21000/api/atlas/v2/entity/guid/";
        for (int i = 0; i < dbCount; i++){
            String dbGuid = dbEntitiesArr.getJSONObject(i).getString("guid");
            String dbGuidUrl = entityUrl + dbGuid;
            ResponseEntity<String> dbGuidResponse = restTemplate.exchange(dbGuidUrl, HttpMethod.GET, httpEntity, String.class);
            String dbGuidResult = dbGuidResponse.getBody();
            JSONObject dbGuidJson = JSON.parseObject(dbGuidResult);
            String dbGuidEntity = dbGuidJson.getString("entity");
            JSONObject dbEntityJson = JSON.parseObject(dbGuidEntity);
            String dbCreateTime = dbEntityJson.getString("createTime");
            String dbUpdateTime = dbEntityJson.getString("updateTime");
            String dbCreator = dbEntityJson.getString("createdBy");
            JSONObject dbEntityAttributes = dbEntityJson.getJSONObject("attributes");
            String dbName = dbEntityAttributes.getString("name");
            String dbOwner = dbEntityAttributes.getString("owner");
            String dbLocation = dbEntityAttributes.getString("location");
            System.out.println("数据库名：" + dbName + "，创建者：" + dbCreator + "，拥有者：" + dbOwner + "，位置：" + dbLocation +
                    "，创建时间：" + stampToDate(dbCreateTime) + "，最近更新时间：" + stampToDate(dbUpdateTime));
        }

        JSONArray tableEntitiesArr = JSON.parseArray(tableEntities);
        if(tableEntitiesArr != null) {
            int tableCount = tableEntitiesArr.size();
            System.out.println("表总数：" + tableCount);
            for (int i = 0; i < tableCount; i++) {
                String tableGuid = tableEntitiesArr.getJSONObject(i).getString("guid");
                String tableGuidUrl = entityUrl + tableGuid;
                ResponseEntity<String> tableGuidResponse = restTemplate.exchange(tableGuidUrl, HttpMethod.GET, httpEntity, String.class);
                String tableGuidResult = tableGuidResponse.getBody();
                JSONObject tableGuidJson = JSON.parseObject(tableGuidResult);
                String tableGuidEntity = tableGuidJson.getString("entity");
                JSONObject tableEntityJson = JSON.parseObject(tableGuidEntity);
                String tableCreateTime = tableEntityJson.getString("createTime");
                String tableUpdateTime = tableEntityJson.getString("updateTime");
                String tableCreator = tableEntityJson.getString("createdBy");

                JSONObject tableEntityAttributes = tableEntityJson.getJSONObject("attributes");
                String tableName = tableEntityAttributes.getString("name");
                String tableOwner = tableEntityAttributes.getString("owner");
                String tableType = tableEntityAttributes.getString("tableType");
                String temporary = tableEntityAttributes.getString("temporary");
                String dbName = tableEntityAttributes.getString("qualifiedName").split("\\.")[0];

                //字段信息
                JSONArray columnsArr = JSON.parseArray(tableEntityAttributes.getString("columns"));
                int columnCount = 0;
                LinkedHashMap<String, String> columnMap = new LinkedHashMap<>();
                if (columnsArr != null) {
                    columnCount = columnsArr.size();
                    for (int j = 0; j < columnCount; j++) {
                        String columGuid = columnsArr.getJSONObject(j).getString("guid");
                        String columGuidUrl = entityUrl + columGuid;
                        ResponseEntity<String> columGuidResponse = restTemplate.exchange(columGuidUrl, HttpMethod.GET, httpEntity, String.class);
                        String columGuidResult = columGuidResponse.getBody();
                        JSONObject columGuidJson = JSON.parseObject(columGuidResult);
                        String columnEntity = columGuidJson.getString("entity");
                        JSONObject columnEntityJson = JSON.parseObject(columnEntity);
                        String columnAttributes = columnEntityJson.getString("attributes");
                        JSONObject columnAttrJson = JSON.parseObject(columnAttributes);
                        String columnName = columnAttrJson.getString("name");
                        String columnType = columnAttrJson.getString("type");
                        columnMap.put(columnName, columnType);
//                System.out.println("字段名：" + columnName + "，字段类型：" + columnType);
                    }
                }

                //存储信息
                JSONObject sdJson = JSON.parseObject(tableEntityAttributes.getString("sd"));
                String sdGuid = sdJson.getString("guid");
                String sdGuidUrl = entityUrl + sdGuid;
                ResponseEntity<String> sdGuidResponse = restTemplate.exchange(sdGuidUrl, HttpMethod.GET, httpEntity, String.class);
                String sdGuidResult = sdGuidResponse.getBody();
                JSONObject sdGuidJson = JSON.parseObject(sdGuidResult);
                String sdEntity = sdGuidJson.getString("entity");
                JSONObject sdEntityJson = JSON.parseObject(sdEntity);
                String sdAttributes = sdEntityJson.getString("attributes");
                JSONObject sdAttrJson = JSON.parseObject(sdAttributes);
                String bucketCols = sdAttrJson.getString("bucketCols");
                String numBuckets = sdAttrJson.getString("numBuckets");
                String tableLocation = sdAttrJson.getString("location");
                String compressed = sdAttrJson.getString("compressed");
                String inputFormat = sdAttrJson.getString("inputFormat");
                String outputFormat = sdAttrJson.getString("outputFormat");

                //分区信息
                JSONArray partitionKeysArr = JSON.parseArray(tableEntityAttributes.getString("partitionKeys"));
                int partitionCount = 0;
                LinkedHashMap<String, String> partitionMap = new LinkedHashMap<>();
                if (partitionKeysArr != null) {
                    partitionCount = partitionKeysArr.size();
                    for (int j = 0; j < partitionCount; j++) {
                        String partitionGuid = partitionKeysArr.getJSONObject(j).getString("guid");
                        String partitionGuidUrl = entityUrl + partitionGuid;
                        ResponseEntity<String> partitionGuidResponse = restTemplate.exchange(partitionGuidUrl, HttpMethod.GET, httpEntity, String.class);
                        String partitionGuidResult = partitionGuidResponse.getBody();
                        JSONObject partitionGuidJson = JSON.parseObject(partitionGuidResult);
                        String partitionEntity = partitionGuidJson.getString("entity");
                        JSONObject partitionEntityJson = JSON.parseObject(partitionEntity);
                        String partitionAttributes = partitionEntityJson.getString("attributes");
                        JSONObject partitionAttrJson = JSON.parseObject(partitionAttributes);
                        String partitionName = partitionAttrJson.getString("name");
                        String partitionType = partitionAttrJson.getString("type");
                        partitionMap.put(partitionName, partitionType);
                    }
                }

                JSONObject tableparaJson = JSON.parseObject(tableEntityAttributes.getString("parameters"));
                String tableTotalSize = tableparaJson.getString("totalSize");
                String tableNumFiles = tableparaJson.getString("numFiles");
                String tableNumRows = tableparaJson.getString("numRows");

                System.out.println("表名：" + tableName + "，创建者：" + tableCreator + "，拥有者：" + tableOwner +
                        "，字段个数：" + columnCount + ",字段信息：" + columnMap.toString() +
                        "，类型：" + tableType + "，是否临时表：" + temporary + "，行数：" + tableNumRows +
                        "，大小：" + tableTotalSize + "，文件数：" + tableNumFiles + "，所属数据库：" + dbName +
                        "，位置：" + tableLocation + "，压缩格式：" + compressed + "，输入格式：" + inputFormat +
                        "，输出格式：" + outputFormat + "，分区个数：" + partitionCount + "，分区信息：" + partitionMap.toString() +
                        "，桶个数：" + numBuckets + "，桶字段：" + bucketCols +
                        "，创建时间：" + stampToDate(tableCreateTime) + "，最近更新时间：" + stampToDate(tableUpdateTime));
            }
        }else{
            System.out.println("hive中没有表");
        }



        return tableResult;
//        return "success";
    }

    /**
     * 时间戳转时间
     * @param s
     * @return
     */
    public String stampToDate(String s){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date(Long.parseLong(s)));
    }

}
