package com.test;

import com.alibaba.fastjson.JSON;
import com.elasticsearchstudy.YbmEsApiApplication;
import com.elasticsearchstudy.pojo.User;
import com.sun.org.apache.bcel.internal.generic.NEW;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.time.temporal.ValueRange;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest(classes = YbmEsApiApplication.class)
@RunWith(SpringRunner.class)
class YbmEsApiApplicationTests {

    @Qualifier("restHighLevelClient")
    @Autowired
    RestHighLevelClient client;

    @Test
    void contextLoads() {
    }


    //创建索引的创建 Request
    @Test
    void testCreateIndex() throws IOException {
        //1.创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("es_study");
        //2.执行创建请求 indices 请求后获得响应
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);

        System.out.println(createIndexResponse);
    }

    //测试获取索引
    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("es_study");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //测试删除索引
    @Test
    void deleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("es_study");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

    //测试添加文档
    @Test
    void testAddDocument() throws IOException {
        //创建对象
        User user = new User("YBM", 22);
        //创建请求
        IndexRequest request = new IndexRequest("es_study");
        // 规则 put /es_study/_doc/1
        request.id("1");  //如果不设置id 他就会默认设置进去。
        request.timeout(TimeValue.timeValueSeconds(1)); //如果1分钟都没有响应就结束请求，这个可以自己配置

        //将我们的数据放入请求 json
        request.source(JSON.toJSONString(user), XContentType.JSON);
        //客户端发送请求，获取响应的结果
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        System.out.println(indexResponse.status());//返回状态  有create update等
        System.out.println(indexResponse.toString());

    }

    ///获取文档，判断是否存在 GET /index/doc/1
    @Test
    void testIsExists() throws IOException {

        GetRequest getRequest = new GetRequest("es_study", "1");
        //不获取返回的_source的上下文了
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println("exists = " + exists);
    }

    //获取文档信息
    @Test
    void getDocument() throws IOException {
        GetRequest getRequest = new GetRequest("es_study", "1");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getSourceAsString());//打印文档的内容   这个就等于是建立的User实体类
        System.out.println(getResponse);  //这个就等于获取的是所有ES信息
    }


    //更新文档信息
    @Test
    void updateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("es_study", "1");
        User user = new User("YBM学习es", 22);
        updateRequest.doc(JSON.toJSON(user), XContentType.JSON);

        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(updateResponse.status());
    }

    //删除文档信息
    @Test
    void deleteDocument() throws IOException {

        DeleteRequest deleteRequest = new DeleteRequest("es_study", "1");
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());

    }

    //真实项目中，肯定用到大批量查询

    @Test
    void testBulkRequestAdd() throws IOException {

        BulkRequest bulkRequest = new BulkRequest();

        //创建数据
        ArrayList<User> userList = new ArrayList<>();
        userList.add(new User("java1", 11));
        userList.add(new User("java2", 12));
        userList.add(new User("java3", 13));
        userList.add(new User("java4", 14));
        userList.add(new User("java5", 15));

        for (int i = 0; i < userList.size(); i++) {
            bulkRequest.add(
                    new IndexRequest("es_study")
                            .source(JSON.toJSONString(userList.get(i)), XContentType.JSON)
            );

        }
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.hasFailures());//是否失败,返回false 表示成功


    }

    @Test
    void testBulkRequestDel() throws IOException {

        BulkRequest bulkRequest = new BulkRequest();


        for (int i = 0; i < 5; i++) {
            bulkRequest.add(
                    new DeleteRequest("es_study").id("" + i)
            );

        }
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.hasFailures());//是否失败,返回false 表示成功


    }


    /*
		查询:
		搜索请求：SearchRequest
		条件构造：SearchSourceBuilder
	 */
    @Test
    void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("es_study");
        //构建搜索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //查询条件，我们可以使用QueryBuilders 工具来实现
        //QueryBuilders.termQuery  精确匹配
        //QueryBuilders.matchAllQuery() 匹配所有
        //fuzzyQuery 模糊查询
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "java");
        FuzzyQueryBuilder fuzzyQueryBuilder = QueryBuilders.fuzzyQuery("name", "java");
//        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        sourceBuilder.query(fuzzyQueryBuilder);
        //分页
        sourceBuilder.from(0);
        sourceBuilder.size(10);
        //响应超时
        sourceBuilder.timeout(new TimeValue(60,TimeUnit.SECONDS));

        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        //所有查询的数据都在(searchResponse.getHits())里面 的 getHits()内，是一个list<SearchHit>
        System.out.println(JSON.toJSONString(searchResponse.getHits()));
        System.out.println("=========================================");
        for (SearchHit documentFields : searchResponse.getHits().getHits()) {
            System.out.println(documentFields.getSourceAsMap());
        }

    }



}