package com.percent.function;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.percent.beans.OrderBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author yunpeng.gu
 * @date 2021/6/18 15:46
 * @Email:yunpeng.gu@percent.cn
 */
public class AsyncHttpQueryFunction extends RichAsyncFunction<String, OrderBean> {

    private static final String key = "352005a2d4f7e4d8884c3e9addca408d";
    CloseableHttpAsyncClient httpClient = null;
    private static String geocodeUrl = "https://restapi.amap.com/v3/geocode/regeo?&location={},{}&key={}";

    @Override
    public void open(Configuration parameters) throws Exception {
        // 创建一个异步的HttpClient连接池
        // 初始化异步的HttpClien
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(3000)
                .setConnectTimeout(3000)
                .build();

        httpClient = HttpAsyncClients.custom()
                .setMaxConnTotal(20)
                .setDefaultRequestConfig(requestConfig)
                .build();
        httpClient.start();
    }

    @Override
    public void asyncInvoke(String line, ResultFuture<OrderBean> resultFuture) throws Exception {
        try {
            //1. 解析JSON
            OrderBean orderBean = JSONUtil.toBean(line, OrderBean.class);
            double longitude = orderBean.getLongitude();
            double latitude = orderBean.getLatitude();

            HttpGet httpGet = new HttpGet(StrUtil.format(geocodeUrl, longitude, latitude, key));

            //2. 提交httpClient异步请求
            Future<HttpResponse> future = httpClient.execute(httpGet, null);

            //3. 从成功的Future中取数据
            CompletableFuture<OrderBean> orderBeanAndPCD = CompletableFuture.supplyAsync(new Supplier<OrderBean>() {
                @Override
                public OrderBean get() {
                    try {
                        HttpResponse response = future.get();

                        String province = null;
                        String city = null;
                        String district = null;

                        if (response.getStatusLine().getStatusCode() == 200) {
                            // 拿出响应的实例对象
                            HttpEntity entity = response.getEntity();
                            // 将对象toString
                            String result = EntityUtils.toString(entity);
                            JSONObject jsonObject = JSONUtil.parseObj(result);
                            JSONObject regeocode = jsonObject.getJSONObject("regeocode");
                            if (regeocode != null && !regeocode.isEmpty()) {
                                JSONObject address = regeocode.getJSONObject("addressComponent");
                                district = address.getStr("district");
                                city = address.getStr("city");
                                province = address.getStr("province");
                            }
                        }
                        orderBean.setProvince(province);
                        orderBean.setProvince(city);
                        orderBean.setDistrict(district);

                        return orderBean;
                    } catch (Exception e) {
                        return null;
                    }
                }
            });
            orderBeanAndPCD.thenAccept(new Consumer<OrderBean>() {
                @Override
                public void accept(OrderBean resultOrderBean) {
                    // complete()里面需要的是Collection集合，
                    // 但是一次执行只返回一个结果
                    // 所以集合使用singleton单例模式，集合中只装一个对象
                    resultFuture.complete(Collections.singleton(resultOrderBean));
                }
            });
        }catch (Exception e){
            resultFuture.complete(Collections.singleton(null));
        }
    }

    @Override
    public void close() throws Exception {
        httpClient.close();
    }

}
