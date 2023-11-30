package com.esl.top3;

import com.alibaba.fastjson.JSONObject;
import com.esl.hbase.HbaseConnect;
import com.esl.hbase.HbaseUtils;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AreaTop3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("top3_area_count");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> textFile = context.textFile(args[0]);

        // 获取业务数据
        JavaPairRDD<Tuple2<String, String>, String> transProductRDD = textFile.mapToPair((PairFunction<String, Tuple2<String, String>, String>) s -> {
                // 将数据转化为JSON格式
                JSONObject jsonObject = JSONObject.parseObject(s);
                String address_name = jsonObject.getString("address_name").replace("\u00A0+", "");
                String product_id = jsonObject.getString("product_id");
                String event_type = jsonObject.getString("event_type");
                return new Tuple2<>(new Tuple2<>(address_name, product_id), event_type);

        });

        // 过滤
        JavaPairRDD<Tuple2<String, String>, String> getViewRDD =
                transProductRDD.filter((Function<Tuple2<Tuple2<String, String>, String>, Boolean>) tuple -> tuple._2.equals("view"));

        // 转换
        JavaPairRDD<Tuple2<String, String>, Integer> productByAreaRDD =
                getViewRDD.mapToPair((PairFunction<Tuple2<Tuple2<String, String>, String>, Tuple2<String, String>, Integer>) tuple -> new Tuple2<>(tuple._1, 1));

        // 聚合：统计每个区域的不同商品的查看次数
        JavaPairRDD<Tuple2<String, String>, Integer> tuple2IntegerJavaPairRDD =
                productByAreaRDD.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);
        // 根据不同区域来分组
        JavaPairRDD<String, Tuple2<String, Integer>> transProductCountByAreaRDD =
                tuple2IntegerJavaPairRDD.mapToPair((PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Tuple2<String, Integer>>) tuple ->
                        new Tuple2<>(tuple._1._1, new Tuple2<>(tuple._1._2, tuple._2)));
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupByKey = transProductCountByAreaRDD.groupByKey();

        // 排序
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> productSortByAreaRDD =
                groupByKey.mapToPair((PairFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>, String, Iterable<Tuple2<String, Integer>>>) stringIterableTuple2 -> {
                    List<Tuple2<String, Integer>> tuple2List = new ArrayList<>();
                    for (Tuple2<String, Integer> stringIntegerTuple2 : stringIterableTuple2._2()) {
                        tuple2List.add(stringIntegerTuple2);
                    }
                    tuple2List.sort((o1, o2) -> o2._2 - o1._2);
                    return new Tuple2<>(stringIterableTuple2._1, tuple2List);
                });

        /* 数据持久化
         * 将分析结果数据储存到Hbase数据库中
         * 通过mapToPair()
         * */
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> top3AreaProductRDD =
                productSortByAreaRDD.mapToPair((PairFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>, String, Iterable<Tuple2<String, Integer>>>) stringIterableTuple2 -> {
                    ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
                    Iterator<Tuple2<String, Integer>> iterator = stringIterableTuple2._2().iterator();
                    // 记录top3数量
                    int i = 0;
                    while (iterator.hasNext()) {
                        list.add(iterator.next());
                        i++;
                        if (i == 3)
                            break;
                    }
                    return new Tuple2<>(stringIterableTuple2._1, list);
                });
        // 执行储存到数据库的操作
        try {
            top3ToHbase(top3AreaProductRDD);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 关闭Hbase数据库的连接
        HbaseConnect.closeConnection();
        // 关闭JavaSparkContext连接
        context.close();
    }

    public static void top3ToHbase(JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> rdd) throws IOException {
        // 创建top3以及top3_area_count
        HbaseUtils.createTable("top3", "top3_area_count");
        // 创建columns数组 top3的列名
        String[] columnNames = {"area", "product_id", "viewCount"};
        // 插值 以遍历的形式插值
        rdd.foreach(
                (VoidFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>>) stringIterableTuple2 -> {
                    // 获取区域的数据
                    String area = stringIterableTuple2._1;
                    String product_id;
                    String viewCount;
                    Iterator<Tuple2<String, Integer>> iterator = stringIterableTuple2._2().iterator();
                    // 迭代器转化为集合的形式
                    List<Tuple2<String, Integer>> list = Lists.newArrayList(iterator);
                    // 获取商品id和商品查看次数
                    for (Tuple2<String, Integer> tuple :
                            list) {
                        product_id = tuple._1;
                        viewCount = String.valueOf(tuple._2);
                        // 创建数组value储存数据表top3的值
                        String[] value = {area, product_id, viewCount};
                        // 加入到Hbase表
                        try {
                            HbaseUtils.putsToHbase("top3", area + product_id, "top3_area_count", columnNames, value);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
        );
    }
}
