package com.esl.top10;

import com.alibaba.fastjson.JSONObject;
import com.esl.hbase.HbaseConnect;
import com.esl.hbase.HbaseUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;

public class CategoryTop10 {
    public static void main(String[] args) {
        // 配置信息
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("top10_category");
        JavaSparkContext context = new JavaSparkContext(sparkConf);

        // 获取业务数据：读取
        JavaRDD<String> textFile = context.textFile(args[0]);

        // 转换数据
        JavaPairRDD<Tuple2<String, String>, Integer> transformRDD =
                textFile.mapToPair((PairFunction<String, Tuple2<String, String>, Integer>) s -> {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String category_id = jsonObject.getString("category_id");
                    String event_type = jsonObject.getString("event_type");
                    return new Tuple2<>(new Tuple2<>(category_id, event_type), 1);
                });

        // 统计品类行为的操作：聚合
        JavaPairRDD<Tuple2<String, String>, Integer> aggRDD =
                transformRDD.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);

        // 过滤
        JavaPairRDD<String, Integer> getViewCategoryRDD =   // 过滤查看次数
                aggRDD.filter((Function<Tuple2<Tuple2<String, String>, Integer>, Boolean>) tuple2IntegerTuple2 -> {
                    String action = tuple2IntegerTuple2._1._2;
                    return action.equals("view");
                }).mapToPair((PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Integer>) tuple2IntegerTuple2 ->
                        new Tuple2<>(tuple2IntegerTuple2._1._2, tuple2IntegerTuple2._2));
        JavaPairRDD<String, Integer> getCartCategoryRDD =   // 过滤加购次数
                aggRDD.filter((Function<Tuple2<Tuple2<String, String>, Integer>, Boolean>) tuple2IntegerTuple2 -> {
                    String action = tuple2IntegerTuple2._1._2;
                    return action.equals("cart");
                }).mapToPair((PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Integer>) tuple2IntegerTuple2 ->
                        new Tuple2<>(tuple2IntegerTuple2._1._2, tuple2IntegerTuple2._2));
        JavaPairRDD<String, Integer> getPurchaseCategoryRDD =
                aggRDD.filter((Function<Tuple2<Tuple2<String, String>, Integer>, Boolean>) tuple2IntegerTuple2 -> {
                    String action = tuple2IntegerTuple2._1._2;
                    return action.equals("purchase");
                }).mapToPair((PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Integer>) tuple2IntegerTuple2 ->
                        new Tuple2<>(tuple2IntegerTuple2._1._2, tuple2IntegerTuple2._2));

        // 合并
        JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> tmpJoinCategoryRDD =
                getViewCategoryRDD.leftOuterJoin(getCartCategoryRDD);
        JavaPairRDD<String, Tuple2<Tuple2<Integer, Optional<Integer>>, Optional<Integer>>> joinCategoryRDD =
                tmpJoinCategoryRDD.leftOuterJoin(getPurchaseCategoryRDD);

        // 转换
        JavaPairRDD<CategorySortKey, String> transCategoryRDD =
                joinCategoryRDD.mapToPair((PairFunction<Tuple2<String, Tuple2<Tuple2<Integer, Optional<Integer>>, Optional<Integer>>>, CategorySortKey, String>) stringTuple2Tuple2 -> {
                    String category_id = stringTuple2Tuple2._1;
                    // 获取查看次数
                    int viewCount = stringTuple2Tuple2._2._1._1;
                    int cartCount = 0;
                    int purchaseCount = 0;
                    // 判断类目被加入的次数是否为空
                    if (stringTuple2Tuple2._2._1._2.isPresent())
                        cartCount = stringTuple2Tuple2._2._1._2.get();
                    if (stringTuple2Tuple2._2._2.isPresent())
                        purchaseCount = stringTuple2Tuple2._2._2.get();
                    CategorySortKey categorySortKey = new CategorySortKey(viewCount, cartCount, purchaseCount);
                    return new Tuple2<>(categorySortKey, category_id);
                });
                    // 排序
                    JavaPairRDD<CategorySortKey, String> sortKeyStringJavaPairRDD = transCategoryRDD.sortByKey(false);
                    List<Tuple2<CategorySortKey, String>> top10CategoryList = sortKeyStringJavaPairRDD.take(10);
                    try {
                        top10ToHbase(top10CategoryList);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    // 关闭Hbase数据库的连接
                    HbaseConnect.closeConnection();
                    // 关闭JavaSparkContext连接
                    context.close();
                }
        public static void top10ToHbase (List < Tuple2 < CategorySortKey, String >> top10CategoryList) throws
        IOException {
            // 创建数据表top10和列族top10
            HbaseUtils.createTable("top10", "top10_category");
            // 创建数组columnNames，用于储存列名
            String[] columnNames = {"category_id", "viewCount", "cartCount", "purchaseCount"};
            String category_id;
            String viewCount;
            String cartCount;
            String purchaseCount;
            int count = 0;
            // 遍历集合
            for (Tuple2<CategorySortKey, String> top10 : top10CategoryList) {
                count++;
                // 获取查看次数
                viewCount = String.valueOf(top10._1.getViewCount());
                // 获取加入购物车次数
                cartCount = String.valueOf(top10._1.getCartCount());
                // 获取购买次数
                purchaseCount = String.valueOf(top10._1.getPurchaseCount());
                // 获取品类次数
                category_id = top10._2;
                String[] value = {category_id, viewCount, cartCount, purchaseCount, category_id};
                HbaseUtils.putsToHbase("top10", "row_key_top" + count, "top10_category", columnNames, value);
            }
        }

    }
