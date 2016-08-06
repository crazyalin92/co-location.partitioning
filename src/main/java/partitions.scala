import java.util

import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.util.GeometricShapeFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.datasyslab.geospark.spatialRDD.PointRDD
import org.wololo.jts2geojson.GeoJSONWriter

/**
  * Created by Alina on 21.06.2016.
  */
object partitions_colocations {

  /*Init Spark Context*/
  def initSparkContext(jars_directory:String):Tuple2[SparkContext,SQLContext] =
  {
    /*  Initialize Spark Context*/
    val conf: SparkConf = new SparkConf()
      .setAppName("gdelt")
      .set("spark.executor.memory", "100g")
      .set("spark.driver.memory", "100g")
      .set("spark.driver.maxResultSize","100g")
      .setMaster("spark://10.114.22.10:7077")

    val sc = new SparkContext(conf)

    sc.addJar(jars_directory+"/commons-csv-1.1.jar")
    sc.addJar(jars_directory+"/spark-csv_2.10-1.4.0.jar")
    sc.addJar(jars_directory+"/univocity-parsers-1.5.1.jar")
    sc.addJar(jars_directory+"/geospark-0.2.jar")
    sc.addJar(jars_directory+"/jts-1.13.jar")
    sc.addJar("C:\\Users\\lws4\\Documents\\Scripts\\Master's Project\\partitioning\\target\\gdelt-1.0-SNAPSHOT.jar")

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    return (sc,sqlContext)

  }

  /*   Save Partitions Grids Boundaries as GeoJSON  */
  def saveAsGeoJSONBoundaries(sc:SparkContext,pointRDD:PointRDD,output:String): Unit = {

    val g = pointRDD.grids.listIterator()
    val writer = new GeoJSONWriter()
    val result: util.ArrayList[String] = new util.ArrayList[String]

    while (g.hasNext) {
      val item = g.next()
      println(item.grid, " ", item.getArea)
      println(item, " ", item.getMinX, item.getMinY, item.getMaxX, item.getMaxY)
      val gsf = new GeometricShapeFactory();
      gsf.setBase(new Coordinate(item.getMinX, item.getMinY))
      gsf.setWidth(item.getMaxX - item.getMinX)
      gsf.setHeight(item.getMaxY - item.getMinY)
      gsf.setNumPoints(4);
      val polygon = gsf.createRectangle()
      val gson = writer.write(polygon).toString + ","
      result.add(gson)

    }
    sc.parallelize(result.toArray()).coalesce(1).saveAsTextFile(output)
  }
/*Get Transactions from Partitions*/
 def  getPartitionTransactions(sqlContext:SQLContext,pointRDD: PointRDD,gdelt_reduced:DataFrame):DataFrame=
  {
    //identify transcations
    val partition_row_RDD = pointRDD
      .gridPointRDD
      .sortByKey
      .rdd
      .map(T =>  Row(T._1.toInt,T._2.getCoordinate.z.toInt))

    val partitions_scheme= StructType(
      Array(StructField("grid_id",IntegerType,false),
        StructField("EventId",IntegerType,false)))

    val partitions=sqlContext.createDataFrame(partition_row_RDD,partitions_scheme)

    val df=gdelt_reduced
      .join(partitions,partitions("EventId") === gdelt_reduced("GLOBALEVENTID"),"inner")


    val partition=df.select("grid_id","GLOBALEVENTID","EventCode","ActionGeo_Lat","ActionGeo_Long")
      .dropDuplicates(Array("GLOBALEVENTID"))
      .sort("grid_id")
    return partition

  }
/*Save Partitions Result*/
 def savePartition(partition:DataFrame,output:String)=
 {
   //save partition result
   partition.rdd
     .map(r=> r(0).toString+";"+r(1).toString+";"+r(2).toString+";"+r(3).toString+";"+r(4).toString)
     .coalesce(1).saveAsTextFile(output)


 }
/*Save Transactions*/
  def saveTransactions(partition:DataFrame,output:String): Unit =
  {
    val transaction=partition.select("grid_id","EventCode").
      rdd.map(t=>(t(0),t(1).toString)).groupByKey().map(t=>t._2.toArray.distinct.mkString(" "))
      .coalesce(1)
      .saveAsTextFile(output)


  }

    def main(args: Array[String]) {

   /*HDFS directory*/
   val HDFS_directory= "hdfs://10.114.22.10:9000/alina/"

   /*Init Spark Context*/
   val (sc,sqlContext)=initSparkContext(HDFS_directory+"/jars/")

   // val input="hdfs://10.114.22.10:9000/alina/gdelt/201410[0-9]*.export.csv";
    val input=HDFS_directory+"/gdelt/2015[0-9]*.export.csv"

   /* Extract Data*/
    val gdelt=new data_extractionGDELT(sqlContext,input)

 /* Extract Specific Country */
    val country="'US'"
    val output=HDFS_directory+"colocation/partitions/reducedGDELT"
    val gdelt_reduced=gdelt.readCountryDataFrame(country,output)

    //gdelt_reduced.write.json(HDFS_directory+"/partitions/"+country+"/reducedGDELT_json")

   /* Create PointRDD */
    val inputLocation: String = HDFS_directory+"/colocation/partitions/reducedGDELT/part-00000"
    val offset: Int = 2
    val splitter: String = "csv"
    val gridType: String = "X-Y"
    val numPartions: Int =200

    var pointRDD: PointRDD = new PointRDD(sc, inputLocation, offset, splitter, gridType, numPartions)

    //save partition boundaries
     val output_boundaries= HDFS_directory+"/colocation/partitions/partitions/partitions_geojson"


      saveAsGeoJSONBoundaries(sc,pointRDD,output_boundaries)

     val partition=getPartitionTransactions(sqlContext,pointRDD,gdelt_reduced)

      //save partition
     savePartition(partition,HDFS_directory+"/partitions/"+country+"/partitions/gridWithEvents")

      //save transactions
    saveTransactions(partition,HDFS_directory+"/colocation/partitions/transactions")

}
}