// Title: Proof of concept 2
// Description: This file will be used to display 10 shortest distances between two airports.


println("CALCULATE 10 SHORTEST ROUTES")

// STEP 1 - READ DATA and IMPORT API

    // 1.1 Read data to dataframe
val df_1 = spark.read.option("header","false").csv("/home/user/Documents/Datasets/test.csv.bz2")

    //Check schema
df_1.printSchema()

    //1.2 Import APIs

// API support for graph processing
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.MurmurHash
import org.apache.spark.SparkContext._

// SQL context to convert RDD result to diagram and display
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

// STEP 2: CREATE THE GRAPH 
// Assumption: The distance may differ between different flights from a airport to an another aiport. 
// Therfore, we calculate the average distance based on the graph to find the shortest distance.
// Noted that: _c5 is OriginAirportID, _c7 is DestinationAirportID, _c16 is distance between 2 ID
    
        // 2.1 Find all vertices by select all Origin airport ID and Destination airport ID, then flat all values, and select all distinct values from the flat array

val airport = df_1.select($"_c5",$"_c7").flatMap(x => Iterable(x(0).toString, x(1).toString)).distinct()
val default_airport = ("Missing")   // Set "missing" as default airport

        // 2.2 Create a dataframe to store all the edges information including OriginAirportID, DestinationAirportID and Distance

val bwdistance = df_1.select($"_c5",$"_c7", $"_c16" cast "Int")

        // 2.3 Create airport vertices by mapping all airport values with it vertex ID. MurmursHash.stringHash is used to create vertex ID 

val airport_vertices:RDD[(VertexId, String)] = airport.rdd.map(x => (MurmurHash.stringHash(x), x))

        // 2.4 Create flight edge from all information store in bwdistance
            // step 1: For each edge in from bwdistance, map OriginAirportID and DestinationAirportID with DestinationAirportID with vertex ID (using MurmurHash.stringHash), at the same time, Distance will be converted to Int, and each flight is assign with the weight of 1. Finally, we have ((origin_vertex_id, destination_vertex_id), distance, 1)

            // Step 2: Map the result of step 1 to the form ((origin_vertex_id, destination_vertex_id), (distance, 1)), in which (origin_vertex_id, destination_vertex_id) is considered "key". 

            // Step 3: apply reduce by key function on the result of step 2, in which the key is (origin_vertex_id, destination_vertex_id). This function will sum up off departure delay and weight of the same key together. Result of this step has the form ((origin_vertex_id, destination_vertex_id), (total departure delay, total flight))

            // Step 4: Map the result of step 3 to the form (origin_vertex_id, destination_vertex_id, average distance). The average distance is calculated by deviding the total departure delay by the total flight

val flight_edge = bwdistance.rdd.map(x => ((MurmurHash.stringHash(x(0).toString),MurmurHash.stringHash(x(1).toString)), x(2).toString.toInt,1)).map{case((a,b),c,d) => ((a,b),(c,d))}.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)).map(x => Edge(x._1._1, x._1._2,x._2._1.toFloat/x._2._2))

        // 2.5 Create graph from airport_vertices, flight_edge and defalut_airport

val graph = Graph(airport_vertices, flight_edge, default_airport)


//STEP 3: DISPLAYING
// Converts bi-directional edges into uni-directional.
//Rewrites the vertex ids of edges so that srcIds are smaller than dstIds, and merges the duplicated edges.

val graph2 = graph.convertToCanonicalEdges()  

// Display result: Short distance in asceding order
val result = graph2.triplets.sortBy(_.attr, ascending=true).map(triplet => (triplet.srcAttr, triplet.dstAttr, triplet.attr.toString))



// Convert to dataframe and rename the columns
// Limit(10) to display only 10 shortest distance
val shortest10 = result.toDF().withColumnRenamed("_1", "From airport").withColumnRenamed("_2", "To airport").withColumnRenamed("_3", "Average Distance").limit(10).show()