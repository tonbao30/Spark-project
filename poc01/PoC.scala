// Title: Proof of concept 1
// Description: This file will be use to demonstrate the streaming kmeans clustering algorithm

// STEP 1 - IMPORT  APIS

import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming._

// STEP 2 - CREATES STREAMING CONTEXT
// set time interval = 3 seconds
val ssc = new StreamingContext(sc, Seconds(3))


// STEP 3 - READ THE STREAMING DATA FROM DEFINED FOLDERS

	//3.1-Read Training data
	// The 2-dimensional training data is in the form "x1,x2" in the text file for each line represents one data point
	//First we need to split "x1,x2" to the double datatype
	// Then we use the Vector function of the linear algebra library to convert it to mllib vectors for training. The Vectors are in the form [x1,x2]
	// Finally, we cache this type of data into memory.

val trainingData = ssc.textFileStream("/home/user/Desktop/poc/kmean_train").map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

	//3.2 - Read Testing data
	// The 2-dimensional testing data is in the form "y,x1 x2" in the text file for each line represents one data point of the testing dataset
	// where y represent the label of the data point
	//First we need to split the data in two parts : Label : y (part(0)) and Vector : x1,x2 (part(1))
	// Then we use the LabeledPoint fuction of the regression library of mllib to convert the part(0) to a mllib labels for testing.
	// Then we use the Vectors function of the linear algebra library of mllib to convert the part(1) to mllib vectors for testing. The Vectors are in the form [x1,x2].
	// Finally, we cache this type of data into memory.

val testData = ssc.textFileStream("/home/user/Desktop/poc/kmean_test").map { line =>
	val parts = line.split(',')
	LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble))) 
}.cache()

// STEP 4 - Set K-means clustering parameters
// - Number of cluster - K :3
// - Decay factor : 0.5
// - Data dimension : 2
// - Center weight : 100.0
val model = new StreamingKMeans().setK(3).setDecayFactor(0.5).setRandomCenters(2,100.00)

// STEP 5 - SET THE MODEL TO TRAIN & TEST ON THE STREAMING  DATA

	//5.1. Set the pre-defince model of the step 4 to be trained on the streaming trainining data of the traininig folder within the time interval of interval 3 seconds
model.trainOn(trainingData)

	//5.2. set the trained model of the training streaming data to predict the value of streaming test data of streaming folder within the time interval of 3 seconds
model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print(20)


//STEP 6-START THE STREAMMING 
ssc.start()
ssc.awaitTermination()

