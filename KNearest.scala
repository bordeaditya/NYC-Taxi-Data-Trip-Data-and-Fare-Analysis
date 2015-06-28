import scala.math
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.util.control.Breaks._

object KNearest {

    val x1 = -73.945358
    val y1 = 40.751053
    val x2 = -73.945
    val y2 = 40.791058
    
    val K = 11
    
    val logFileTripData = "src/data/tripdata_2.gsd"
    val logFileTripFare = "src/data/tripfare_2.gsd"
    
    val testFileTripData = "src/data/tripdata_1.gsd"
    val testFileTripFare = "src/data/tripfare_1.gsd"
    
    val sc = new SparkContext("local", "DB Scan APP", "/path/to/spark-0.9.1-incubating",
    List("target/scala-2.10/simple-project_2.10-1.0.jar"))
    
    val tripData = sc.textFile(logFileTripData).map(line=>line.split(",")).map(line => (line(5),line(10)+","+line(11)+","+line(12)+","+line(13)))
    val tripFare = sc.textFile(logFileTripFare).map(line=>line.split(",")).map(line=> (line(3),line(8).toDouble))

    // Join Result : from Train
    val joinedResult = tripData.join(tripFare)
    
    
    // Region Query function to get Neighbor Points - To get all points in proximity
    def Similarity(p:String,testPoint:String):Double= { 
      
      // similarity value
      var similarity = 0.0
      
      var splittedTestString = testPoint.split(",")
      var x1 = splittedTestString(0).toDouble
      var y1 = splittedTestString(1).toDouble
      var x2 = splittedTestString(2).toDouble
      var y2 = splittedTestString(3).toDouble
      
      var splittedString = p.split(",")
      var x11 = splittedString(0).toDouble
      var y11 = splittedString(1).toDouble
      var x22 = splittedString(2).toDouble
      var y22 = splittedString(3).toDouble
      
      var d1 = math.sqrt(math.pow(x11, 2) + math.pow(x1, 2))
      var d2 = math.sqrt(math.pow(y11, 2) + math.pow(y1, 2))
      var d3 = math.sqrt(math.pow(x22, 2) + math.pow(x2, 2))
      var d4 = math.sqrt(math.pow(y22, 2) + math.pow(y2, 2))
      
      var dotProd = (x11*x1) + (x22 * x2) + (y11 * y1) + (y22 * y2)
      
      similarity = (dotProd * 1000)/(d1 * d2 * d3 * d4)
      
      similarity
   }
  
   // Prediction On Test Data
   def Prediction(testPoint:String):String= {
   
    var similarityResult = joinedResult.map(res=>(Similarity(res._2._1,testPoint),(res._2._2.toDouble,1.0))).filter(line=>(line._1 != 0.0))
    
    var sortedSimilarityresult = similarityResult.sortByKey(false,1)
    //var sortedSimilarityresult = similarityResult.sortBy(x=> x._1,false)
    
    var topSimilar = sortedSimilarityresult.take(K)
    
    var maxValue =  Double.NegativeInfinity
    var avg = 0.0
    for(i <- 0 to K-1)
    {
      var currentRecord = topSimilar(i)
      var tipValue = currentRecord._2._1
      avg += currentRecord._2._1.toDouble
      if(maxValue < tipValue)
      {
        maxValue = tipValue
      }
    }
    avg = avg / K

    sortedSimilarityresult.saveAsTextFile("src/data/SimilarityResult")
    maxValue.toString +"::"+ avg.toString
    
   }
 
  def main(args: Array[String]) 
  {
    var predictedData = new ArrayBuffer[String]
    val testTripData = sc.textFile(testFileTripData).map(line=>line.split(",")).map(line => (line(5),line(10)+","+line(11)+","+line(12)+","+line(13)))
    val testTripFare = sc.textFile(testFileTripFare).map(line=>line.split(",")).map(line=> (line(3),line(8).toDouble))
    
    // test Data 
    val joinedTestData = testTripData.join(testTripFare).toArray
    
    // Iterate over each test data
    // key -> Original tip  Value-> Prediction = max::average
    //val str = "-73.945358" +","+"40.751053"+","+"-73.945"+","+"40.791058"
    
    // Prediction for Every Data point
    for(i <- 0 to joinedTestData.length -1)
    {
      var tempTestData = joinedTestData(i)
      predictedData +=  tempTestData._2._2 + "#" + Prediction(tempTestData._2._1)
    }
    
    //predictedData.take(5).foreach(println)
    val predictedDataRDD = sc.parallelize(predictedData.toSeq)
    predictedDataRDD.saveAsTextFile("src/data/KNEAREST_RESULT")
    
    var meanError = 0.0
    var sqrError = 0.0
    // Mean Absolute Error And Root Mean Squared Error
    for(i <-0 to predictedData.length-1)
    {
      var actualTipValue = predictedData(i).split("#")(0).toDouble
      var temp = predictedData(i).split("#")(1)
      var avgSimilarityTip = temp.split("::")(0).toDouble
      var error = math.abs(actualTipValue - avgSimilarityTip)
      
      meanError = meanError + error
      sqrError = sqrError + math.pow(error, 2)
    }
    
    // Mean Absolute Error
    meanError = meanError / predictedData.length
    sqrError = math.sqrt(sqrError / predictedData.length)
    
    
    println("****Model Evaluation****")
    println("1) Mean Absolute Error = " + meanError.toString)
    println("2) Root Mean Squared Error = " + sqrError.toString)
    
    //val similarityRDD = joinedTestData.map(line=> (line._2._2,Prediction(line._2._1)))
    
    //similarityRDD.saveAsTextFile("src/data/KNEAREST_RESULT")
  
    //similarityRDD.saveAsTextFile("src/data/KNEAREST_RESULT")
    
  }
}
