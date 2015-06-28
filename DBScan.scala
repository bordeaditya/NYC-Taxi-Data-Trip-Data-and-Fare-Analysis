import scala.math
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.util.control.Breaks._

object DBScan {

  var sc = new SparkContext("local", "DB Scan APP", "/path/to/spark-0.9.1-incubating",
  List("target/scala-2.10/simple-project_2.10-1.0.jar"))
  
   var R = 396.313
   var eps = 0.0454
   var minPts = 18
   var clusterNumer = 0
   var clusterpoints = new HashMap[Int,ArrayBuffer[(Double, Double)]]()
   var visited = new ArrayBuffer[(Double,Double)]()
   val logFile = "src/data/tripdata_1.gsd"
 
  val logData = sc.textFile(logFile,4).cache()
  var pickPoints = logData.map(line => (line.split(",")(10).toDouble,line.split(",")(11).toDouble)).toArray
  
  
     
  // Normalized Value    
  def Normalize(p:Double,q:Double): (Double,Double) = { 
       var x = R * p * scala.math.Pi / 180;
       var y = R * q * scala.math.Pi / 180;
       var t =  scala.math.cos(x) * scala.math.cos(y);
       var z =  scala.math.cos(x) * scala.math.sin(y);
       
     (t,z)
  }
     
  // Region Query function to get Neighbor Points - To get all points in proximity
  def RegionQuery(point1:Double, point2:Double) : ArrayBuffer[(Double,Double)] = { 
      
    var neighborPoints = new ArrayBuffer[(Double,Double)]()
    var tuplevalue = (0.0,0.0)
    
    for(i<- 0 to pickPoints.size - 1) {
              
        var line  = Normalize(pickPoints(i)._1, pickPoints(i)._2)
        
        var line2 = Normalize(point1, point2)
        var tempDist = math.sqrt(math.pow((line._1 - line2._1),2) +  math.pow((line._2 - line2._2),2))
        if (tempDist <= eps) {
               tuplevalue = (pickPoints(i)._1, pickPoints(i)._2)
               neighborPoints += tuplevalue
         }
        
       }
       neighborPoints
   }
  
  
  // Expand Cluster - classify each point in the cluster
  def ExpandCluster(p:Double, q:Double, neighborPoints: ArrayBuffer[(Double,Double)]){
    
       var singleClusterPoints = new ArrayBuffer[(Double,Double)]()
       var neighborsOfNeighborPts = new ArrayBuffer[(Double,Double)]()
   
       var currentPoint = (p, q)
       neighborsOfNeighborPts += currentPoint
    
       var bufferNeighBors = neighborPoints
       
       //Add P to the cluster
       singleClusterPoints += currentPoint
       //println("*************************=" + bufferNeighBors.length)
       // every point in neighborhood of (p,q)
       for(i <- 0 to bufferNeighBors.length-1) {
        
           var point = bufferNeighBors(i)
           var pointX = point._1
           var pointY = point._2
           var isClassified = 0
           var pDash = point 
           //println("####################="+visited.size)
           //If point is not visited
           if(!visited.contains(pointX,pointY))
           {
               // Mark P visited
                visited += pDash
                neighborsOfNeighborPts = RegionQuery(pointX, pointY)
               //println(bufferNeighBors.size)
               // If size is greater than minimum number of points
               if(neighborsOfNeighborPts.size >= minPts)
               {
                 bufferNeighBors = bufferNeighBors ++ neighborsOfNeighborPts
               }

                // Verify if p is not a member of any cluster
                for((key,value) <- clusterpoints)
                {
                   var tempClusterPoints = clusterpoints(key)
                   if(tempClusterPoints.contains(pDash))
                   {
                     isClassified = 1
                     break;
                    }
                 }

                //If not classified - Add to the cluster points
                 if(isClassified!=1)
                 {
                   singleClusterPoints += pDash
                 }
             //clusterpoints += (clusterNumer -> singleClusterPoints)
             //println("@@@@@@@@@@@@@@="+singleClusterPoints.size)
           }
        }
       clusterpoints += (clusterNumer -> singleClusterPoints)
       // Increment Cluster Number
       clusterNumer  += 1       
  }

  // Main Function
  def main(args: Array[String]) {
  
      for(i<- 0 to pickPoints.size - 1) {
       //print(pickPoints(i)._2)
        if(!visited.contains((pickPoints(i)._1,pickPoints(i)._2)))
        {    
         var tupleval = (pickPoints(i)._1,pickPoints(i)._2)
         visited += tupleval 
         var neighborPoints = RegionQuery(pickPoints(i)._1, pickPoints(i)._2)
         //println(neighborPoints.size)
         ExpandCluster(pickPoints(i)._1,pickPoints(i)._2,neighborPoints)
        }
    }
    val clusterPoints = sc.parallelize(clusterpoints.toSeq)
    clusterPoints.saveAsTextFile("src/data/DBScan_Cluster")
      
   }
}