        import org.apache.spark.SparkContext
        import org.apache.spark.SparkConf
        
        
        
        object AccBroadVariables {
          
          def main(args :Array[String]){
            
             import org.apache.log4j.Logger
            import org.apache.log4j.Level
            Logger.getLogger("org").setLevel(Level.OFF)  
            
            val conf = new SparkConf().setMaster("local[*]").setAppName("AccBroadVariables") 
            val sc = new SparkContext(conf);
            
            val broadVar = sc.broadcast(1)
            println("broad var ---> "+broadVar.value)
            
            val acc = sc.longAccumulator("myAccu")
            sc.parallelize(Array(1, 2, 3, 4)).foreach(x => acc.add(x))
            println("acc var -----> "+acc.value)    
          
            
            //Exectors 
            def process(){
              //business execution
              //start
                 //  interResult   1 2 3 4
                 //   interResult abc def ert uyt     
                 //   acc.add(insterResult)
              //End
            }
            
            def fin{
             
              //acc.value() 1,2,3,4 
              //acc.value() abc,def,ert,uyt
            }
          val data = Array(1, 2, 3, 4, 5)
        val distData = sc.parallelize(data)
        val distDataD = sc.parallelize(data,2)
        val red = distData.reduce((a, b) => a + b)
        println(red)
        
        
        /**
         *coalesce(numPartitions)	: Decrease the number of partitions in the RDD to numPartitions. 
         * 													Useful for running operations more efficiently after filtering down a large dataset.
        	repartition(numPartitions) :	Reshuffle the data in the RDD randomly to create either more or 
        																fewer partitions and balance it across them. 
        																This always shuffles all data over the network.
         * 
         * */
          }
        }