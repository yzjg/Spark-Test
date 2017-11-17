import   org.apache.spark.{SparkConf,SparkContext}
/*
*使用Spark  Accumulator完成job的数据量的处理，统计emp表中的NULL出现次数以及正常数据的条数  &   打印正常数据的信息
*/

object  AccumulatorApp{
  def  main(args:Array[String]):Unit{
       val conf=new SparkConf().setMaster("local[2]").setAppName("AccumulatorApplication")
       val sc=new SparkContext(conf)
       val lines=sc.textFile("E:/emp.txt")
       //long类型的累加器值
       val  nullNum=sc.longAccumulator("NullNumber")
       val  normalData=lines.filter(line=>{
       var flag=true
       val splitLines=line.split("\t")
       for(splitLine<-splitLines){
         if("".equals(splitLine)){
            flag=false
            nullNum.add(1)
         }
       }
       flag
       })
       //使用cache方法，将RDD的第一次计算结果进行缓存上，防止后面RDD进行重复计算，导致累加器的值不准确的
       normalData.cache()
       //打印每一条正常数据
       normalData.foreach(println)
       //打印正常数据的条数
       println("Normal Data  number:"+normalData.count())
       //打印emp表中NULL出现的次数
       println("Null:"+nullNum.value)
       
       
       sc.stop()
    
  }
}






































