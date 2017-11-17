# Spark-Test
#目录结构说明
spark-Test/Accumulator  案例一代码
spark-Test/BroadCast    案例二代码
spark-Test/MultipleOutput  案例三代码
spark-train/data      测试数据


#案例一：Spark Accumulator的使用
* 需求
  使用Accumulator统计emp表中NULL出现的次数以及正常数据的条数的 & 打印正常数据的信息
  
*  数据
   参照/data/emp1.txt
 * 遇到的坑& 解决方法
      现象描述 &  原因分析
      我们都知道，Spark中的一系列transform操作会构成一串长的任务链的，此时就需要通过一个action操作来触发的，accumulator也是一样的，
      只有当action操作执行时候，才会触发accumulator的执行的，因此在一个actoion操作之前，我们调用accumulator的value方法是无法查看其中的数值的额，
      肯定是没有任何的变化的，所以在对normalData进行foreach操作之后，即action操作之后，我们会发现累加器的数值就变成了11，之后，我们对normalData进       行一次count操作之后，即又一次的action操作之后，其实这时候，又去执行一次前面的transform操作的，因此累加器的值又会增加了11，变成22的
      
      
 解决的办法：
        经过上面的方法，我们可以知道，使用累加器的时候，我们只有使用一次的action操作才能够保证结果的准确性的，因此，我们面对这种情况下，是有办法的
        做法就是切断它们相互之间的依赖关系即可的额，因此对normalData使用cache方法，当使用RDD第一次计算出来时候，就会直接缓存起来的，在调用时候，相         同的计算操作就不会在重新计算一遍的
        
      
      
      
###  案例二：SparkBroadCast的使用
*   需求
使用spark实现mapJoin&  commoinJoin的

*  数据
参照/data/emp1.txt



###  案例三：多目录输出  & 作业重跑
*  需求
1.按照分区信息进行多目录输出，每个分区下输出一个文件
2.在需求1的基础上，在某个分区目录下输出多个文件
3.在需求2的基础上，实现数据的采样，获取造成数据倾斜的key
4.完成作业的重跑  &  对不同的数据进行追加


*   数据
需求1，需求2参照/data/emp1.txt
需求3参照/data/emp3.txt
需求4参照/data/emp1.txt    emp2.txt


*  核心思路

1.实现需求的核心在于继承MultipleTextOutputFormat类的，并重写generateFileNameForKeyValue与generateActualKey 
generateFileNameForKeyValue保证了按照分区信息进行多目录输出的
generateActualKey保证了不将分区信息写入到文件中的
2.实现需求2的核心在于巧妙的使用union算子的
3.实现需求3的核心在于采样的，spark为我们提供了算子sample的
4.实现需求4的核心在于使用hadoop  api中的rename方法，实现对新文件进行追加的额，老文件的删除的，即是重跑机制的，
需求4核心该案例核心，具体思路见代码中注释的














































