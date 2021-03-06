package dataset

import org.apache.spark.sql.SparkSession

object CaseClass 
{
	case class Number(i: Int, english: String, french: String)

	def main(args: Array[String]) 
	{
		val spark =     SparkSession.builder()       
						.appName("Dataset-CaseClass")       
						.master("local[4]")       
						.getOrCreate()
						
		import spark.implicits._
		
		val numbers = Seq(Number(1, "one", "un"), Number(2, "two", "deux"), Number(3, "three", "trois"))
						  
		val numberDS=numbers.toDS()
		
		println("Dataset Types")
		println(numberDS.printSchema())
		
		println("filter dataset where i>1")
		println(numberDS.filter($"i" > 1).show())
		
		println("select the number with English column and display")
		println(numberDS.select("i", "english").show() )
		
		println("select the number with English column and filter for i>1")
		println(numberDS.select("i", "english").filter($"i" > 1).show() )
		
		println("sparkSession dataset")
		val anotherDS=spark.createDataset(numbers)
		println("Spark Dataset Types")
		println(anotherDS.printSchema())
		
		
		
	}
}