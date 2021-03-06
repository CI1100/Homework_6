import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession


object Question2 
{
	def main(args: Array[String]) 
	{
	
		
	
		Logger.getLogger("org").setLevel(Level.ERROR)
		val session = SparkSession.builder
								  .master("local[2]")
								  .appName("word_1")
								  .getOrCreate()
								  
		import session.implicits._ 
		
		
		val df = session.read.option("header", true).option("inferSchema", true).csv("./src/main/scala/insurance_csv.csv")
		
		println("Dataset Size:")
		println(df.count() )
		println(df.columns.size)
		
		println("Sex count using group by")
		println(df.groupBy("sex").count().show() )
		
		println("Smokers sex count")
		println(df.filter($"smoker" === true).groupBy("sex").count().orderBy($"count".desc).show() )
		
		println("Sum of Charges by Regions")
		println(df.groupBy("region").sum("charges").orderBy($"sum(charges)".desc).show() )
	}
}