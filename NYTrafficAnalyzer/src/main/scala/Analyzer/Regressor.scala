import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.io.Serializable
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.feature.VectorIndexer

class Regressor(val sqlContext:SQLContext,val sc:SparkContext){
	def trainAndTest(df:org.apache.spark.sql.DataFrame)={
		import sqlContext._
		import sqlContext.implicits._
		var lr = new LinearRegression()
					 .setMaxIter(2000)
					 .setElasticNetParam(0.8)
		var assembler=new VectorAssembler()
					  .setInputCols(Array("Speed"))
					  .setOutputCol("features")
		var train = assembler.transform(df)
		println("training the model...........................")
		var lrModel = lr.fit(train)
		val test = 0.0 to 50.0 by 0.5
		val dfTest = test.toDF()
		val testDF = dfTest.withColumnRenamed("value","Speed")
		println("Testing started...............................")
		var testing = assembler.transform(testDF)
		val predictions = lrModel.transform(testing).drop("features").sort($"Speed")
		println("Results saved..................................")
		predictions.write.saveAsTable("sg6148.AnalysisML_Linear")
		//Coefficients: [-0.014994449315546594] Intercept: 16.017121331109944
	}
	def trainAndTestForest(df:org.apache.spark.sql.DataFrame)={
		import sqlContext._
		import sqlContext.implicits._
		var assembler=new VectorAssembler()
					  .setInputCols(Array("Speed"))
					  .setOutputCol("features")
		var train = assembler.transform(df)
		val rf = new RandomForestRegressor().setLabelCol("label").setFeaturesCol("features")
		println("training the model...........................")
		val model = rf.fit(train)
		println("testing the model...........................")
		val predictions = model.transform(train)
		val result=predictions.drop("features").drop("label").withColumnRenamed("prediction","Tip")
		result.write.saveAsTable("sg6148.AnalysisML_Forest")

	}

}