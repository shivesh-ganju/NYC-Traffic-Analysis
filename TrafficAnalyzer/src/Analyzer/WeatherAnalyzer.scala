package Analyzer

class WeatherAnalyzer {
  def sampleTest(){
    val data = sc.textFile("loudacre/weblog/*")
    data.take(2)
  }
}