package Gateway
import com.koddi.geocoder.Geocoder

class APIGateway {
  val geo = Geocoder.create
  def test : Unit = {
    val geoWithKey = Geocoder.create("AIzaSyA7iBwz17N_5hySdDSZGSDerO78_b8XUbw")
    val results = geoWithKey.lookup("Upper West Side South Manhattan,NY")

// Lookups can also be done with Component objects
// See com.koddi.geocoder.Component for more examples
    results.foreach(println)
  }
}