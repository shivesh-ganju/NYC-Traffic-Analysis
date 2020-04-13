package Gateway
import com.koddi.geocoder.Geocoder

class APIGateway {
  val geo = Geocoder.create
  def test : Unit = {
    val geoWithKey = Geocoder.create("AIzaSyC51vsc71hLQwLGvm-6o_k4zjJb7mtSf_Q")
    val results = geoWithKey.lookup(40.6782,-73.9442)
    //40.6782° N, 73.9442° W
// Lookups can also be done with Component objects
// See com.koddi.geocoder.Component for more examples
    results.foreach(println)
  }
}