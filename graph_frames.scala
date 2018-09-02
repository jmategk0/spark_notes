//import stuff for tests
import org.apache.spark.sql.SparkSession
val ss = SparkSession.
builder().
master("local").
appName("Spark Example").
config("spark.config.option", "some-value").
enableHiveSupport().
getOrCreate()

import ss.implicits._
import org.apache.spark.sql.functions._
import org.graphframes._
import java.util.Date;

//import data 
Dataset rawDataAirport = session.read().csv("data/airports_new.csv");
Dataset rawDataRoute = session.read().csv("data/routes_new.csv");

//define rdd flds 
JavaRDD airportsRdd = rawDataAirport.javaRDD().map(row -> {
					Airport ap = new Airport();
						ap.setId(row.getString(0));
						ap.setName(row.getString(1));
						ap.setCity(row.getString(2));
						ap.setCountry(row.getString(3));
						ap.setIATA(row.getString(4));
						ap.setICAO(row.getString(5));
						ap.setLatitude(row.getString(6));
						ap.setLongitude(row.getString(7));
						ap.setAltitude(row.getString(8));
						ap.setTimezone(row.getString(9));
						ap.setDST(row.getString(10));
						ap.setTz(row.getString(11));
						ap.setType(row.getString(12));
						ap.setSource(row.getString(13));
					return ap;
				});
JavaRDD routesRdd = rawDataRoute.javaRDD().map(row -> {
				Route r = new Route();
				r.setSrc(String)
				r.setAirline(row.getString(0));
				r.setAirlineID(row.getString(1));
				r.setSourceAirport(row.getString(2));
				r.setSrc(row.getString(3));
				r.setDestinationAirport(row.getString(4));
				r.setDst(row.getString(5));
				r.setCodeshare(row.getString(6));
				r.setStops(row.getString(7));
				r.setEquipment(row.getString(8));
			return r;
});

//cast to datasets 
Dataset airports = session.createDataFrame(airportsRdd.rdd(),Airport.class);
Dataset routes = session.createDataFrame(routesRdd.rdd(), Route.class);

//airports = vertices and routes = edges; make new GraphFrame
GraphFrame gf = new GraphFrame(airports, routes);
gf.vertices().show();

//How many in Seattle? 
System.out.println(gf.vertices().filter("City = Seattle'").count());

//setup sql sql temp tables
airports.createOrReplaceTempView("airports");
Dataset degreesDS = gf.degrees();
degreesDS.createOrReplaceTempView("DEGREES");

// 3577 is the main Seattle airport, return number of flights in/out of seatac (~400)
session.sql("select a.airportName, a.City, a.Country, d.degree from AIRPORTS a, DEGREES d where a.id = d.id and d.id = '3577'" ).show();

//Flight between SEA and MSP by id code (three flights)
session.sql("select a.airlineName, r.src,r.dst from ROUTES r, AIRLINES a where r.src = '3577' and r.dst = '3858' and r.airlineCode = a.airlineId").show();

//Can use triplets as well
gf.triplets().filter("src.id='3858' and dst.id='3577'").show();

//breadth first search from Seattle WA to DLH
Dataset seaToDlhDS = gf.bfs().fromExpr("id = '3577'").toExpr("id = '3598'").maxPathLength(3).run();

seaToDlhDS.createOrReplaceTempView("sea_to_dlh");

//see results for multi-stop flights from Seattle WA to DLH
session.sql("select distinct from.city , v1.city, to.city from sea_to_dlh").show(100);

