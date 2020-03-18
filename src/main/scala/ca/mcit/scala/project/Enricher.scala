package ca.mcit.scala.project

import scala.collection.mutable.ListBuffer
import scala.io.{BufferedSource, Source}

object Enricher extends App {

  val tripsource: BufferedSource = Source.
    fromFile("/home/bd-user/Documents/trips.txt")

  val tripList: List[Trip] = tripsource
    .getLines()
    .drop(1)
    .filter(_.length > 9)
    .map(f=>f.split(","))
    .map(p => Trip(p(0).toInt, p(1), p(2), p(3), p(4), p(5), p(6))).toList

   val routesource: BufferedSource = Source.
    fromFile("/home/bd-user/Documents/routes.txt")

    val routeList: List[Route] = routesource
      .getLines()
      .drop(1)
      .map(f => f.split(","))
      .map(p => Route(p(0).toInt, p(1), p(2), p(3), p(4), p(5), p(6)))
      .toList

  val routeLookup = new RouteLookup(routeList)

  val enrichedTripRoute : List[TripRoute] = tripList.map(trip => TripRoute(trip,routeLookup.lookup(trip.route_id)))

  val calendarsource : BufferedSource= Source
    .fromFile("/home/bd-user/Documents/calendar.txt")
  val calenderList: List[Calender] = calendarsource
    .getLines()
    .drop(1)
    .map(line => line.split(",")).map(p => Calender(p(0), p(1), p(2), p(3), p(4), p(5), p(6),p(7),p(8),p(9)))
    .toList

  val calendarLookup = new CalendarLookup(calenderList)
  var enrichedTrip = new ListBuffer[EnrichedTrip]()
  for{
    tripRoute <- enrichedTripRoute
  } yield enrichedTrip += EnrichedTrip(tripRoute,calendarLookup.lookup(tripRoute.trip.service_id))
  //val enrichedTrip : List[EnrichedTrip] = enrichedTripRoute.map(tripRoute => EnrichedTrip(tripRoute,calendarLookup.lookup(tripRoute.trip.service_id) ) )

  try {
    val writer = new DataWriter(enrichedTrip.toList.filter(l => l.calender.monday == "1" && l.tripRoute.trip.trip_headsign.contains("STATION")))
    writer.writeData()
  }
  catch {
    case npe: NullPointerException =>
      println("Result NPE: " + npe)

  }
}
