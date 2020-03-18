package ca.mcit.scala.project

import java.io.{File, FileWriter}
//import com.github.tototoshi.csv.CSVWriter
import com.opencsv._


class DataWriter(enrichedList: List[EnrichedTrip]) {

  val Path = "/home/bd-user/Downloads/Output.csv"
  var file: File = new File(Path)
  val out: FileWriter = new FileWriter(file)
  val writer: CSVWriter = new CSVWriter(out)
  val csvSchema: Array[String] = Array("Route Id", "Service Id", "Trip Id", "Trip Head Sign", "Direction Id",
    "Shape Id", "Wheelchair accessible", "Note_FR", "Note En", "Agency Id",
    "Route Short Name", "Route Long Name", "Route Type", "Route Url", "Route Colour",
    "Monday", //"Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
    "Start Date", "End Date")

  def writeData(): Unit = {
    writer.writeNext(csvSchema)
    enrichedList.foreach(w => {
      val data = Array(w.tripRoute.trip.route_id.toString, w.calender.service_id.toString,
        w.tripRoute.trip.trip_id.toString, w.tripRoute.trip.trip_headsign.toString,
        w.tripRoute.trip.direction_id.toString, w.tripRoute.trip.shape_id.toString,
        w.tripRoute.trip.wheelchair_accessible.toString, w.tripRoute.trip.note_fr.toString,
        w.tripRoute.trip.note_en.toString, w.tripRoute.route.agency_id.toString,
        w.tripRoute.route.route_short_name.toString, w.tripRoute.route.route_long_name.toString,
        w.tripRoute.route.route_type.toString, w.tripRoute.route.route_url.toString,
        w.tripRoute.route.route_color.toString, w.calender.monday.toString,
        //w.calender.tuesday.toString, w.calender.wednesday.toString,
       // w.calender.thursday.toString, w.calender.friday.toString,
        //w.calender.saturday.toString, w.calender.sunday.toString,
        w.calender.start_date.toString, w.calender.end_date.toString
      )
      writer.writeNext(data)
    })

    writer.close()


  }
}