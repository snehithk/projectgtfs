package ca.mcit.scala.project

import java.io.OutputStreamWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import com.opencsv._


class DataWriter(enrichedList: List[EnrichedTrip]) {
  val conf = new Configuration()
  conf.addResource(new Path ("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  conf.addResource(new Path ("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
  val fs: FileSystem= FileSystem.get(conf)

  val path = "/user/fall2019/snehith/course3/output.csv"
    val out = new OutputStreamWriter(fs.create(new Path(path), true))
    val writer: CSVWriter = new CSVWriter(out)
    val csvSchema: Array[String] = Array("Route Id", "Service Id", "Trip Id", "Trip Head Sign", "Direction Id",
      "Shape Id", "Wheelchair accessible", "Note_FR", "Note En", "Agency Id",
      "Route Short Name", "Route Long Name", "Route Type", "Route Url", "Route Colour",
      "Monday",
      "Start Date", "End Date")

    def writeData(): Unit = {
      writer.writeNext(csvSchema)
      enrichedList.foreach(w => {
        val data = Array(w.tripRoute.trip.route_id.toString, w.calender.service_id,
          w.tripRoute.trip.trip_id, w.tripRoute.trip.trip_headsign,
          w.tripRoute.trip.direction_id, w.tripRoute.trip.shape_id,
          w.tripRoute.trip.wheelchair_accessible, w.tripRoute.trip.note_fr.toString,
          w.tripRoute.trip.note_en.toString, w.tripRoute.route.agency_id,
          w.tripRoute.route.route_short_name, w.tripRoute.route.route_long_name,
          w.tripRoute.route.route_type, w.tripRoute.route.route_url,
          w.tripRoute.route.route_color, w.calender.monday,
          w.calender.start_date, w.calender.end_date
        )
        writer.writeNext(data)
      })

      writer.close()


    }

}