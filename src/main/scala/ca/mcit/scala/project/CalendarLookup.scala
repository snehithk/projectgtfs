package ca.mcit.scala.project

class CalendarLookup (calendars: List[Calender]){

  private val lookupTable: Map[String, Calender] =
    calendars.map(calendar => calendar.service_id -> calendar).toMap

  def lookup(serviceId: String): Calender = lookupTable.getOrElse(serviceId, null)
}