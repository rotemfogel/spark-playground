package me.rotemfo

import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.exception.GeoIp2Exception
import com.maxmind.geoip2.model.CityResponse

import java.io.{File, FileInputStream, IOException, InputStream}

case class GeoData(countryCode: Option[String],
                   countryName: Option[String],
                   region: Option[String],
                   city: Option[String],
                   postalCode: Option[String],
                   continent: Option[String],
                   regionCode: Option[String],
                   timezone: Option[String])

object GeoData {
  def apply(omni: CityResponse): GeoData = new GeoData(
    if (omni.getCountry != null) Option(omni.getCountry.getIsoCode) else None,
    if (omni.getCountry != null) Option(omni.getCountry.getName) else None,
    if (omni.getMostSpecificSubdivision != null) Option(omni.getMostSpecificSubdivision.getName) else None,
    if (omni.getCity != null) Option(omni.getCity.getName) else None,
    if (omni.getPostal != null) Option(omni.getPostal.getCode) else None,
    if (omni.getContinent != null) Option(omni.getContinent.getName) else None,
    if (omni.getMostSpecificSubdivision != null) Option(omni.getMostSpecificSubdivision.getIsoCode) else None,
    if (omni.getLocation.getTimeZone != null) Option(omni.getLocation.getTimeZone) else None
  )
}

class MaxMindWrapper(dbInputStream: InputStream) extends Serializable {
  private val maxmind = new DatabaseReader.Builder(dbInputStream).build

  @throws[GeoIp2Exception]
  @throws[IOException]
  def resolve(ipAddress: String): Option[GeoData] = {
    val maybe = scala.util.Try(GeoData(maxmind.city(java.net.InetAddress.getByAddress(ipAddress.split('.').map(_.toInt.toByte)))))
    if (maybe.isSuccess) Some(maybe.get) else None
  }

  def getCountry(ipAddress: String): Option[String] = {
    resolve(ipAddress) match {
      case Some(v) => v.countryName
      case _ => None
    }
  }
}

object MaxMindWrapper {
  def apply(dbFile: String): MaxMindWrapper = new MaxMindWrapper(new FileInputStream(new File(dbFile)))
}