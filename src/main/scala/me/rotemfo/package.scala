package me

import org.apache.spark.sql.types.{StringType, StructField, StructType}

package object rotemfo {
  final val DATABRICKS_CSV = "com.databricks.spark.csv"

  lazy val colNamePostsUserAgent = "user_agent"
  lazy val colNamePostsUserAgentJson = "user_agent_json"
  lazy val colNamePostsUserAgentStruct = "user_agent_struct"
  lazy val colNameEventsDeviceBrand = "device_brand"
  lazy val colNameUserAgentDeviceBrand = "DeviceBrand"
  lazy val colNameEventsDeviceClass = "device_class"
  lazy val colNameUserAgentDeviceClass = "DeviceClass"
  lazy val colNameEventsOsName = "os_name"
  lazy val colNameUserAgentOsName = "OperatingSystemName"
  lazy val colNamEventsOsVersion = "os_version"
  lazy val colNameUserAgentOsVersion = "OperatingSystemVersion"
  lazy val colNameEventsAgentVersion = "agent_version"
  lazy val colNameUserAgentAgentVersion = "AgentVersion"
  lazy val colNameEventsDeviceName = "device_name"
  lazy val colNameUserAgentDeviceName = "DeviceName"
  lazy val colNameEventsAgentClass = "agent_class"
  lazy val colNameUserAgentAgentClass = "AgentClass"
  lazy val colNameEventsAgentName = "agent_name"
  lazy val colNameUserAgentAgentName = "AgentName"

  lazy val userAgentSchema: StructType = StructType(
    Seq(
      StructField(colNameUserAgentAgentClass, StringType, nullable = true),
      StructField("AgentLanguage", StringType, nullable = true),
      StructField("AgentLanguageCode", StringType, nullable = true),
      StructField(colNameUserAgentAgentName, StringType, nullable = true),
      StructField("AgentNameVersion", StringType, nullable = true),
      StructField("AgentNameVersionMajor", StringType, nullable = true),
      StructField("AgentSecurity", StringType, nullable = true),
      StructField(colNameUserAgentAgentVersion, StringType, nullable = true),
      StructField("AgentVersionMajor", StringType, nullable = true),
      StructField(colNameUserAgentDeviceBrand, StringType, nullable = true),
      StructField(colNameUserAgentDeviceClass, StringType, nullable = true),
      StructField("DeviceCpu", StringType, nullable = true),
      StructField(colNameUserAgentDeviceName, StringType, nullable = true),
      StructField("DeviceVersion", StringType, nullable = true),
      StructField("LayoutEngineClass", StringType, nullable = true),
      StructField("LayoutEngineName", StringType, nullable = true),
      StructField("LayoutEngineNameVersion", StringType, nullable = true),
      StructField("LayoutEngineNameVersionMajor", StringType, nullable = true),
      StructField("LayoutEngineVersion", StringType, nullable = true),
      StructField("LayoutEngineVersionMajor", StringType, nullable = true),
      StructField("OperatingSystemClass", StringType, nullable = true),
      StructField(colNameUserAgentOsName, StringType, nullable = true),
      StructField("OperatingSystemNameVersion", StringType, nullable = true),
      StructField("OperatingSystemNameVersionMajor", StringType, nullable = true),
      StructField(colNameUserAgentOsVersion, StringType, nullable = true),
      StructField("OperatingSystemVersionMajor", StringType, nullable = true)
    )
  )
}
