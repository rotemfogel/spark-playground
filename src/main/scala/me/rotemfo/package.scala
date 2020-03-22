package me

package object rotemfo {
  final val DATABRICKS_CSV = "com.databricks.spark.csv"

  lazy val colNameUserClass = "agent_class"
  lazy val colNameUserAgent = "user_agent"
  lazy val colNameUserAgentJson = "user_agent_json"
  lazy val colNameUserAgentStruct = "user_agent_struct"
  lazy val colNameDeviceBrand = "device_brand"
  lazy val colNameDeviceClass = "device_class"
  lazy val colNameOsName = "os_name"
  lazy val colNameOsVersion = "os_version"
  lazy val colNameAgentVersion = "agent_version"
  lazy val colNameDeviceName = "device_name"
  lazy val colNameAgentName = "agent_name"
  lazy val colNameAgentClass = "agent_class"

  lazy val colNameUserAgentDeviceBrand = "DeviceBrand"
  lazy val colNameUserAgentDeviceClass = "DeviceClass"
  lazy val colNameUserAgentOsName = "OperatingSystemName"
  lazy val colNameUserAgentOsNameVersion = "OperatingSystemNameVersion"
  lazy val colNameUserAgentOsVersion = "OperatingSystemVersion"
  lazy val colNameUserAgentAgentName = "AgentName"
  lazy val colNameUserAgentAgentVersion = "AgentVersion"
  lazy val colNameUserAgentDeviceName = "DeviceName"
  lazy val colNameUserAgentAgentClass = "AgentClass"
  lazy val colNameUserAgentAgentNameVersion: String = "AgentNameVersion"
}
