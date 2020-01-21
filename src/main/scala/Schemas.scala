import org.apache.spark.sql.types._

/**
 * project: spark-demo
 * package: 
 * file:    Schemas
 * created: 2019-10-17
 * author:  rotem
 */
object Schemas {
  lazy val colNamePostsMachineCookie = "machine_cookie"
  lazy val colNameEventsMachineIp = "machine_ip"
  lazy val colNamePageViewOther = "other"
  lazy val colNamePostsOtherSaGeo = "sa_geo"
  lazy val colNamePostsCountryGeo = "country_geo"
  lazy val colNamePostsTerritoryGeo = "territory_geo"
  lazy val colNamePostsUsOnly = "us_only"
  lazy val colNamePostsSrcEventsPageKey = "page_key"
  lazy val colNamePostsPageType = "page_type"
  lazy val colNamePostsClientType = "client_type"
  lazy val colNamePostsPxScore = "px_score"
  lazy val colNamePostsReferrer = "referrer"
  lazy val colNamePostsReferrerDomain = "referrer_domain"
  lazy val colNamePostsReferrerKey = "referrer_key"
  lazy val colNamePostsSrcEventsReqTime = "req_time"
  lazy val colNamePostsSessionCookie = "session_cookie"
  lazy val colNamePostsSessionCookieKey = "session_cookie_key"
  lazy val colNamePostsUrl = "url"
  lazy val colNamePostsUrlParams = "url_params"
  lazy val colNameUrlParamsRow = "url_params_row"
  lazy val colNamePostsUrlParamsTrafficSourceParam = "traffic_source_param"
  lazy val colNamePostsUserAgent = "user_agent"
  lazy val colNamePostsUserEmail = "user_email"
  lazy val colNamePostsUserId = "user_id"
  lazy val colNamePostsUserIdKey = "user_id_key"
  lazy val colNamePostsUserVocation = "user_vocation"
  lazy val colNamePostsVersion = "version"
  lazy val colNamePageEventsData = "data"

  lazy val schemaSrcPosts: StructType = StructType(
    StructField(colNamePostsSrcEventsReqTime, TimestampType, nullable = true) :: // will be rename to ts
      StructField(colNamePostsUserId, StringType, nullable = true) ::
      StructField(colNamePostsMachineCookie, StringType, nullable = true) ::
      StructField(colNamePostsSessionCookie, StringType, nullable = true) ::
      StructField(colNamePostsUserAgent, StringType, nullable = true) ::
      StructField(colNamePostsReferrer, StringType, nullable = true) ::
      StructField(colNamePostsUrl, StringType, nullable = true) ::
      StructField(colNamePostsUrlParams, StringType, nullable = true) ::
      StructField(colNameEventsMachineIp, StringType, nullable = true) :: // uses for join
      StructField(colNamePostsSrcEventsPageKey, StringType, nullable = true) :: // uses for join
      StructField(colNamePostsPageType, StringType, nullable = true) ::
      StructField(colNamePostsReferrerKey, StringType, nullable = true) ::
      StructField(colNamePostsPxScore, StringType, nullable = true) ::
      StructField(colNamePageViewOther, StringType, nullable = true) :: // will rename to payload
      Nil)


  lazy val colNameSrcEventsAction = "action"
  lazy val colNameSrcEventsDataTime = "time"
  lazy val colNameSrcEventsDataActive = "active"
  lazy val colNameSrcEventsDataUserId = "user_id"
  lazy val colNameSrcEventsSource = "source"
  lazy val colNameSrcEventsType = "type"

  lazy val schemaSrcEvents: StructType = StructType(
    StructField(colNamePostsSrcEventsReqTime, TimestampType, nullable = true) :: // will be rename to ts
      StructField(colNameSrcEventsSource, StringType, nullable = true) :: // will be uses for event_name
      StructField(colNameSrcEventsType, StringType, nullable = true) :: // will be uses for event_name
      StructField(colNameSrcEventsAction, StringType, nullable = true) :: // will be uses for event_name
      StructField(colNameEventsMachineIp, StringType, nullable = true) :: // will be for join
      StructField(colNamePostsSrcEventsPageKey, StringType, nullable = true) :: // will be for join
      StructField(colNamePageEventsData, StringType, nullable = true) :: // will be rename to payload
      Nil)


  lazy val paegViewSchema: StructType = StructType(
    StructField("ts", TimestampType, nullable = true) ::
      StructField("client_ts", TimestampType, nullable = true) ::
      StructField("event_name", StringType, nullable = true) ::
      StructField("event_type", StringType, nullable = true) ::
      StructField("event_source", StringType, nullable = true) ::
      StructField("event_action", StringType, nullable = true) ::
      StructField("user_id", IntegerType, nullable = true) ::
      StructField("px_score", IntegerType, nullable = true) ::
      StructField("machine_cookie", StringType, nullable = true) ::
      StructField("session_cookie_key", StringType, nullable = true) ::
      StructField("machine_ip", StringType, nullable = true) ::
      StructField("page_key", StringType, nullable = true) ::
      StructField("page_type", StringType, nullable = true) ::
      StructField("client_type", StringType, nullable = true) ::
      StructField("content_id", IntegerType, nullable = true) ::
      StructField("device_class", StringType, nullable = true) ::
      StructField("os_name", StringType, nullable = true) ::
      StructField("agent_version", StringType, nullable = true) ::
      StructField("referrer", StringType, nullable = true) ::
      StructField("referrer_domain", StringType, nullable = true) ::
      StructField("referrer_key", StringType, nullable = true) ::
      StructField("referrer_internal", IntegerType, nullable = true) ::
      StructField("referrer_first_level_internal", StringType, nullable = true) ::
      StructField("referrer_group", StringType, nullable = true) ::
      StructField("referrer_message_id", StringType, nullable = true) ::
      StructField("url", StringType, nullable = true) ::
      StructField("url_first_level", StringType, nullable = true) ::
      StructField("symbol", StringType, nullable = true) ::
      StructField("url_params", StringType, nullable = true) ::
      StructField("email_identifier", StringType, nullable = true) ::
      StructField("product_id", StringType, nullable = true) ::
      StructField("product_id_group", StringType, nullable = true) ::
      StructField("territory_geo", StringType, nullable = true) ::
      StructField("country_geo", StringType, nullable = true) ::
      StructField("traffic_source_param", StringType, nullable = true) ::
      StructField("traffic_source_group", StringType, nullable = true) ::
      StructField("utm_source", StringType, nullable = true) ::
      StructField("utm_medium", StringType, nullable = true) ::
      StructField("utm_campaign", StringType, nullable = true) ::
      StructField("utm_term", StringType, nullable = true) ::
      StructField("utm_content", StringType, nullable = true) ::
      StructField("is_non_refresh", IntegerType, nullable = true) ::
      StructField("data", StringType, nullable = true) ::
      StructField("data_full", StringType, nullable = true) ::
      StructField("data_calc", StringType, nullable = true) ::
      StructField("other", StringType, nullable = true) ::
      StructField("other_calc", StringType, nullable = true) :: Nil
  )
}
