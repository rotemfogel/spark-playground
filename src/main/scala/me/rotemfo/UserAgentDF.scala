package me.rotemfo


object UserAgentDF extends App {
  val data = Array(
    "com.seekingalpha.webwrapper/4.29.3(331) (Linux; U; Android Mobile R; en-gb; Pixel 2 XL Build/RPP1.200123.016; google) 1440X2824 Google Pixel 2 XL SAUSER-36815366",
    "com.seekingalpha.webwrapper/4.29.3(331) (Linux; U; Android Mobile R; en-gb; Pixel 3a Build/RP1A.200205.001.A3; google) 1080X2176 Google Pixel 3a SAUSER-50243559",
    "com.seekingalpha.webwrapper/4.29.3(331) (Linux; U; Android Mobile R; en-us; Pixel 4 XL Build/RP1A.200205.001.A3; google) 1440X2984 Google Pixel 4 XL SAUSER-47082566",
    "com.seekingalpha.webwrapper/4.29.3(331) (Linux; U; Android Mobile R; en-us; Pixel 4 XL Build/RPP1.200123.018; google) 1440X2984 Google Pixel 4 XL SAUSER-28821505",
    "Mozilla/5.0 (iPhone SeekingAlphaiPhoneApp; U; iPhone10,6; en-LV) IOS/13.1.2 USERID/50620594 DEVICETOKEN/ab92c80352cf0e629997059b9047459eb66087f5 VER/3.12.11 KIND/portfolio",
    "Mozilla/5.0 (iPhone SeekingAlphaiPhoneApp; U; iPhone10,6; en-LV) IOS/13.3.1 USERID/51135016 DEVICETOKEN/8fa39255a5aec84db519e6781f181f90e2e3febd VER/3.12.11 KIND/portfolio",
    "Mozilla/5.0 (iPhone SeekingAlphaiPhoneApp; U; iPhone10,6; en-MK) IOS/13.3.1 USERID/50421956 DEVICETOKEN/0715287777b79f63e05dad587d6d0d49ea3e0018 VER/3.12.11 KIND/portfolio",
    "Mozilla/5.0 (iPhone SeekingAlphaiPhoneApp; U; iPhone10,6; en-MT) IOS/13.3.1 USERID/48889459 DEVICETOKEN/9f150f2e932dd1ca9eb52929dff0648c266ff0be VER/3.12.11 KIND/portfolio",
    "Mozilla/5.0 (iPhone SeekingAlphaiPhoneApp; U; iPhone10,6; en-MX) IOS/13.3.1 USERID/10149201 DEVICETOKEN/feda43df21019f2f178d33bbca7cae65441b7e01 VER/3.12.11 KIND/portfolio",
    "Mozilla/5.0 (iPhone SeekingAlphaiPhoneApp; U; iPhone10,6; en-MX) IOS/13.3.1 USERID/26242873 DEVICETOKEN/e42dfe3455a0e3d0f9748d64e4f505fd241e6586 VER/3.12.11 KIND/portfolio")


  data.foreach(ua => {
    val parsed = UdfStore.parseUserAgent(ua)
    println(parsed)
    //    println(values.mkString("\n"))
    //    println("")
  })

}
