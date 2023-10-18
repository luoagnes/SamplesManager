package com.tencent.openrc.MSM.DF.util



import java.text.SimpleDateFormat
import java.util.{Calendar, Date, GregorianCalendar}
import TimeGranularity._

object TimeUtil {

  def toMinutes(tg: TimeGranularity): Int = {
    tg match {
      case MINUTE => 1
      case HOUR   => 60
      case DAY    => 60 * 24
      case WEEK   => 7 * 60 * 24
      case _ =>
        throw new RuntimeException(s"${tg} not supported")
    }
  }

  def toMs(tg: TimeGranularity): Long = toMinutes(tg).toLong * 60 * 1000

  private val DAY_OFFSET = 8 * toMs(HOUR) // 8 HOUR

  def isAlignTo(ts: Long, tg: TimeGranularity): Boolean = {
    val tgToMs = toMs(tg)
    tg match {
      case MINUTE | HOUR => ts % tgToMs == 0
      case DAY           => (ts + DAY_OFFSET) % tgToMs == 0
      case _             => throw new RuntimeException(s"${tg} not supported")
    }
  }

  def assertAlignTo(ts: Long, tg: TimeGranularity): Unit = {
    val tgToMs = toMs(tg)
    assert(isAlignTo(ts, tg), s"ts must be multiples of $tgToMs, but is ${ts}")
  }

  val yyyyMMdd: ThreadLocal[SimpleDateFormat] =
    new ThreadLocal[SimpleDateFormat]() {
      override protected def initialValue: SimpleDateFormat = {
        return new SimpleDateFormat("yyyyMMdd")
      }
    }

  val yyyyMMddHH: ThreadLocal[SimpleDateFormat] =
    new ThreadLocal[SimpleDateFormat]() {
      override protected def initialValue: SimpleDateFormat = {
        return new SimpleDateFormat("yyyyMMddHH")
      }
    }

  val yyyyMMddHHmm: ThreadLocal[SimpleDateFormat] =
    new ThreadLocal[SimpleDateFormat]() {
      override protected def initialValue: SimpleDateFormat = {
        return new SimpleDateFormat("yyyyMMddHHmm")
      }
    }

  val HH: ThreadLocal[SimpleDateFormat] =
    new ThreadLocal[SimpleDateFormat]() {
      override protected def initialValue: SimpleDateFormat = {
        return new SimpleDateFormat("HH")
      }
    }

  private def dateFormat(tg: TimeGranularity) = {
    tg match {
      case MINUTE => yyyyMMddHHmm
      case HOUR   => yyyyMMddHH
      case DAY    => yyyyMMdd
      case _ =>
        throw new RuntimeException(s"${tg} not supported")
    }
  }

  def autoChooseTimeGranularity(str: String): TimeGranularity = {
    assert(str.toLong > 0)
    str.length match {
      case 12 => TimeGranularity.MINUTE
      case 10 => TimeGranularity.HOUR
      case 8  => TimeGranularity.DAY
      case _ =>
        throw new RuntimeException(s"$str not supported")
    }
  }

  def tsToStr(ts: Long, tg: TimeGranularity, alignCheck: Boolean = true): String = {
    if (alignCheck) {
      assertAlignTo(ts, tg)
    }
    dateFormat(tg).get().format(new Date(ts))
  }

  def strToTs(str: String, tg: TimeGranularity = null): Long = {
    val format =
      if (tg == null) {
        dateFormat(autoChooseTimeGranularity(str))
      } else {
        dateFormat(tg)
      }
    format.get().parse(str).getTime
  }

  /**
   * 计算回归
   * 如传入 20180115, DAY, 3, 3, true，则返回 20180113 - 20180116，应当当做左开右闭来用，即 [0113, 0116)
   */
  def calRegression(
                     ts: Long,
                     tg: TimeGranularity,
                     fowardRegressionCycle: Int,
                     backwardRegressionCycle: Int,
                     deterministicResult: Boolean,
                     alignCheck: Boolean = true) = {
    calRegressionCommon(ts,
      1,
      tg,
      fowardRegressionCycle,
      backwardRegressionCycle,
      deterministicResult,
      alignCheck)
  }

  def calRegressionCommon(
                           ts: Long,
                           granularityCount: Int,
                           tg: TimeGranularity,
                           fowardRegressionCycle: Int,
                           backwardRegressionCycle: Int,
                           deterministicResult: Boolean,
                           alignCheck: Boolean = true): (Long, Long) = {
    if (alignCheck) {
      assertAlignTo(ts, tg)
    }
    assert(fowardRegressionCycle > 0,
      s"fowardRegressionCycle must > 0, but is ${fowardRegressionCycle}")
    assert(backwardRegressionCycle > 0,
      s"backwardRegressionCycle must > 0, but is ${backwardRegressionCycle}")
    val end = if (deterministicResult) {
      1
    } else {
      backwardRegressionCycle
    }
    (ts - (fowardRegressionCycle - 1) * toMs(tg) * granularityCount,
      ts + end * toMs(tg) * granularityCount)
  }

  def replaceTimeWildcards(str: String, ts: Long): String = {
    var replaced = str

    // 把 raw 中 wild 替换成tg时间粒度的值
    def replaceRaw(raw: String, wild: String, tg: TimeGranularity): String = {
      // 先有偏移后无偏移
      // 有偏移的情况
      val rep1 = (wild + """([+-]\d+)((?:MINUTE|HOUR|DAY)?)""").r.replaceAllIn(
        raw,
        m => {
          val deltaTg = m.group(2) match {
            case "MINUTE" => MINUTE
            case "HOUR"   => HOUR
            case "DAY"    => DAY
            case ""       => tg
          }
          tsToStr(ts + m.group(1).toInt * toMs(deltaTg), tg, alignCheck = false)
        }
      )
      // 无偏移的情况
      val rep2 = rep1.replaceAll(wild, tsToStr(ts, tg, alignCheck = false))
      rep2
    }

    // 支持时间粒度偏移
    // 下面的顺序不能随意更改，分钟 -> 小时 -> 天
    replaced = replaceRaw(replaced, "YYYYMMDDHHFF", MINUTE)
    replaced = replaceRaw(replaced, "YYYYMMDDHHMM", MINUTE)
    replaced = replaceRaw(replaced, "YYYYMMDDHH", HOUR)
    replaced = replaceRaw(replaced, "YYYYMMDD", DAY)

    replaced
  }

  def replaceCurrentTimeWildcards(str: String, ts: Long): String = {
    var replaced = str
    // replace currentDay currentHour currentMinute
    // currentDayShort currentHourShort currentMinuteShort
    replaced = replaced.replace("{currentMinute}", tsToStr(ts, MINUTE, alignCheck = false))
    replaced = replaced.replace("{currentHour}", tsToStr(ts, HOUR, alignCheck = false))
    replaced = replaced.replace("{currentDay}", tsToStr(ts, DAY, alignCheck = false))
    replaced = replaced.replace("{currentMinuteShort}",
      tsToStr(ts, MINUTE, alignCheck = false).substring(10))
    replaced =
      replaced.replace("{currentHourShort}", tsToStr(ts, HOUR, alignCheck = false).substring(8, 10))
    replaced =
      replaced.replace("{currentDayShort}", tsToStr(ts, DAY, alignCheck = false).substring(0, 8))
    replaced
  }

  def strToDate(dataTime: String): Date = {
    new Date(strToTs(dataTime))
  }

  def dateToStr(date: Date, tg: TimeGranularity): String = {
    dateFormat(tg).get().format(date)
  }

  /**
   * 根据当前时间的字符串表示计算一段时间（n年/月/天/小时/分钟）前的时间字符串表示
   * @param dataTime 当前时间
   * @param tg       当前时间的粒度
   * @param compareTg
   * @param count
   * @return
   */
  def getLastDateTime(
                       dataTime: String,
                       tg: TimeGranularity,
                       compareTg: TimeGranularity,
                       count: Int): String = {
    val cal = Calendar.getInstance();
    val date = strToDate(dataTime)
    cal.setTime(date)
    compareTg match {
      case TimeGranularity.MINUTE => cal.add(Calendar.MINUTE, -count)
      case TimeGranularity.HOUR   => cal.add(Calendar.HOUR, -count)
      case TimeGranularity.DAY    => cal.add(Calendar.DAY_OF_MONTH, -count)
      case TimeGranularity.WEEK   => cal.add(Calendar.WEEK_OF_MONTH, -count)
      case TimeGranularity.MONTH  => cal.add(Calendar.MONTH, -count)
      case TimeGranularity.YEAR   => cal.add(Calendar.YEAR, -count)
    }
    val lastDate = cal.getTime()
    dateToStr(lastDate, tg)
  }

  def calTodayCycle(dataTime: String, tg: TimeGranularity): Int = {
    val cal = Calendar.getInstance();
    val date = strToDate(dataTime)
    cal.setTime(date)
    val cycle = tg match {
      case TimeGranularity.HOUR   => cal.get(Calendar.HOUR_OF_DAY)
      case TimeGranularity.MINUTE => cal.get(Calendar.HOUR_OF_DAY) * 60 + cal.get(Calendar.MINUTE)
      case _                      => 0
    }
    cycle
  }

  def getPeriodDay(date: String, int: Int): String = {
    val sdf =new SimpleDateFormat("yyyyMMdd")
    val newtime :Date = sdf.parse(date)
    val calendar = new GregorianCalendar;
    calendar.setTime(newtime)
    calendar.add(Calendar.DAY_OF_MONTH, int)
    val current = calendar.getTime
    val current_str = sdf.format(current)
    current_str
  }
}
