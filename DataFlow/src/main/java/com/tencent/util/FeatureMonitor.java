package com.tencent.util;

//import com.tencent.infra.monitor.ExtField;
//import com.tencent.infra.monitor.MonitorClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class FeatureMonitor {

  private static final Logger LOG = LoggerFactory.getLogger(FeatureMonitor.class);
  private static FeatureMonitor instance = null;
//  private MonitorClient monitorClient;
//
//  private List<ExtField> baseDim = new ArrayList<>();
//
//  public FeatureMonitor() {
//    MonitorClient.MonitorClientBuilder builder = MonitorClient.newBuilder();
//    String tdBankBid = "b_teg_gdtjk";
//    String tdBankTid = "index";
//    String tdBankManagerIp = "tl-tdbank-tdmanger.tencent-distribute.com";
////    SYSID = "1058665";
////    INTFID = "1";
//    boolean monitorTDBank = true;
//    boolean monitorLog = false;
//    boolean sumWithSameKey = false;
//    int monitorFlushIntervalMs = 1000;
//    builder.setMonitorBid(tdBankBid).setMonitorTid(tdBankTid).setManagerIp(tdBankManagerIp)
//        .setFlushInterval(monitorFlushIntervalMs, TimeUnit.MILLISECONDS);
//    if (monitorTDBank) {
//      builder.enableUploadTdbank();
//    }
//    if (monitorLog) {
//      builder.enablePrintLog();
//    }
//    if (!sumWithSameKey) {
//      builder.setSumWithSameKey(false);
//    }
//    builder.setBusUpdateIntervalMinutes(1);
//    monitorClient = builder.build();
//  }
//
//  public void monitorForController(String sysId, String intFid, Map<String, String> dataMap) {
//    List<ExtField> dims = new ArrayList<>(baseDim);
//    List<String> keyList = dataMap.keySet().stream()
//        .collect(Collectors.toList());
//    ExtField[] extFields = new ExtField[keyList.size()];
//    for (int i = 0; i < keyList.size(); i++) {
//      extFields[i] = new ExtField(keyList.get(i), dataMap.get(keyList.get(i)));
//    }
//    monitorClient.monitorLatency(sysId, intFid, 1L, extFields);
//  }
}
