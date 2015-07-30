package com.asiainno.base.kafka;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.regex.Pattern;

/**
 * @ClassName: KafkaUtils
 * @author: mingyu.zhao
 * @date: 15/6/26 下午6:09
 */
public class KafkaUtils {
    private static final Pattern IPV4_PATTERN =
            Pattern.compile(
                    "^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$");

    /**
     * 返回当前JVM的pid
     *
     * @return
     */
    public static int getPid() {
        int pid = 0;
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        String name = runtime.getName();
        int index = name.indexOf("@");
        if (index != -1) {
            pid = Integer.parseInt(name.substring(0, index));
        }

        return pid;
    }

    /**
     * 返回当前主机的ip
     *
     * @return
     */
    public static String getHost() {
        String host = "";
        try {
            for (Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces(); interfaces.hasMoreElements(); ) {
                NetworkInterface networkInterface = interfaces.nextElement();
                if (networkInterface.isLoopback() || networkInterface.isVirtual() || !networkInterface.isUp()) {
                    continue;
                }
                Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress inetAddress = addresses.nextElement();
                    host = inetAddress.getHostAddress();
                    if (IPV4_PATTERN.matcher(host).matches()) {
                        break;
                    }
                }
            }
        } catch (Exception e) {

        }
        return host;
    }
}
