package flink.demo;

import java.util.Arrays;

public class JVMTools {
    public static String getMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        long availableMemory = maxMemory - usedMemory;
        return String.format(
                "最大内存: %d MB, 已分配: %d MB, 已用: %d MB, 剩余可用: %d MB",
                maxMemory / 1024 / 1024,
                totalMemory / 1024 / 1024,
                usedMemory / 1024 / 1024,
                availableMemory / 1024 / 1024
        );
    }

    public static String getProperty(String[] candidate, String defaultValue) {
        String property = null;
        if (candidate != null) {
            for (String c : candidate) {
                if (c != null && !c.isEmpty()) {
                    property = c;
                    break;
                }
            }
        }
        return property == null ? defaultValue : property;
    }

    public static void main(String[] args) {
//        System.out.println(getMemoryUsage());

        System.out.println(getProperty(new String[]{null, ""}, "default"));

    }
}
