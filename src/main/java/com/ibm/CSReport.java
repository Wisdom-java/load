package com.ibm;

import com.ibm.util.ProducerThread;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.zip.GZIPInputStream;

public class CSReport {
    static String topic = "helloworld";

    static Properties properties = new Properties();

    static KafkaProducer<String, String> producer = null;

    // 核心池大小
    static int corePoolSize = 10;
    // 最大值
    static int maximumPoolSize = 20;
    // 无任务时存活时间
    static long keepAliveTime = 60;
    // 时间单位
    static TimeUnit timeUnit = TimeUnit.SECONDS;

    // 阻塞队列
    static BlockingQueue blockingQueue = new LinkedBlockingQueue();
    // 线程池
    static ExecutorService service = null;
    static {
        // 配置项
        //properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.20.140.3:9092," +
                "172.20.140.4:9092," +
                "172.20.140.5:9092," +
                "172.20.140.12:9092," +
                "172.20.140.13:9092," +
                "172.20.140.54:9092," +
                "172.20.140.62:9092," +
                "172.20.140.63:9092," +
                "172.20.140.72:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.ACKS_CONFIG,"0");
        producer = new KafkaProducer<>(properties);
        // 初始化线程池
        service = new ThreadPoolExecutor(corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                timeUnit,
                blockingQueue);
    }

    public static void main( String[] args ) {
        System.out.println("开始加载 cs_report");
        //服务器上的地址
        String path1 = "/data/log20210716";
        String path2 = "/var/log/dsnet-core";
        //String path1 = "/Users/wisdom/Desktop/log";
        //String path2 = "/dsnet-core";
        //data/log20210716/172.20.77.10/var/log
        try {
            //服务器上的数据路径
            File file= new File(path1);
            //ip 文件夹
            File[] fileList = file.listFiles();
            for (int i = 0; i < fileList.length; i++) {
                //获取以ip为名称的那一层
                String ipDirName = fileList[i].getName();
                if (!ipDirName.contains("172")){
                    continue;
                }
                // 10 、11、12为ca report
                String ipSubStr = ipDirName.substring(7,9);
                if (ipSubStr.equals("10") || ipSubStr.equals("11") || ipSubStr.equals("12")){
                    continue;
                }
                System.out.println("当前处理文件夹：" + ipDirName);
                String finalName = path1 + "/" + ipDirName + path2;
                //具体文件存放的位置
                File dataFile = new File(finalName);
                File[] files = dataFile.listFiles();
                for (int j = 0; j < files.length; j++) {
                    //具体文件的名称
                    String fileName = files[j].getName();
                    if (!fileName.contains("report")){
                        continue;
                    }
                    System.out.println("filename:"+ fileName);
                    String dateStr = fileName.substring(19, 21);
                    if (!dateStr.equals("15")){
                        continue;
                    }
                    String hourStr = fileName.substring(22, 24);
                    if (!hourStr.equals("09") && !hourStr.equals("10") && !hourStr.equals("11")){
                        continue;
                    }
                    InputStream in = new GZIPInputStream(new FileInputStream(finalName + "/" + fileName));
                    Scanner sc = new Scanner(in);
                    while (sc.hasNextLine()) {
                        String message = sc.nextLine();
                        //设置key 则已key算哈说的方式分区 不设置则是轮询 也可以手动设置分区
                        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,message);
                        service.submit(new ProducerThread(producer,record));
                    }
                }
            }
            System.out.println("程序结束");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
