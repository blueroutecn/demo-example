package com.thirdlucky.storm.example.webcount;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Random;

public class WebCountGenerator {
    public static void main(String[] args) {
        File file = new File("e:/website.log");
        String host = "www.thirdlucky.com";
        String[] session_id = {"ABYH6Y4V4SCVXTG6DPB4VH9U123", "XXYH6YCGFJYERTT834R52FDXV9U34", "BBYH61456FGHHJ7JL89RG5VV9UYU7",
                "CYYH6Y2345GHI899OFG4V9U567", "VVVYH6Y4V4SFXZ56JIPDPB4V678"};
        String[] time = {"2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-07 08:40:52", "2014-01-07 08:40:53",
                "2014-01-07 09:40:49", "2014-01-07 10:40:49", "2014-01-07 11:40:49", "2014-01-07 12:40:49"};

        String[] ips = {"192.168.227.161", "192.168.227.162", "192.168.227.163", "192.168.227.135", "192.168.227.160"
                , "192.168.227.131", "192.168.227.132", "192.168.227.133", "192.168.227.130"};

        Random random = new Random();
        StringBuffer sbBuffer = new StringBuffer();
        for (int i = 1; i <= 30; i++) {
            sbBuffer.append(String.format("%s\t%d:%s\t%s\t%s\n",
                    host,
                    i,
                    session_id[random.nextInt(5)],
                    time[random.nextInt(8)],
                    ips[random.nextInt(9)]
            ));
        }

        byte[] b = (sbBuffer.toString()).getBytes();

        FileOutputStream fs;
        try {
            fs = new FileOutputStream(file);
            fs.write(b);
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
