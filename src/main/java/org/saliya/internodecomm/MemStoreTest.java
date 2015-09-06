package org.saliya.internodecomm;

import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.MappedStore;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

public class MemStoreTest {
    public static void main(String[] args) {
        long size = 2000;
        File f = new File("src/main/resources/test.ms");
        try {
            MappedStore ms = new MappedStore(f, FileChannel.MapMode.READ_WRITE, size<<3);
            DirectBytes bytes = ms.bytes();
            for (int i = 0; i < size; ++i){
                bytes.writeDouble(i + Math.random());
            }

            System.out.println("Came here");


        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
