package org.saliya.internodecomm;

import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.MappedStore;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

public class MemStoreMPITest {
    public static void main(String[] args) throws MPIException {
        MPI.Init(args);
        Intracomm comm = MPI.COMM_WORLD;
        int rank = comm.getRank();
        int count = comm.getSize();
        long size = 200;
        long extent = size * count;
        File f = new File(args[0] + "/test.ms");
        DirectBytes bytes = null;
        try {
            MappedStore ms = new MappedStore(f, FileChannel.MapMode.READ_WRITE, extent<<3);
            bytes = ms.bytes(size*rank*Double.BYTES, size*Double.BYTES);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < size; ++i){
            bytes.writeDouble(i + Math.random());
        }

        System.out.println("Came here");
        MPI.Finalize();
    }
}
