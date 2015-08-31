package org.saliya.internodecomm;

import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.ShortBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class MemMapPrimer {
    public static void main(String[] args) throws MPIException {
        MPI.Init(args);
        Intracomm worldComm = MPI.COMM_WORLD;
        int worldRank = worldComm.getRank();
        int worldSize = worldComm.getSize();

        try (FileChannel fc = FileChannel.open(Paths.get("test.bin"), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            int size = 4; // size in bytes
            long pos = size*worldRank;
            MappedByteBuffer mappedBytes = fc.map(FileChannel.MapMode.READ_WRITE, pos, size);
            ShortBuffer sb = mappedBytes.asShortBuffer();
            int numShorts = size / Short.BYTES;
            sb.position(0);
            for (int i = 0; i < numShorts; ++i){
                short s = (short) worldRank;
                sb.put(s);
            }

            worldComm.barrier();
            if (worldRank == 0) {
                sb = fc.map(FileChannel.MapMode.READ_ONLY, 0, size*worldSize).asShortBuffer();
                sb.position(0);
                for (int i = 0; i < numShorts*worldSize; ++i) {
                    System.out.println(sb.get(i));
                }
            }

        }
        catch (IOException e) {
            e.printStackTrace();
        }
        MPI.Finalize();
    }
}
