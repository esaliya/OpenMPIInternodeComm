package org.saliya.internodecomm;

import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.MappedStore;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

public class MemStoreMPITest2 {
    public static void main(String[] args) throws MPIException {
        MPI.Init(args);
        Intracomm procComm = MPI.COMM_WORLD;
        int procRank = procComm.getRank();
        int procCount = procComm.getSize();

        int totalRowCount = 1000;
        int targetDimension = 3;

        int q = totalRowCount / procCount;
        int r = totalRowCount % procCount;

        int myRowCount = procRank < r ? q+1 : q;
        int myRowOffset = procRank < r ? (q+1)*procRank : q*procRank + r;

        int myByteOffset = myRowOffset * targetDimension * Double.BYTES;
        int myByteExtent = myRowCount * targetDimension * Double.BYTES;
        int totalByteExtent = totalRowCount * targetDimension * Double.BYTES;

        File f = new File(args[0] + "/test.ms");
        DirectBytes bytes = null;
        try {
            MappedStore ms = new MappedStore(f, FileChannel.MapMode.READ_WRITE, totalByteExtent);
            bytes = ms.bytes(myByteOffset, myByteExtent);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        assert bytes != null;
        for (int i = 0; i < myRowCount; ++i){
            for (int j = 0; j < targetDimension; ++j){
                bytes.writeDouble((i*targetDimension+j)*Double.BYTES, Math.random());
            }
        }

        System.out.println("Came here");
        MPI.Finalize();
    }
}