package org.saliya.internodecomm;

import edu.indiana.soic.spidal.common.Range;
import edu.indiana.soic.spidal.common.RangePartitioner;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;
import mpi.Win;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class OneSidedComm {
    static Bytes bytes;
    static ByteBuffer byteBuffer;
    public static void main(String[] args) throws MPIException {
        args = MPI.Init(args);
        Intracomm worldProcComm = MPI.COMM_WORLD;
        int worldProcRank = worldProcComm.getRank();
        int worldProcsCount = worldProcComm.getSize();

        int size = Integer.parseInt(args[0]);
        Range[] ranges = RangePartitioner.partition(size, worldProcsCount);
        Range myRange = ranges[worldProcRank];

        String fname = "/dev/shm/mmap.tmp";
        try (FileChannel fc = FileChannel.open(Paths.get(fname),
                                                    StandardOpenOption.CREATE,StandardOpenOption.WRITE,StandardOpenOption.READ)) {
            int extent = size*Double.BYTES;
            bytes = ByteBufferBytes.wrap(fc.map(
                FileChannel.MapMode.READ_WRITE, 0L,
                extent));
//            byteBuffer = bytes.sliceAsByteBuffer(byteBuffer);
            byteBuffer = MPI.newByteBuffer(extent);


            Win win = new Win(byteBuffer, extent, Double.BYTES, MPI.INFO_NULL, worldProcComm);

            for (int i = 0; i < myRange.getLength(); ++i){
                bytes.writeDouble(i*Double.BYTES, worldProcRank);
            }
            win.fence(0);
            if (worldProcRank != 0){
                win.put(byteBuffer, extent, MPI.DOUBLE, 0, myRange.getStartIndex(), myRange.getLength(), MPI.DOUBLE);
            }
            win.fence(0);
            win.free();

            worldProcComm.barrier();
            if (worldProcRank == 0){
                for (int i = 0; i < size; ++i) {
                    System.out.println(byteBuffer.get(i*Double.BYTES));
                }
            }
            worldProcComm.barrier();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        MPI.Finalize();
    }
}
