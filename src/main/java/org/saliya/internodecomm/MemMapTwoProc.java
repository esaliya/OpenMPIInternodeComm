package org.saliya.internodecomm;

import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.stream.IntStream;

public class MemMapTwoProc {
    public static void main(String[] args) throws MPIException {
        args = MPI.Init(args);
        Intracomm worldProcComm = MPI.COMM_WORLD;
        int worldProcRank = worldProcComm.getRank();
        int worldProcCount = worldProcComm.getSize();

        int size = 100;
        String file = "/scratch/tmp.bin";
        boolean isWriter = worldProcRank == 0;
        try(FileChannel fc = FileChannel.open(Paths.get(file),
                                              StandardOpenOption.CREATE, isWriter ? StandardOpenOption.WRITE : StandardOpenOption.READ)) {
            double[] randomValues = new double[size];
            if (isWriter){
                IntStream.range(0, size).parallel().forEach(
                    i -> randomValues[i] = Math.random());
            }
            worldProcComm.bcast(randomValues, size, MPI.DOUBLE, 0);
            // Alright, everyone knows what the writer will write by now.

            int extent = size*Double.BYTES;
            MappedByteBuffer mbb = fc.map(isWriter ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE, 0, extent);
            if (isWriter){
                mbb.asDoubleBuffer().put(randomValues);
            }
            worldProcComm.barrier();
            double[] readValues = new double[size];
            mbb.position(0);
            mbb.asDoubleBuffer().get(readValues);

            for (int i = 0; i < size; ++i){
                if (randomValues[i] != readValues[i]){
                    System.out.println("Inconsistent rank " + worldProcRank + " " + i + " expected " + randomValues[i] + " found " + readValues[i]);
                }
            }

        }
        catch (IOException e) {
            e.printStackTrace();
        }


        MPI.Finalize();
    }
}
