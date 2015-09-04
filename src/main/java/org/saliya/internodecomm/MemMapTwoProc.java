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

        int size = Integer.parseInt(args[0]);
        String file = "/scratch/tmp.bin";
        try(FileChannel fc = FileChannel.open(Paths.get(file),
                                              StandardOpenOption.CREATE, StandardOpenOption.WRITE , StandardOpenOption.READ)) {
            double[] randomValues = new double[size];
            if (worldProcRank == 0){
                IntStream.range(0, size).parallel().forEach(
                    i -> randomValues[i] = Math.random());
            }
            worldProcComm.bcast(randomValues, size, MPI.DOUBLE, 0);
            // Alright, everyone knows what the writer will write by now.

            int q = size / worldProcCount;
            int r = size % worldProcCount;
            int mySize = worldProcRank < r ? q+1 : q;
            int myOffset = worldProcRank < r ? worldProcRank*(q+1) : worldProcRank*q + r;
            int myExtent = mySize*Double.BYTES;
            int fullExtent = size*Double.BYTES;

            // OK this fails  - two views
         /*   MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_WRITE, myOffset, myExtent);
            MappedByteBuffer readMbb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fullExtent);

            mbb.asDoubleBuffer().put(randomValues,myOffset, mySize);
            worldProcComm.barrier();
            double[] readValues = new double[size];
            readMbb.position(0);
            readMbb.asDoubleBuffer().get(readValues);*/

            MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_WRITE, 0, fullExtent);

            mbb.position(myOffset);
            mbb.asDoubleBuffer().put(randomValues,myOffset, mySize);
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
