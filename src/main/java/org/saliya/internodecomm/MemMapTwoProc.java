package org.saliya.internodecomm;

import mpi.Comm;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;

import java.io.IOException;
import java.nio.DoubleBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.stream.IntStream;

public class MemMapTwoProc {
    public static void main(String[] args) throws MPIException {
        args = MPI.Init(args);
        Intracomm worldProcComm = MPI.COMM_WORLD;
        int worldProcRank = worldProcComm.getRank();
        int worldProcCount = worldProcComm.getSize();

        int size = Integer.parseInt(args[0]);
        String file = "/dev/shm/tmp.bin";
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
            int myOffsetInBytes = myOffset*Double.BYTES;
            int myExtent = mySize*Double.BYTES;
            int fullExtent = size*Double.BYTES;

            Bytes fullBytes = ByteBufferBytes.wrap(fc.map(FileChannel.MapMode.READ_WRITE,0, fullExtent));
            Bytes writeBytes = fullBytes.slice(myOffsetInBytes, myExtent);
            for (int i = 0; i < mySize; ++i){
                writeBytes.position(i*Double.BYTES);
                writeBytes.writeDouble(randomValues[i+myOffset]);
            }

            double[] readValues = new double[size];
            for (int i = 0; i < size; ++i){
                readValues[i] = fullBytes.readDouble(i*Double.BYTES);
            }

            for (int i = 0; i < size; ++i){
                if (randomValues[i] != readValues[i]){
                    System.out.println("Inconsistent rank " + worldProcRank + " " + i + " expected " + randomValues[i] + " found " + readValues[i]);
                }
            }


            // OK this fails  - two views
         /*   MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_WRITE, myOffset, myExtent);
            MappedByteBuffer readMbb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fullExtent);

            mbb.asDoubleBuffer().put(randomValues,myOffset, mySize);
            worldProcComm.barrier();
            double[] readValues = new double[size];
            readMbb.position(0);
            readMbb.asDoubleBuffer().get(readValues);*/


            // This fails too. So interesting. Next try with some screwing around and also with OpenHFT Bytes
            /*MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_WRITE, 0, fullExtent);
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
            }*/

            /*testIfDivisionOfPointsIsCorrect(worldProcComm, worldProcRank,
                                            worldProcCount, size, randomValues,
                                            mySize, myOffset);*/

        }
        catch (IOException e) {
            e.printStackTrace();
        }


        MPI.Finalize();
    }

    private static void testIfDivisionOfPointsIsCorrect(
        Intracomm worldProcComm, int worldProcRank, int worldProcCount,
        int size, double[] randomValues, int mySize, int myOffset)
        throws MPIException {
        double[] partial = new double[mySize];
        for (int z = myOffset; z < myOffset+mySize; ++z){
            partial[z-myOffset] = randomValues[z];
        }

        DoubleBuffer partialBuffer = MPI.newDoubleBuffer(mySize);
        partialBuffer.put(partial);
        DoubleBuffer fullBuffer = MPI.newDoubleBuffer(size);
        allGather(partialBuffer, 1, worldProcCount, mySize, worldProcRank, worldProcComm, fullBuffer);
        double[] readValues = new double[size];
        fullBuffer.position(0);
        fullBuffer.get(readValues);
        for (int i = 0; i < size; ++i){
            if (randomValues[i] != readValues[i]){
                System.out.println("Inconsistent rank " + worldProcRank + " " + i + " expected " + randomValues[i] + " found " + readValues[i]);
            }
        }
    }

    public static DoubleBuffer allGather(
        DoubleBuffer partialPointBuffer, int dimension, int procCount,
        int procRowCount, int procRank, Comm procComm, DoubleBuffer pointBuffer) throws MPIException {

        int [] lengths = new int[procCount];
        int length = procRowCount * dimension;
        lengths[procRank] = length;
        procComm.allGather(lengths, 1, MPI.INT);
        int [] displas = new int[procCount];
        displas[0] = 0;
        System.arraycopy(lengths, 0, displas, 1, procCount - 1);
        Arrays.parallelPrefix(displas, (m, n) -> m + n);
        procComm.allGatherv(partialPointBuffer, length, MPI.DOUBLE, pointBuffer, lengths, displas, MPI.DOUBLE);
        return  pointBuffer;
    }
}
