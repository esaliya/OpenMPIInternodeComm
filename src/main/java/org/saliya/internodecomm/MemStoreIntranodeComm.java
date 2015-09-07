package org.saliya.internodecomm;

import edu.indiana.soic.spidal.common.Range;
import edu.indiana.soic.spidal.common.RangePartitioner;
import mpi.Comm;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;
import net.openhft.lang.io.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;

import static edu.rice.hj.Module0.asyncNbAwait;
import static edu.rice.hj.Module0.launchHabaneroApp;
import static edu.rice.hj.Module1.forallChunked;

public class MemStoreIntranodeComm {
    public static String machineName;
    public static int nodeCount=1;
    public static int threadCount=1;

    public static int nodeId;

    public static Intracomm worldProcsComm;
    public static int worldProcRank;
    public static int worldProcsCount;
    public static int worldProcsPerNode;

    public static Intracomm mmapProcComm;
    // Number of memory mapped groups per process
    public static int mmapsPerNode;
    public static String mmapScratchDir;
    public static int worldProcRankLocalToNode;
    public static int mmapIdLocalToNode;
    public static int mmapProcsCount;
    public static boolean isMmapLead;
    public static int[] mmapProcsWorldRanks;
    public static int mmapLeadWorldRank;
    public static int mmapLeadWorldRankLocalToNode;
    public static int mmapProcsRowCount;

    // mmap leaders form one communicating group and the others (followers)
    // belong to another communicating group.
    public static Intracomm cgProcComm;
    public static int cgProcRank;
    public static int cgProcsCount;
    public static int[] cgProcsMmapRowCounts;
    public static int[] cgProcsMmapXByteExtents;
    public static int[] cgProcsMmapXDisplas;

    public static String parallelPattern;
    public static Range[] procRowRanges;
    public static Range procRowRange;
    public static int procRowStartOffset;
    public static int procRowCount;

    public static long procPointStartOffset;
    public static Range[] threadRowRanges;
    public static int[] threadRowStartOffsets;
    public static int[] threadRowCounts;


    public static int[] threadPointStartOffsets;

    public static int globalColCount;

    // Buffers for MPI operations
    private static ByteBuffer statBuffer;
    private static DoubleBuffer doubleBuffer;
    private static IntBuffer intBuffer;
    static DoubleBuffer partialPointBuffer;
    static DoubleBuffer pointBuffer;
    public static LongBuffer threadsAndMPIBuffer;
    public static LongBuffer mpiOnlyBuffer;

    public static MappedStore mmapXMS;
    public static MappedStore fullXMS;
    public static DirectBytes mmapXReadBytes;
    public static ByteBuffer mmapXReadByteBuffer;
    public static NativeBytes mmapXWriteBytes;
    public static DirectBytes fullXBytes;
    public static ByteBuffer fullXByteBuffer;


    public static void main(String[] args)
        throws MPIException, InterruptedException, IOException {
        args = MPI.Init(args);

        nodeCount = Integer.parseInt(args[0]);
        threadCount = Integer.parseInt(args[1]);
        mmapsPerNode = Integer.parseInt(args[2]);
        mmapScratchDir = args[3];

        int numberDataPoints = Integer.parseInt(args[4]);
        int targetDimension = Integer.parseInt(args[5]);

        setupParallelism();
        setParallelDecomposition(numberDataPoints, targetDimension);
        double[][] preX = generateInitMapping(numberDataPoints,
                                              targetDimension);

        DoubleBuffer mmapXpartial = MPI.newDoubleBuffer(procRowCount * targetDimension * Double.BYTES);
        long
            mmapXWriteByteOffset =
            (procRowStartOffset
             - procRowRanges[mmapLeadWorldRank].getStartIndex())
            * targetDimension * Double.BYTES;

        long count = 0L;
        for (int i = procRowStartOffset;
             i < procRowCount + procRowStartOffset; ++i) {
            for (int j = 0; j < targetDimension; ++j) {
                double d = preX[i][j];
                mmapXReadBytes.writeDouble(mmapXWriteByteOffset + count* Double.BYTES, d);
                mmapXpartial.put(d);
                ++count;
            }
        }

        DoubleBuffer mmapXfull = MPI.newDoubleBuffer(mmapProcsRowCount * targetDimension * Double.BYTES);
        allGather(mmapXpartial, mmapXfull, targetDimension, mmapProcComm.getSize(), mmapProcComm.getRank(), mmapProcComm);
        double[][] whatWeWrote = extractPoints(mmapXfull,mmapProcsRowCount, targetDimension);

        final int startIndex = procRowRanges[mmapLeadWorldRank].getStartIndex();
        for (int i = startIndex; i < startIndex+mmapProcsRowCount; ++i){
            for (int j = 0; j < targetDimension; ++j){
                double original = preX[i][j];
//                double read = whatWeWrote[i-startIndex][j];
                double read = mmapXfull.get((((i-startIndex)*targetDimension)+j));
//                double read = mmapXReadBytes.readDouble((((i-startIndex)*targetDimension)+j)*Double.BYTES);
                if (original != read){
                    System.out.println("Shit! It's wrong still at i " + i  + " j " + j + "on rank " + worldProcRank);
                }
            }
        }

        /*for (int i = procRowStartOffset;
             i < procRowCount + procRowStartOffset; ++i) {
            for (int j = 0; j < targetDimension; ++j) {
                double d = preX[i][j];
                mmapXWriteBytes.writeDouble(d);
            }
        }

        final int startIndex = procRowRanges[mmapLeadWorldRank].getStartIndex();
        for (int i = startIndex; i < startIndex+mmapProcsRowCount; ++i){
            for (int j = 0; j < targetDimension; ++j){
                double original = preX[i][j];
                double read = mmapXReadBytes.readDouble(((i-startIndex)*targetDimension+j)*Double.BYTES);
                if (original != read){
                    System.out.println("Shit! It's wrong still at i " + i  + " j " + j + "on rank " + worldProcRank);
                }
            }
        }*/
        System.out.println("Came here");

       /* int mmapXReadByteExtent = mmapProcsRowCount * targetDimension * Double.BYTES;
        long mmapXReadByteOffset = 0L;
        int mmapXWriteByteExtent = procRowCount * targetDimension * Double.BYTES;
        long mmapXWriteByteOffset = (procRowStartOffset - procRowRanges[mmapLeadWorldRank].getStartIndex()) * targetDimension * Double.BYTES;

        final String mmapXFname = machineName + ".mmapId." + mmapIdLocalToNode + ".mmapX.bin";
        *//*try (MappedStore mmapXMS = new MappedStore(new File(mmapScratchDir + File.separator + mmapXFname),
                                                   FileChannel.MapMode.READ_WRITE,mmapXReadByteExtent)) {

//            DirectBytes bytes = mmapXMS.bytes(mmapXWriteByteOffset, mmapXWriteByteExtent);
            mmapXWriteBytes = mmapXMS.bytes(mmapXWriteByteOffset, mmapXWriteByteExtent);
//            bytes.positionAndSize(0L, mmapXWriteByteExtent);
            mmapXWriteBytes.positionAndSize(0L, mmapXWriteByteExtent);*//*
        try (MappedStore mmapXMS = new MappedStore(new File(mmapScratchDir + File.separator + mmapXFname),
                                                   FileChannel.MapMode.READ_WRITE,mmapXReadByteExtent)) {

            //            DirectBytes bytes = mmapXMS.bytes(mmapXWriteByteOffset, mmapXWriteByteExtent);
            mmapXWriteBytes = mmapXMS.bytes(mmapXWriteByteOffset,
                                            mmapXWriteByteExtent);
            //            bytes.positionAndSize(0L, mmapXWriteByteExtent);
            mmapXWriteBytes.positionAndSize(0L, mmapXWriteByteExtent);


            int count = 0;
            for (int i = procRowStartOffset;
                 i < procRowCount + procRowStartOffset; ++i) {
                for (int j = 0; j < targetDimension; ++j) {
                    double d = preX[i][j];
                    //                    bytes.writeDouble(d);
                    mmapXWriteBytes.writeDouble(d);
                    ++count;
                }
            }

            System.out.println(
                "Rank: " + worldProcRank + " " + (count * Double.BYTES
                                                  == mmapXWriteByteExtent));

            System.out.println("Came here");
        }*/
        /*}*/

        /*double[][] X = calculateNothing(preX, targetDimension);*/

        /*mmapXReadBytes.release();
        mmapXWriteBytes.release();
        fullXBytes.release();*/
        tearDownParallelism();
    }

    public static void allGather(
        DoubleBuffer partialPointBuffer, DoubleBuffer result, int dimension, int procCount,
        int procRank, Comm procComm) throws MPIException {

        int [] lengths = new int[procCount];
        int length = procRowCount * dimension;
        lengths[procRank] = length;
        procComm.allGather(lengths, 1, MPI.INT);
        int [] displas = new int[procCount];
        displas[0] = 0;
        System.arraycopy(lengths, 0, displas, 1, procCount - 1);
        Arrays.parallelPrefix(displas, (m, n) -> m + n);
        procComm.allGatherv(partialPointBuffer, length, MPI.DOUBLE, result, lengths, displas, MPI.DOUBLE);
    }

    public static void tearDownParallelism() throws MPIException {
        if (mmapXMS != null) mmapXMS.close();
        if (fullXMS != null) fullXMS.close();
        if (mmapXReadBytes != null) mmapXReadBytes.close();
        if (mmapXWriteBytes != null) mmapXWriteBytes.close();
        if (fullXBytes != null) fullXBytes.close();

        MPI.Finalize();
    }

    public static void setupParallelism() throws MPIException {
        worldProcsComm = MPI.COMM_WORLD; //initializing MPI world communicator
        worldProcRank = worldProcsComm.getRank();
        worldProcsCount = worldProcsComm.getSize();

        /* Create communicating groups */
        worldProcsPerNode = worldProcsCount / nodeCount;

        worldProcRankLocalToNode = worldProcRank % worldProcsPerNode;
        nodeId = worldProcRank / worldProcsPerNode;
        int q = worldProcsPerNode / mmapsPerNode;
        int r = worldProcsPerNode % mmapsPerNode;

        // Memory mapped groups and communicating groups
        mmapIdLocalToNode =
            worldProcRankLocalToNode < r * (q + 1)
                ? worldProcRankLocalToNode / (q + 1)
                : (worldProcRankLocalToNode - r) / q;
        mmapProcsCount = worldProcRankLocalToNode < r*(q+1) ? q+1 : q;
        isMmapLead = worldProcRankLocalToNode % mmapProcsCount == 0;
        mmapProcsWorldRanks = new int[mmapProcsCount];
        mmapLeadWorldRankLocalToNode =
            isMmapLead
                ? worldProcRankLocalToNode
                : (q * mmapIdLocalToNode + (mmapIdLocalToNode < r
                                                ? mmapIdLocalToNode
                                                : r));
        mmapLeadWorldRank = worldProcRank - (worldProcRankLocalToNode
                                             - mmapLeadWorldRankLocalToNode);
        for (int i = 0; i < mmapProcsCount; ++i){
            mmapProcsWorldRanks[i] = mmapLeadWorldRank +i;
        }

        // Leaders talk, their color is 0
        // Followers will get a communicator of color 1,
        // but will make sure they don't talk ha ha :)
        cgProcComm = worldProcsComm.split(isMmapLead ? 0 : 1, worldProcRank);
        cgProcRank = cgProcComm.getRank();
        cgProcsCount = cgProcComm.getSize();

        // Communicator for processes within a  memory map group
        mmapProcComm = worldProcsComm.split((nodeId*mmapsPerNode)+mmapIdLocalToNode, worldProcRank);

        /* Allocate basic buffers for communication */
        doubleBuffer = MPI.newDoubleBuffer(1);
        intBuffer = MPI.newIntBuffer(1);

        machineName = MPI.getProcessorName();

    }

    public static void setParallelDecomposition(int globalRowCount, int targetDimension)
        throws MPIException, IOException {
        procRowRanges = RangePartitioner.partition(globalRowCount,
                                                   worldProcsCount);
        Range rowRange = procRowRanges[worldProcRank]; // The range of points for this process

        procRowRange = rowRange;
        procRowStartOffset = rowRange.getStartIndex();
        procRowCount = rowRange.getLength();
        globalColCount = globalRowCount;
        procPointStartOffset = ((long)procRowStartOffset) * globalColCount;

        // Next partition points per process among threads
        threadRowRanges = RangePartitioner.partition(procRowCount, threadCount);
        threadRowCounts = new int[threadCount];
        threadRowStartOffsets = new int[threadCount];
        threadPointStartOffsets = new int[threadCount];
        IntStream.range(0, threadCount)
            .parallel()
            .forEach(threadIdx -> {
                Range threadRowRange = threadRowRanges[threadIdx];
                threadRowCounts[threadIdx] =
                    threadRowRange.getLength();
                threadRowStartOffsets[threadIdx] =
                    threadRowRange.getStartIndex();
                threadPointStartOffsets[threadIdx] =
                    threadRowStartOffsets[threadIdx] * globalColCount;
            });

        // Allocate vector buffers
        partialPointBuffer = MPI.newDoubleBuffer(procRowCount * targetDimension);
        pointBuffer = MPI.newDoubleBuffer(globalRowCount * targetDimension);
        mpiOnlyBuffer = MPI.newLongBuffer(worldProcsCount);
        threadsAndMPIBuffer = MPI.newLongBuffer(worldProcsCount * threadCount);

        cgProcsMmapRowCounts = new int[cgProcsCount];
        cgProcsMmapXByteExtents = new int[cgProcsCount];
        cgProcsMmapXDisplas = new int[cgProcsCount];
        mmapProcsRowCount = IntStream.range(mmapLeadWorldRank,
                                            mmapLeadWorldRank + mmapProcsCount)
            .map(i -> procRowRanges[i].getLength())
            .sum();
        if (isMmapLead){
            cgProcsMmapRowCounts[cgProcRank] = mmapProcsRowCount;
            cgProcComm.allGather(cgProcsMmapRowCounts, 1, MPI.INT);
            for (int i = 0; i < cgProcsCount; ++i){
                cgProcsMmapXByteExtents[i] = cgProcsMmapRowCounts[i] * targetDimension * Double.BYTES;
            }

            cgProcsMmapXDisplas[0] = 0;
            System.arraycopy(cgProcsMmapXByteExtents, 0, cgProcsMmapXDisplas, 1, cgProcsCount - 1);
            Arrays.parallelPrefix(cgProcsMmapXDisplas, (m, n) -> m + n);
        }

        final String mmapXFname = machineName + ".mmapId." + mmapIdLocalToNode + ".mmapX.bin";
        final String fullXFname = machineName + ".mmapId." + mmapIdLocalToNode +".fullX.bin";

        int mmapXReadByteExtent = mmapProcsRowCount * targetDimension * Double.BYTES;
        long mmapXReadByteOffset = 0L;
        int mmapXWriteByteExtent = procRowCount * targetDimension * Double.BYTES;
        long mmapXWriteByteOffset = (procRowStartOffset - procRowRanges[mmapLeadWorldRank].getStartIndex()) * targetDimension * Double.BYTES;
        int fullXByteExtent = globalRowCount * targetDimension * Double.BYTES;
        long fullXByteOffset = 0L;


        mmapXMS = new MappedStore(new File(mmapScratchDir + File.separator + mmapXFname),
                                              FileChannel.MapMode.READ_WRITE,mmapXReadByteExtent);
        fullXMS = new MappedStore(new File(mmapScratchDir + File.separator + fullXFname),
                                              FileChannel.MapMode.READ_WRITE,fullXByteExtent);

        mmapXReadBytes = mmapXMS.bytes(mmapXReadByteOffset, mmapXReadByteExtent);
        mmapXReadByteBuffer = MPI.newByteBuffer(mmapXReadByteExtent);

        mmapXWriteBytes = mmapXReadBytes.slice(mmapXWriteByteOffset,
                                               mmapXWriteByteExtent);

        fullXBytes = fullXMS.bytes(fullXByteOffset, fullXByteExtent);
        fullXByteBuffer = MPI.newByteBuffer(fullXByteExtent);
    }

    public static double allReduce(double value) throws MPIException{
        doubleBuffer.put(0, value);
        worldProcsComm.allReduce(doubleBuffer, 1, MPI.DOUBLE, MPI.SUM);
        return doubleBuffer.get(0);
    }

    public static int allReduce(int value) throws MPIException{
        intBuffer.put(0, value);
        worldProcsComm.allReduce(intBuffer, 1, MPI.INT, MPI.SUM);
        return intBuffer.get(0);
    }

    public static void broadcast(DoubleBuffer buffer, int extent, int root)
        throws MPIException {
        worldProcsComm.bcast(buffer, extent, MPI.DOUBLE, root);
    }

    public static void gather(LongBuffer buffer, int count, int root)
        throws MPIException {
        worldProcsComm.gather(buffer, count, MPI.LONG, root);
    }

    static double[][] generateInitMapping(int numPoints,
                                          int targetDim) throws MPIException {

        DoubleBuffer buffer = pointBuffer;
        if (worldProcRank == 0) {
            buffer.position(0);
            // Use Random class for generating random initial mapping solution.
            Random rand = new Random(System.currentTimeMillis());
            for (int i = 0; i < numPoints; i++) {
                for (int j = 0; j < targetDim; j++) {
                    buffer.put(rand.nextBoolean() ? rand.nextDouble() : -rand.nextDouble());
                }
            }
        }

        if (worldProcsCount > 1){
            // Broadcast initial mapping to others
            broadcast(buffer, numPoints * targetDim, 0);
        }
        return extractPoints(buffer, numPoints, targetDim);
    }

    private static void mergePartials(double [][][] partials, int dimension, double [][] result){
        int row = 0;
        for (double [][] partial : partials){
            for (double [] point : partial){
                System.arraycopy(point, 0, result[row], 0, dimension);
                ++row;
            }
        }
    }

    private static void mergePartials(
        double[][][] partials, int targetDimension, DirectBytes result){
        result.position(0);
        int pos = 0;
        for (double [][] partial : partials){
            for (double [] point : partial){
                for (int i = 0; i < targetDimension; ++i){
                    result.position(pos);
                    result.writeDouble(point[i]);
                    pos += Double.BYTES;
                }
            }
        }
        //        result.force();
    }

    private static double[][] extractPoints(
        Bytes bytes, int numPoints, int dimension) {
        int pos = 0;
        double [][] points = new double[numPoints][dimension];
        for (int i = 0; i < numPoints; ++i){
            double[] pointsRow = points[i];
            for (int j = 0; j < dimension; ++j) {
                bytes.position(pos);
                pointsRow[j] = bytes.readDouble(pos);
                pos += Double.BYTES;
            }
        }
        return  points;
    }

    private static double[][] extractPoints(
        ByteBuffer buffer, int numPoints, int dimension) {
        int pos = 0;
        double [][] points = new double[numPoints][dimension];
        for (int i = 0; i < numPoints; ++i){
            double[] pointsRow = points[i];
            for (int j = 0; j < dimension; ++j) {
                buffer.position(pos);
                pointsRow[j] = buffer.getDouble(pos);
                pos += Double.BYTES;
            }
        }
        return  points;
    }

    private static double[][] extractPoints(
        DoubleBuffer buffer, int numPoints, int dimension) {
        int pos = 0;
        double [][] points = new double[numPoints][dimension];
        for (int i = 0; i < numPoints; ++i){
            buffer.position(pos);
            buffer.get(points[i]);
            pos += dimension;
        }
        return  points;
    }

}
