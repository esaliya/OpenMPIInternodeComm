package org.saliya.internodecomm;

import edu.indiana.soic.spidal.common.Range;
import edu.indiana.soic.spidal.common.RangePartitioner;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.MappedStore;

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

    public static DirectBytes mmapXReadBytes;
    public static ByteBuffer mmapXReadByteBuffer;
    public static DirectBytes mmapXWriteBytes;
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
        double[][] X = calculateNothing(preX, targetDimension);

        MPI.Finalize();
    }

    private static double[][] calculateNothing(double[][] preX, int targetDimension) throws MPIException, InterruptedException {
        double [][][] partials = new double[threadCount][][];

        if (threadCount > 1) {
            launchHabaneroApp(
                () -> forallChunked(
                    0, threadCount - 1,
                    (threadIdx) -> {
                        partials[threadIdx] =
                            calculateNothingInternal(threadIdx, preX,
                                                     targetDimension);
                    }));
        }
        else {
            partials[0] = calculateNothingInternal(
                0, preX, targetDimension);
        }

        if (worldProcsCount > 1) {
            mergePartials(partials, targetDimension, mmapXWriteBytes);
            // Check if you get points local to you as is if read by mmapXWriteBytes - This MUST work unless some indexing error with points
            for (int i = procRowStartOffset; i < procRowStartOffset+procRowCount; ++i){
                for (int j = 0; j < targetDimension; ++j){
                    double originalValue = preX[i][j];
                    double writtenValue = mmapXWriteBytes.readDouble((i-procRowStartOffset)*targetDimension*Double.BYTES + j*Double.BYTES);
                    if (writtenValue != originalValue){
                        System.out.println(
                            "Rank " + worldProcRank + " testloopNeg2-(" + i + "," + j + ") originalValue " + originalValue + " writtenValue " + writtenValue);
                    }
                }
            }

            // Check if what you wrote can be read through your reader
            int offset = procRowStartOffset - procRowRanges[mmapLeadWorldRank].getStartIndex();
            for (int i = procRowStartOffset; i < procRowStartOffset+procRowCount; ++i){
                for (int j = 0; j < targetDimension; ++j) {
                    double originalValue = preX[i][j];
                    double writtenValueAsReadByReader = mmapXReadBytes.readDouble((offset+(i-procRowStartOffset))*targetDimension*Double.BYTES + j*Double.BYTES);
                    if (originalValue != writtenValueAsReadByReader) {
                        System.out.println(
                            "Rank " + worldProcRank + " testloopNeg1-(" + i +
                            "," + j + ") originalValue " + originalValue
                            + " writtenValueAsReadByReader " +
                            writtenValueAsReadByReader);
                    }
                }
            }


            // NO this doesn't work. Putting a thread sleep works means that we have a delay in write propagation. Shit!
            Thread.sleep(5);
            // Check if what all in your mem group wrote can be read through your reader
            int mmapLeadRowOffset = procRowRanges[mmapLeadWorldRank].getStartIndex();
            for (int i = 0; i < mmapProcsRowCount; ++i){
                for (int j = 0; j < targetDimension; ++j){
                    double writtenValue = mmapXReadBytes.readDouble(i*targetDimension*Double.BYTES+j*Double.BYTES);
                    double originalValue = preX[mmapLeadRowOffset+i][j];
                    if (writtenValue != originalValue){
                        System.out.println(
                            "Rank " + worldProcRank + " testloop0-(" + i + "," + j + ") originalValue " + originalValue + " writtenValue " + writtenValue);
                    }
                }
            }

            return null;
            /*if (isMmapLead) {
                partialXAllGather();
            }
            // Each process in a memory group waits here.
            // It's not necessary to wait for a process
            // in another memory map group, hence the use of mmapProcComm
            //            mmapProcComm.barrier();
            // TODO - remove after testing
            worldProcsComm.barrier();

            double[][] result = extractPoints(
                fullXBytes, globalColCount,
                targetDimension);
            if (worldProcRank == 0) {
                // TODO - a test to see if we assume writes are all good then this read should be good
                // because it's reading the buffer returned by MPI allgather.
                // OK it's a FAILURE, so writing may not be good.
                *//*result = extractPoints(
                    fullXByteBuffer, globalColCount,
                    targetDimension);*//*
                for (int i = 0; i < result.length; ++i) {
                    for (int j = 0; j < targetDimension; ++j) {
                        if (preX[i][j] != result[i][j]) {
                            System.out.println(
                                "testloop1-(" + i + "," + j + ") preX " + preX[i][j] + " result " + result[i][j]);
                        }
                    }
                }
            }
            return result;*/
        }else {
            double [][] result = new double[globalColCount][targetDimension];
            mergePartials(partials, targetDimension, result);
            for (int i = 0; i < result.length; ++i) {
                for (int j = 0; j < targetDimension; ++j) {
                    if (preX[i][j] != result[i][j]) {
                        System.out.println(
                            "testloop-2(" + i + "," + j + ") preX " + preX[i][j] + " result " + result[i][j]);
                    }
                }
            }
            return result;
        }

    }

    private static double[][] calculateNothingInternal(int threadIdx, double[][] preX, int targetDimension){
        final int threadRowCount = threadRowCounts[threadIdx];
        final int globalRowStartOffset = threadRowStartOffsets[threadIdx] + procRowStartOffset;
        double[][] array = new double[threadRowCount][targetDimension];
        for (int i = globalRowStartOffset; i < threadRowCount+globalRowStartOffset; ++i){
            System.arraycopy(preX[i], 0, array[i-globalRowStartOffset],0, targetDimension);
        }
        return array;
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
        final String lockAndCountFname = machineName + ".lockAndCount.bin";

        int mmapXReadByteExtent = mmapProcsRowCount * targetDimension * Double.BYTES;
        long mmapXReadByteOffset = 0L;
        int mmapXWriteByteExtent = procRowCount * targetDimension * Double.BYTES;
        long mmapXWriteByteOffset = (procRowStartOffset - procRowRanges[mmapLeadWorldRank].getStartIndex()) * targetDimension * Double.BYTES;
        int fullXByteExtent = globalRowCount * targetDimension * Double.BYTES;
        long fullXByteOffset = 0L;
        try (MappedStore mmapXMS = new MappedStore(new File(mmapScratchDir + File.separator + mmapXFname),
                                                       FileChannel.MapMode.READ_WRITE,mmapXReadByteExtent);
            MappedStore fullXMS = new MappedStore(new File(mmapScratchDir + File.separator + fullXFname),
                                                  FileChannel.MapMode.READ_WRITE,fullXByteExtent)){

            mmapXReadBytes = mmapXMS.bytes();
            mmapXReadByteBuffer = MPI.newByteBuffer(mmapXReadByteExtent);

            mmapXWriteBytes = mmapXMS.bytes(mmapXWriteByteOffset, mmapXWriteByteExtent);

            fullXBytes = fullXMS.bytes();
            fullXByteBuffer = MPI.newByteBuffer(fullXByteExtent);

            /*lockAndCountBytes = ByteBufferBytes.wrap(lockAndCountFc.map(
                FileChannel.MapMode.READ_WRITE, 0, LOCK_AND_COUNT_EXTENT));*/

            // Print debug info in order of world ranks
            for (int i = 0; i < worldProcsCount; ++i){
                intBuffer.put(0, i);
                worldProcsComm.bcast(intBuffer, 1, MPI.INT, 0);
                int next = intBuffer.get(0);
                if (next == worldProcRank){
                    try (BufferedWriter bw = Files.newBufferedWriter(Paths.get(
                                                                         "mmap.debug"
                                                                         +
                                                                         worldProcRank
                                                                         + ".out.txt"),
                                                                     StandardOpenOption.CREATE,
                                                                     StandardOpenOption.WRITE)) {

                        PrintWriter writer = new PrintWriter(bw, true);
                        // Good it's my turn to print
                        writer.println(
                            "World rank: " + worldProcRank + " on " + machineName);
                        writer.println("  mmapIdLocalToNode:             " + mmapIdLocalToNode);
                        writer.println("  mmapProcsCount:                " + mmapProcsCount);
                        writer.println("  isMmapLead:                    " + isMmapLead);
                        writer.println("  mmapProcsWorldRanks:           " + Arrays.toString(
                            mmapProcsWorldRanks));
                        writer.println("  mmapLeadWorldRankLocalToNode:  "
                                       + "" + mmapLeadWorldRankLocalToNode);
                        writer.println("  mmapLeadWorldRank:             " + mmapLeadWorldRank);
                        writer.println("  cgProcRank:                    " + cgProcRank);
                        writer.println("  cgProcsCount:                  "
                                       + "" + cgProcsCount);
                        writer.println("  cgProcsMmapRowCounts:              "
                                       + Arrays.toString(cgProcsMmapRowCounts));
                        writer.println("  mmapXReadByteExtent:      "
                                       + mmapXReadByteExtent);
                        writer.println("  mmapXWriteByteExtent:           "
                                       + mmapXWriteByteExtent);
                        writer.println("  mmapXWriteByteOffset:                "
                                       + mmapXWriteByteOffset);
                        writer.println("  fullXByteExtent:                   "
                                       + fullXByteExtent);
                        writer.println("  fullXByteOffset:                   "
                                       + fullXByteOffset);
                    }
                }
            }
        }
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

    public static void partialXAllGather() throws MPIException {
        mmapXReadByteBuffer.position(0);
        mmapXReadBytes.position(0);
        mmapXReadBytes.read(mmapXReadByteBuffer);
        fullXByteBuffer.position(0);
        cgProcComm.allGatherv(mmapXReadByteBuffer,
                              cgProcsMmapXByteExtents[cgProcRank], MPI.BYTE,
                              fullXByteBuffer, cgProcsMmapXByteExtents,
                              cgProcsMmapXDisplas, MPI.BYTE);
        fullXBytes.position(0);
        fullXByteBuffer.position(0);
        fullXBytes.write(fullXByteBuffer);
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
        double[][][] partials, int targetDimension, Bytes result){
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