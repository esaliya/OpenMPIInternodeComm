package org.saliya.internodecomm;

import mpi.Group;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;

import java.nio.IntBuffer;

public class MemMapInternodeComm {
    public static void main(String[] args) throws MPIException {
        args = MPI.Init(args);
        Intracomm worldComm = MPI.COMM_WORLD;
        int worldRank = worldComm.getRank();
        int worldSize = worldComm.getSize();
        System.out.println("Hello from " + worldRank + " of " + worldSize);

        int nodeCount = Integer.parseInt(args[0]);
        assert worldSize % nodeCount == 0;
        int worldProcsPerNode = worldSize / nodeCount;
        // Number of communicating groups per node
        int cgPerNode = Integer.parseInt(args[1]);

        int worldNodeLocalRank = worldRank % worldProcsPerNode;
        int q = worldProcsPerNode / cgPerNode;
        int r = worldProcsPerNode % cgPerNode;

        // Communicating group
        int cgId = worldNodeLocalRank < r*(q+1) ? worldNodeLocalRank/(q+1) : (worldNodeLocalRank-r)/q;
        int cgProcCountPerNode = worldNodeLocalRank < r*(q+1) ? q+1 : q;
        boolean cgLead = worldNodeLocalRank % cgProcCountPerNode == 0;

        Intracomm cgComm = worldComm.split(cgLead ? worldNodeLocalRank : worldSize+1, worldRank);

        // Testing
        if (cgLead) {
            IntBuffer buff = MPI.newIntBuffer(1);
            buff.position(0);
            buff.put(cgComm.getRank());
            cgComm.allReduce(buff, 1, MPI.INT, MPI.MAX);
            System.out.println("WorldRank: " + worldRank + " CGRank: " + cgComm.getRank() + " MaxCGRank: " + buff.get(0));
        }


        MPI.Finalize();
    }
}
