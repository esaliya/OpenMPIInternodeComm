package org.saliya.internodecomm;

import mpi.Group;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;

import java.nio.IntBuffer;

public class MemMapInternodeComm {
    public static void main(String[] args) throws MPIException {
        args = MPI.Init(args);

        int nodeCount = Integer.parseInt(args[0]);
        int mmapsPerNode = Integer.parseInt(args[1]);


        Intracomm worldComm = MPI.COMM_WORLD;
        int worldRank = worldComm.getRank();
        int worldSize = worldComm.getSize();

        assert worldSize % nodeCount == 0;
        int worldProcsPerNode = worldSize / nodeCount;
        // Number of communicating groups per node

        int worldNodeLocalRank = worldRank % worldProcsPerNode;
        int q = worldProcsPerNode / mmapsPerNode;
        int r = worldProcsPerNode % mmapsPerNode;

        // Communicating group
        int cgId = worldNodeLocalRank < r*(q+1) ? worldNodeLocalRank/(q+1) : (worldNodeLocalRank-r)/q;
        int cgProcCountPerNode = worldNodeLocalRank < r*(q+1) ? q+1 : q;
        boolean cgLead = worldNodeLocalRank % cgProcCountPerNode == 0;

        // Leaders talk, their color is 0
        // Followers will get a communicator of color 1, but will make sure they don't talk ha ha :)
        Intracomm cgComm = worldComm.split(cgLead ? 0 : 1, worldRank);

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
