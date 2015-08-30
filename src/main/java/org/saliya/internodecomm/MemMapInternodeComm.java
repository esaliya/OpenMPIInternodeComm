package org.saliya.internodecomm;

import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;

public class MemMapInternodeComm {
    public static void main(String[] args) throws MPIException {
        args = MPI.Init(args);
        Intracomm worldComm = MPI.COMM_WORLD;
        int worldRank = worldComm.getRank();
        int worldSize = worldComm.getSize();
        System.out.println("Hello from " + worldRank + " of " + worldSize);
        MPI.Finalize();
    }
}
