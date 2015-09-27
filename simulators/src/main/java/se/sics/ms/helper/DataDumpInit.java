package se.sics.ms.helper;

import se.sics.kompics.Init;
import se.sics.ms.main.AggregatorCompHelper;

/**
 * Initialization class for the data dump in the system.
 *
 * Created by babbar on 2015-09-18.
 */
public class DataDumpInit {


    public static class Write extends Init<DataDump.Write>{

        public final AggregatorCompHelper helper;
        public final int maxWindowsPerFile;

        public Write(AggregatorCompHelper helper, int maxWindowsPerFile){
            this.helper = helper;
            this.maxWindowsPerFile = maxWindowsPerFile;
        }
    }


    public static class Read extends Init<DataDump.Read>{

        public final String location;

        public Read(String location){
            this.location = location;
        }
    }

}
