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

        public final String location;
        public final AggregatorCompHelper helper;

        public Write(String location, AggregatorCompHelper helper){

            this.location = location;
            this.helper = helper;
        }
    }


    public static class Read extends Init<DataDump.Read>{

        public final String location;

        public Read(String location){
            this.location = location;
        }
    }

}
