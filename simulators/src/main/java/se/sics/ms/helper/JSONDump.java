package se.sics.ms.helper;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import se.sics.ms.aggregator.design.ReplicationLagDesignInfo;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;

/**
 * Helper Class for performing a JSON Dump.
 * Created by babbar on 2015-09-21.
 */
public class JSONDump {


    public static void dumpSystemLagInfo(Collection<ReplicationLagDesignInfo> collection ,String fileLocation) throws IOException {

        JSONArray list = new JSONArray();

        for(ReplicationLagDesignInfo obj : collection){

            JSONObject object = new JSONObject();
            object.put("time", obj.time);
            object.put("avgLag", obj.averageLag);
            object.put("maxLag", obj.maxLag);
            object.put("minLag", obj.minLag);

            list.add(object);
        }


        FileWriter writer = new FileWriter(fileLocation);
        writer.write(list.toJSONString());
        writer.flush();
        writer.close();

    }



}
