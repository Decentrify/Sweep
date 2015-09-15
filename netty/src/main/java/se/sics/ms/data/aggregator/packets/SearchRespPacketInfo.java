
package se.sics.ms.data.aggregator.packets;
import se.sics.ktoolbox.aggregator.common.PacketInfo;

/**
 * Packet Containing information regarding the latest search response time.
 *
 * Created by babbar on 2015-09-06.
 */
public class SearchRespPacketInfo implements PacketInfo {


    private float searchResponse;

    public SearchRespPacketInfo(float searchResponse){
        this.searchResponse = searchResponse;
    }

    @Override
    public String toString() {
        return "SearchRespPacketInfo{" +
                "searchResponse=" + searchResponse +
                '}';
    }

    public float getSearchResponse() {
        return searchResponse;
    }

}
