package se.sics.ms.types;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.DirectMsg;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.ms.data.ComponentStatus;

import java.util.Map;

/**
 * The periodic update message sent by the Aggregator 
 * to the external component, for analysis.
 *
 * Created by babbarshaer on 2015-02-20.
 */
public class AggregatorUpdateMsg extends DirectMsgNetty.Oneway{
    
    private final Map<String, ComponentStatus> componentStatusMap;
    
    public AggregatorUpdateMsg(VodAddress source, VodAddress destination, Map<String, ComponentStatus> componentStatusMap) {
        super(source, destination);
        this.componentStatusMap = componentStatusMap;
    }

    public Map<String, ComponentStatus> getComponentStatusMap(){
        return this.componentStatusMap;
    }
    
    @Override
    public int getSize() {
        return 0;
    }

    @Override
    public RewriteableMsg copy() {
        return null;
    }

    @Override
    public ByteBuf toByteArray() throws MessageEncodingException {
        return null;
    }

    @Override
    public byte getOpcode() {
        return 0;
    }
}
