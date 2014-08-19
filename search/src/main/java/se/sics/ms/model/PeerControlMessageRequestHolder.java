package se.sics.ms.model;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * This object is used to hold responses received from various components,
 * which will be sent forward in the Control Message Response Object.
 *
 * @author babbarshaer
 */
public class PeerControlMessageRequestHolder {

    private int count;
    private final int maxCount;
    private ByteBuf buffer;

    /**
     * Parametrized Constructor.
     * @param maxCount
     */
    public PeerControlMessageRequestHolder(int maxCount){

       this.count =0;
       this.buffer = Unpooled.buffer(0);
       this.buffer.writeInt(maxCount);
       this.maxCount = maxCount;

    }

    /**
     * @return isComplete.
     */
    public boolean addAndCheckStatus(){

        count++;
        return (count >= maxCount) ? true : false;

    }


    /**
     * Creates a new buffer with zero length.
     */
    public void reset(){

        count=0;
        this.buffer = Unpooled.buffer(0);
        this.buffer.writeInt(maxCount);

    }

    /**
     * Get the contents of buffer.
     * @return
     */
    public ByteBuf getBuffer(){
        return this.buffer;
    }

}
