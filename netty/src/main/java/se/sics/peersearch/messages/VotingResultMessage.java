package se.sics.peersearch.messages;

import org.jboss.netty.buffer.ChannelBuffer;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.peersearch.net.ApplicationTypesEncoderFactory;
import se.sics.peersearch.net.MessageFrameDecoder;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/8/13
 * Time: 10:48 AM
 */
public class VotingResultMessage extends DirectMsgNetty{
    private final VodAddress[] view;

    public VotingResultMessage(VodAddress source, VodAddress destination, VodAddress[] view) {
        super(source, destination);
        this.view = view;
    }

    public VodAddress[] getView() {
        return view;
    }

    @Override
    public int getSize() {
        return getHeaderSize();
    }

    @Override
    public RewriteableMsg copy() {
        return new VotingResultMessage(vodSrc, vodDest, view);
    }

    @Override
    public ChannelBuffer toByteArray() throws MessageEncodingException {
        ChannelBuffer buffer = createChannelBufferWithHeader();
        ApplicationTypesEncoderFactory.writeVodAddressArray(buffer, view);
        return buffer;
    }

    @Override
    public byte getOpcode() {
        return MessageFrameDecoder.VOTING_RESULT_MESSAGE;
    }
}
