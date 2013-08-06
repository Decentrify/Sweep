package se.sics.peersearch.messages;

import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.peersearch.net.MessageFrameDecoder;
import sun.misc.BASE64Encoder;

import java.security.PublicKey;

import static se.sics.gvod.net.util.UserTypesEncoderFactory.writeStringLength65536;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/2/13
 * Time: 1:49 PM
 */
public class PublicKeyMessage extends DirectMsgNetty.Oneway {
    private final PublicKey publicKey;

    public PublicKeyMessage(VodAddress source, VodAddress destination, PublicKey publicKey) {
        super(source, destination);

        if(publicKey == null)
            throw new NullPointerException("public key can't be null");

        this.publicKey = publicKey;
    }

    public PublicKey getPublicKey() {
        return publicKey;
    }

    @Override
    public int getSize() {
        return getHeaderSize();
    }

    @Override
    public RewriteableMsg copy() {
        return new PublicKeyMessage(vodSrc, vodDest, publicKey);
    }

    @Override
    public ByteBuf toByteArray() throws MessageEncodingException {
        ByteBuf buffer = createChannelBufferWithHeader();
        writeStringLength65536(buffer, new BASE64Encoder().encode(publicKey.getEncoded()));
        return buffer;
    }

    @Override
    public byte getOpcode() {
        return MessageFrameDecoder.PUBLIC_KEY_MESSAGE;
    }
}
