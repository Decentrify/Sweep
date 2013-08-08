package se.sics.ms.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Base64;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.net.util.UserTypesDecoderFactory;

import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/2/13
 * Time: 1:53 PM
 */
public class PublicKeyMessageFactory extends DirectMsgNettyFactory.Oneway {

    private PublicKeyMessageFactory() {
    }

    public static PublicKeyMessage fromBuffer(ByteBuf buffer) throws MessageDecodingException {
        return (PublicKeyMessage) new PublicKeyMessageFactory().decode(buffer);
    }

    @Override
    protected PublicKeyMessage process(ByteBuf buffer) throws MessageDecodingException {
        String key = UserTypesDecoderFactory.readStringLength65536(buffer);
        KeyFactory keyFactory;
        PublicKey pub = null;
        try {
            keyFactory = KeyFactory.getInstance("RSA");
            byte[] decode = Base64.decodeBase64(key.getBytes());
            X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(decode);
            pub = keyFactory.generatePublic(publicKeySpec);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (InvalidKeySpecException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        return new PublicKeyMessage(vodSrc, vodDest, pub);
    }
}
