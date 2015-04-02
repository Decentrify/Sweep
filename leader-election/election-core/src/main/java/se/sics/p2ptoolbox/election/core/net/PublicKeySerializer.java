package se.sics.p2ptoolbox.election.core.net;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Base64;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.util.UserTypesDecoderFactory;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.p2ptoolbox.serialization.SerializationContext;
import se.sics.p2ptoolbox.serialization.Serializer;
import sun.misc.BASE64Encoder;

import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;

import static se.sics.gvod.net.util.UserTypesDecoderFactory.readStringLength65536;

/**
 * Public Key Serializer.
 *
 * Created by babbar on 2015-04-02.
 */
public class PublicKeySerializer implements Serializer<PublicKey>{

    @Override
    public ByteBuf encode(SerializationContext context, ByteBuf byteBuf, PublicKey publicKey) throws SerializerException, SerializationContext.MissingException {

        try{
            if(publicKey == null){
                UserTypesEncoderFactory.writeStringLength65536(byteBuf, new String());
            }
            else{
                UserTypesEncoderFactory.writeStringLength65536(byteBuf,  new BASE64Encoder().encode(publicKey.getEncoded()));
            }
        }
        catch (MessageEncodingException e) {
            throw new SerializerException(e);
        }

        return byteBuf;
    }

    @Override
    public PublicKey decode(SerializationContext serializationContext, ByteBuf byteBuf) throws SerializerException, SerializationContext.MissingException {

        try {
            String stringKey = UserTypesDecoderFactory.readStringLength65536(byteBuf);

            if(stringKey == null){
                return null;
            }

            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            byte[] decode = Base64.decodeBase64(stringKey.getBytes());
            X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(decode);
            PublicKey pub = keyFactory.generatePublic(publicKeySpec);

            return pub;
        }
        catch(Exception ex){
            throw new SerializerException(ex);
        }
    }

    @Override
    public int getSize(SerializationContext serializationContext, PublicKey publicKey) throws SerializerException, SerializationContext.MissingException {

        int size = 0;
        String pubString;

        if(publicKey == null){
            pubString = null;
        }
        else{
            pubString = new BASE64Encoder().encode(publicKey.getEncoded());
        }

        size += UserTypesEncoderFactory.getStringLength65356(pubString);
        return size;
    }
}
