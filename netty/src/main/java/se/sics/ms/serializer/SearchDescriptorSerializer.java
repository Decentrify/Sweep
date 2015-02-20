package se.sics.ms.serializer;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.util.UserTypesDecoderFactory;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.ms.types.OverlayAddress;
import se.sics.ms.types.SearchDescriptor;
import se.sics.p2ptoolbox.serialization.SerializationContext;
import se.sics.p2ptoolbox.serialization.Serializer;

/**
 * Serializer for the Search Descriptor.
 * Created by babbarshaer on 2015-02-14.
 */
public class SearchDescriptorSerializer implements Serializer<SearchDescriptor>{

    private Logger logger = LoggerFactory.getLogger(SearchDescriptorSerializer.class);
    @Override
    public ByteBuf encode(SerializationContext serializationContext, ByteBuf buffer, SearchDescriptor descriptor) throws SerializerException, SerializationContext.MissingException {

        
        // == Identify items that we need to encode for a search descriptor.
        // 1. VodAddress.
        // 2. Sample Age.
        // 3. Number of Index Entries.
        // 4. received partitioning depth.
        // 5. connected. ? (declared as transient in VodDescriptor, so leaving it as it is ... )

        try {
            
            UserTypesEncoderFactory.writeVodAddress(buffer, descriptor.getVodAddress());
            buffer.writeInt(descriptor.getAge());
            buffer.writeLong(descriptor.getNumberOfIndexEntries());
            buffer.writeInt(descriptor.getReceivedPartitionDepth());
            
        } catch (MessageEncodingException e) {
            logger.error("Message Encoding Failed.");
            e.printStackTrace();
        }

        return buffer;
    }

    /**
     * Search Descriptor Deserializer.
     *
     * @param serializationContext
     * @param byteBuf
     * @return
     * @throws SerializerException
     * @throws SerializationContext.MissingException
     */
    @Override
    public SearchDescriptor decode(SerializationContext serializationContext, ByteBuf byteBuf) throws SerializerException, SerializationContext.MissingException {

        SearchDescriptor descriptor = null;
        
        try {

            VodAddress vodAddress = UserTypesDecoderFactory.readVodAddress(byteBuf);
            int age = byteBuf.readInt();
            long numberOfIndexEntries = byteBuf.readLong();
            int partitioningDepth = byteBuf.readInt();
            
            descriptor = new SearchDescriptor(new OverlayAddress(vodAddress),age,false,numberOfIndexEntries,partitioningDepth);
            
            
        } catch (MessageDecodingException e) {
            logger.error("Search Descriptor decoding failed.");
            throw new SerializerException(e.getMessage());
        }

        return descriptor;
    }

    @Override
    public int getSize(SerializationContext serializationContext, SearchDescriptor descriptor) throws SerializerException, SerializationContext.MissingException {
        
        int size = 0;

        // VodAddress.
        VodAddress addr = descriptor.getVodAddress();
        size += UserTypesEncoderFactory.ADDRESS_LEN; // address
        size += Integer.SIZE/8; // overlayId
        size += Byte.SIZE/8; //natPolicy
        size += (addr.getParents().isEmpty() ? 2 : 2 + addr.getParents().size() * UserTypesEncoderFactory.ADDRESS_LEN);
        
        // Age.
        size+= Integer.SIZE/8;
        
        // Index Entries.
        size += Long.SIZE/8;
        
        // Partitioning Depth.
        size += Integer.SIZE/8;
        
        return size;
    }
}
