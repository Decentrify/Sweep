package se.sics.ms.serializer;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.EntryAddCommit;
import se.sics.ms.helper.SerializerDecoderHelper;
import se.sics.ms.helper.SerializerEncoderHelper;
import se.sics.ms.types.ApplicationEntry;

import java.util.UUID;

/**
 * Serializer for the Entry Add Commit Message As 
 * part of the entry addition protocol.
 * *
 * Created by babbarshaer on 2015-06-25.
 */
public class EntryAddCommitSerializer implements Serializer{
    
    private int id;
    
    public EntryAddCommitSerializer(int id){
        this.id = id;
    }
    
    
    @Override
    public int identifier() {
        return this.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        
        try {
            
            EntryAddCommit.Request request = (EntryAddCommit.Request)o;
            Serializers.lookupSerializer(UUID.class).toBinary(request.getCommitRoundId(), buf);
            Serializers.lookupSerializer(ApplicationEntry.ApplicationEntryId.class).toBinary(request.getEntryId(), buf);
            SerializerEncoderHelper.writeStringLength65536(buf, request.getSignature());
            
        } catch (MessageEncodingException e) {
            e.printStackTrace();
            throw new RuntimeException("Entry Add Serialization Failed", e);
        }
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
        
        
        try {
            
            UUID commitRoundId = (UUID) Serializers.lookupSerializer(UUID.class).fromBinary(buf, hint);
            ApplicationEntry.ApplicationEntryId entryId = (ApplicationEntry.ApplicationEntryId) Serializers.lookupSerializer(ApplicationEntry.ApplicationEntryId.class).fromBinary(buf, hint);
            String signature = SerializerDecoderHelper.readStringLength65536(buf);
            
            return new EntryAddCommit.Request(commitRoundId, entryId, signature);
        } 
        catch (MessageDecodingException e) {
            
            e.printStackTrace();
            throw new RuntimeException("Unable to decode the entry add commit", e);
        }
    }
    
    
}
