/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.peersearch.messages;

import java.util.logging.Level;
import java.util.logging.Logger;
import io.netty.buffer.ByteBuf;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.msgs.RewriteableRetryTimeout;
import se.sics.gvod.net.msgs.ScheduleRetryTimeout;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.gvod.timer.TimeoutId;
import se.sics.gvod.timer.UUID;
import se.sics.peersearch.exceptions.IllegalSearchString;
import se.sics.peersearch.net.ApplicationTypesEncoderFactory;
import se.sics.peersearch.net.MessageFrameDecoder;
import se.sics.peersearch.types.IndexEntry;
import se.sics.peersearch.types.SearchPattern;

/**
 *
 * @author jdowling
 */
public class SearchMessage {
    public static class Request extends DirectMsgNetty {
        private final UUID requestId;
        private final SearchPattern pattern;

        public Request(VodAddress source, VodAddress destination,
                       TimeoutId timeoutId, UUID requestId, SearchPattern pattern)
        {
            super(source, destination, timeoutId);
            this.requestId = requestId;
            this.pattern = pattern;
//            if (query.length() > 255) {
//                throw new IllegalSearchString("Search string is too long. Max length is 255 chars.");
//            }
        }

        public UUID getRequestId() {
            return requestId;
        }

        public SearchPattern getPattern() {
            return pattern;
        }

        @Override
        public int getSize() {
            return getHeaderSize()
                    + 30 // guess at length of query
                    ;
        }

        @Override
        public RewriteableMsg copy() {
             SearchMessage.Request r = null;
            r = new Request(vodSrc, vodDest, timeoutId, requestId, pattern);
            return r;
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            UserTypesEncoderFactory.writeTimeoutId(buffer, requestId);
            ApplicationTypesEncoderFactory.writeSearchPattern(buffer, pattern);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.SEARCH_REQUEST;
        }
    }

    public static class Response extends DirectMsgNetty {
        
        public static final int MAX_RESULTS_STR_LEN = 1400;

        private final UUID requestId;
        private final IndexEntry[] results;
        private final int numResponses;
        private final int responseNumber;
        
        public Response(VodAddress source,
                        VodAddress destination, TimeoutId timeoutId,
                        UUID requestId, int numResponses, int responseNumber,
                        IndexEntry[] results) throws IllegalSearchString
        {
            super(source, destination, timeoutId);
            this.requestId = requestId;

            this.numResponses = numResponses;
            this.responseNumber = responseNumber;
            this.results = results;
        }

        public IndexEntry[] getResults() {
            return results;
        }

        public int getResponseNumber() {
            return responseNumber;
        }

        public int getNumResponses() {
            return numResponses;
        }

        public UUID getRequestId() {
            return requestId;
        }

        @Override
        public int getSize() {
            return getHeaderSize()
                    + 4 // numResponses
                    + 4 // responseNum
                    + MAX_RESULTS_STR_LEN; 
        }

        @Override
        public RewriteableMsg copy() {
            try {
                return new SearchMessage.Response(vodSrc, vodDest, timeoutId,
                        requestId, numResponses, responseNumber, results);
            } catch (IllegalSearchString ex) {
                // we can swallow the exception because the original object should 
                // have been correctly constructed.
                Logger.getLogger(SearchMessage.class.getName()).log(Level.SEVERE, null, ex);
            }
            // shouldn't get here.
            return null;
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            UserTypesEncoderFactory.writeTimeoutId(buffer, requestId);
            UserTypesEncoderFactory.writeUnsignedintAsOneByte(buffer, numResponses);
            UserTypesEncoderFactory.writeUnsignedintAsOneByte(buffer, responseNumber);
            ApplicationTypesEncoderFactory.writeIndexEntryArray(buffer, results);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.SEARCH_RESPONSE;

        }
    }

    public static class RequestTimeout extends RewriteableRetryTimeout {

        public RequestTimeout(ScheduleRetryTimeout st, RewriteableMsg retryMessage) {
            super(st, retryMessage);
        }
    }
}
