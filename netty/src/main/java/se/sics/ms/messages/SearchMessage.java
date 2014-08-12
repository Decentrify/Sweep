/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.ms.messages;

import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;
import io.netty.buffer.ByteBuf;
import se.sics.ms.types.SearchDescriptor;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.util.UserTypesEncoderFactory;
import se.sics.gvod.timer.ScheduleTimeout;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.exceptions.IllegalSearchString;
import se.sics.ms.net.ApplicationTypesEncoderFactory;
import se.sics.ms.net.MessageFrameDecoder;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchPattern;

/**
 *
 * @author jdowling
 */
public class SearchMessage {
    public static class Request extends DirectMsgNetty.Request {
        private final SearchPattern pattern;
        private final TimeoutId searchTimeoutId;
        private final int partitionId;

        public Request(VodAddress source, VodAddress destination, TimeoutId timeoutId, TimeoutId searchTimeoutId, SearchPattern pattern, int partitionId) {
            super(source, destination, timeoutId);
            this.partitionId = partitionId;

            if(pattern == null)
                throw new NullPointerException("pattern can't be null");

            this.pattern = pattern;
//            if (query.length() > 255) {
//                throw new IllegalSearchString("Search string is too long. Max length is 255 chars.");
//            }

            this.searchTimeoutId = searchTimeoutId;
        }

        public SearchPattern getPattern() {
            return pattern;
        }

        public TimeoutId getSearchTimeoutId() {
            return searchTimeoutId;
        }

        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public int getSize() {
            return getHeaderSize() + 30; // guess at length of query
        }

        @Override
        public RewriteableMsg copy() {
            SearchMessage.Request r = null;
            r = new Request(vodSrc, vodDest, timeoutId, searchTimeoutId, pattern, partitionId);
            return r;
        }

        @Override
        public ByteBuf toByteArray() throws MessageEncodingException {
            ByteBuf buffer = createChannelBufferWithHeader();
            ApplicationTypesEncoderFactory.writeSearchPattern(buffer, pattern);
            UserTypesEncoderFactory.writeTimeoutId(buffer, searchTimeoutId);
            buffer.writeInt(partitionId);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.SEARCH_REQUEST;
        }
    }

    public static class Response extends DirectMsgNetty.Response {
        
        public static final int MAX_RESULTS_STR_LEN = 1400;

        private final Collection<IndexEntry> results;
        private final int numResponses;
        private final int responseNumber;
        private final TimeoutId searchTimeoutId;
        private final int partitionId;
        
        public Response(VodAddress source, VodAddress destination, TimeoutId timeoutId, TimeoutId searchTimeoutId, int numResponses, int responseNumber, Collection<IndexEntry> results, int partitionId) throws IllegalSearchString {
            super(source, destination, timeoutId);
            this.partitionId = partitionId;

            if(results == null)
                throw new NullPointerException("results can't be null");

            this.numResponses = numResponses;
            this.responseNumber = responseNumber;
            this.results = results;
            this.searchTimeoutId = searchTimeoutId;
        }

        public Collection<IndexEntry> getResults() {
            return results;
        }

        public int getResponseNumber() {
            return responseNumber;
        }

        public int getNumResponses() {
            return numResponses;
        }

        public TimeoutId getSearchTimeoutId() {
            return searchTimeoutId;
        }

        public int getPartitionId() {
            return partitionId;
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
                return new SearchMessage.Response(vodSrc, vodDest, timeoutId, searchTimeoutId, numResponses, responseNumber, results, partitionId);
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
            UserTypesEncoderFactory.writeUnsignedintAsOneByte(buffer, numResponses);
            UserTypesEncoderFactory.writeUnsignedintAsOneByte(buffer, responseNumber);
            ApplicationTypesEncoderFactory.writeIndexEntryCollection(buffer, results);
            UserTypesEncoderFactory.writeTimeoutId(buffer, searchTimeoutId);
            buffer.writeInt(partitionId);
            return buffer;
        }

        @Override
        public byte getOpcode() {
            return MessageFrameDecoder.SEARCH_RESPONSE;

        }
    }

    /**
     * Timeout for active {@link se.sics.ms.messages.SearchMessage.Request}s.
     */
    public static class RequestTimeout extends IndividualTimeout {
        private final SearchDescriptor searchDescriptor;

        /**
         * @param request
         *            the ScheduleTimeout that holds the Timeout
         */
        public RequestTimeout(ScheduleTimeout request, int id, SearchDescriptor searchDescriptor) {
            super(request, id);
            this.searchDescriptor = searchDescriptor;
        }

        public SearchDescriptor getSearchDescriptor() {
            return searchDescriptor;
        }
    }
}
