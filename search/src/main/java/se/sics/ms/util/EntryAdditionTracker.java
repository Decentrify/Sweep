package se.sics.ms.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.timer.TimeoutId;
import se.sics.ms.messages.ReplicationPrepareCommitMessage;
import se.sics.ms.search.Search;
import se.sics.ms.types.IndexEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

/**
 * Tracker class for the add index entry mechanism.
 * When the leader receives a add entry request, it starts the two phase commit
 * The class keeps track of the progress for the entry addition mechanism.
 * 
 * Created by babbarshaer on 2015-04-11.
 */
public class EntryAdditionTracker {
    
    private TimeoutId roundTrackerId;
    private Collection<VodAddress> leaderGroupNodes;
    private Logger logger = LoggerFactory.getLogger(Search.class);
    private int responses;
    private IndexEntry entryToAdd;
    private VodAddress entryAddSourceNode;
    
    public EntryAdditionTracker(){
        leaderGroupNodes = new ArrayList<VodAddress>();
    }

    /**
     * Start tracking a new round id for the entry addition mechanism.
     * @param roundId round id.
     * @param leaderGroup leader group nodes.
     */
    public void startTracking(TimeoutId roundId, Collection<VodAddress> leaderGroup, IndexEntry entry, VodAddress entryAddSourceNode){
        
        logger.debug("Started tracking a new round for entry addition.");
        this.roundTrackerId = roundId;
        this.leaderGroupNodes = leaderGroup;
        this.responses = 0;
        this.entryToAdd = entry;
        this.entryAddSourceNode = entryAddSourceNode;
    }


    /**
     * Check if the responses that are received are of the same round id that the node is tracking.
     * Increment the responses that are being tracked
     *
     * @param response Prepare response.
     */
    public void addPromiseResponse(ReplicationPrepareCommitMessage.Response response){
        
        if(roundTrackerId != null && response.getTimeoutId().equals(roundTrackerId)){
            responses +=1;
        }
        else{
            logger.warn("Received entry addition promise response for a old or an unknown round id.");
        }
    }

    /**
     * Get the round tracker id information.
     * @return timeoutId.
     */
    public TimeoutId getRoundTrackerId() {
        return roundTrackerId;
    }

    public Collection<VodAddress> getLeaderGroupNodes() {
        return leaderGroupNodes;
    }

    public VodAddress getEntryAddSourceNode() {
        return entryAddSourceNode;
    }

    public IndexEntry getEntry() {
        return entryToAdd;
    }

    public boolean promiseComplete(){
        return roundTrackerId!= null && leaderGroupNodes  != null && responses == leaderGroupNodes.size();
    }

    public void resetTracker(){
        
        this.roundTrackerId = null;
        this.leaderGroupNodes = null;
        this.responses = 0;
        this.entryToAdd = null;
        this.entryAddSourceNode = null;
    }
}

