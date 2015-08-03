/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.ms.util;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public enum HeartbeatServiceEnum {
    CROUPIER((byte)-1)
    ;
    private byte serviceId;

    HeartbeatServiceEnum(byte serviceId) {
        this.serviceId = serviceId;
    }
    
    /**
     * Needs to be initiated to a unique value before it is used
     * Typically should be initiated in Launcher
     * @param serviceId 
     */
    public void setServiceId(byte serviceId) {
        this.serviceId = serviceId;
    }
    
    /**
     * Needs to be initiated to a unique value before it is used
     * Typically should be initiated in Launcher
     * @param serviceId 
     */
    public byte getServiceId() {
        return serviceId;
    }
}
