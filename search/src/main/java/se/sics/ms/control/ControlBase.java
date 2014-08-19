package se.sics.ms.control;

/**
 * Base Class for the Control Message Responses.
 * @author babbarshaer
 */

public abstract class ControlBase {

    protected ControlMessageResponseTypeEnum controlMessageResponseTypeEnum;
    // Add abstract methods to this.

    public ControlBase(ControlMessageResponseTypeEnum controlMessageResponseTypeEnum){
        this.controlMessageResponseTypeEnum = controlMessageResponseTypeEnum;
    }

    public ControlMessageResponseTypeEnum getControlMessageResponseTypeEnum(){
        return this.controlMessageResponseTypeEnum;
    }
}
