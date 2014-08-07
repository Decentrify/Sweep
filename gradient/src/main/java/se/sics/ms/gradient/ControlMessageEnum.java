package se.sics.ms.gradient;

/**
 * Enum used to specify different types of Control Messages.
 * @author babbarshaer
 *
 */
public enum ControlMessageEnum {

    PARTITION_UPDATE,
    LEADER_UPDATE,
    NO_PARTITION_UPDATE,
    REJOIN;

    private ControlMessageEnum(){}
}
