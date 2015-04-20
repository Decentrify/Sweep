package se.sics.ms.control;

import se.sics.gvod.net.VodAddress;
import se.sics.ms.gradient.control.ControlMessageEnum;
import se.sics.ms.util.PartitionHelper;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.util.List;

/**
 * Wrapper for the partition update received.
 * @author babbarshaer
 */
public class PartitionControlResponse extends ControlBase{


    private ControlMessageEnum controlMessageEnum;
    private List<PartitionHelper.PartitionInfoHash> partitionUpdateHashes;
    private DecoratedAddress sourceAddress;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PartitionControlResponse that = (PartitionControlResponse) o;

        if (sourceAddress != null ? !sourceAddress.equals(that.sourceAddress) : that.sourceAddress != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return sourceAddress != null ? sourceAddress.hashCode() : 0;
    }

    public PartitionControlResponse(ControlMessageEnum controlMessageEnum , List<PartitionHelper.PartitionInfoHash> partitionUpdateHashes , DecoratedAddress sourceAddress){

        super(ControlMessageResponseTypeEnum.PARTITION_UPDATE_RESPONSE);
        this.controlMessageEnum = controlMessageEnum;
        this.partitionUpdateHashes = partitionUpdateHashes;
        this.sourceAddress = sourceAddress;
    }

    public ControlMessageEnum getControlMessageEnum(){
        return this.controlMessageEnum;
    }

    public List<PartitionHelper.PartitionInfoHash> getPartitionUpdateHashes(){
        return this.partitionUpdateHashes;
    }

    public DecoratedAddress getSourceAddress(){
        return this.sourceAddress;
    }


}
