package se.sics.ms.common;

import se.sics.ms.types.PeerDescriptor;
import se.sics.p2ptoolbox.util.Container;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

/**
 * Container used by the routing table to keep track of entries.
 *
 * Created by babbar on 2015-04-17.
 */
public class RoutingTableContainer implements Container<DecoratedAddress, PeerDescriptor>{

    private int age;
    private DecoratedAddress address;
    private PeerDescriptor descriptor;

    public RoutingTableContainer(int age, DecoratedAddress address,  PeerDescriptor descriptor) {
        this.age = age;
        this.address = address;
        this.descriptor = descriptor;
    }

    public int getAge() {
        return age;
    }

    public String toString(){
        return "Routing Table Entry: " + " Age: " + this.age + " Address: " + this.address + " Content: " + this.descriptor;
    }

    @Override
    public DecoratedAddress getSource() {
        return this.address;
    }

    @Override
    public PeerDescriptor getContent() {
        return this.descriptor;
    }

    public void incrementAge(){
        age++;
    }
}
