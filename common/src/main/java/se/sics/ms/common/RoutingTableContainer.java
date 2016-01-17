package se.sics.ms.common;

import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.other.Container;
import se.sics.ms.types.PeerDescriptor;

/**
 * Container used by the routing table to keep track of entries.
 *
 * Created by babbar on 2015-04-17.
 */
public class RoutingTableContainer implements Container<KAddress, PeerDescriptor>{

    private int age;
    public final KAddress address;
    public final PeerDescriptor descriptor;

    public RoutingTableContainer(int age, KAddress address,  PeerDescriptor descriptor) {
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
    public KAddress getSource() {
        return address;
    }

    @Override
    public PeerDescriptor getContent() {
        return descriptor;
    }

    public void incrementAge(){
        age++;
    }

    @Override
    public Container copy() {
        return new RoutingTableContainer(age, address, descriptor);
    }
}
