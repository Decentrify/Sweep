package se.sics.ms.types;

import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.net.VodAddress;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by alidar on 8/11/14.
 */
public class
        SearchDescriptor implements DescriptorBase, Comparable<SearchDescriptor>, Serializable {

    private se.sics.gvod.net.VodAddress vodAddress;
    private int age;
    private transient boolean connected;

    //// Conversion functions
    public static VodDescriptor toVodDescriptor(SearchDescriptor searchDescriptor) {
        VodDescriptor descriptor = new VodDescriptor(searchDescriptor.getVodAddress());
        descriptor.setAge(searchDescriptor.getAge());
        descriptor.setConnected(searchDescriptor.isConnected());

        return descriptor;
    }

    public static List<VodDescriptor> toVodDescriptorList(List<SearchDescriptor> searchDescriptors)
    {
        ArrayList<VodDescriptor> descriptorList = new ArrayList<VodDescriptor>();

        for(SearchDescriptor descriptor: searchDescriptors) {
            descriptorList.add(SearchDescriptor.toVodDescriptor(descriptor));
        }

        return  descriptorList;
    }

    public static List<SearchDescriptor> toSearchDescriptorList(List<VodDescriptor> descriptors) {

        ArrayList<SearchDescriptor> searchDescriptorsList = new ArrayList<SearchDescriptor>();

        for(VodDescriptor descriptor: descriptors) {
            searchDescriptorsList.add(new SearchDescriptor(descriptor));
        }

        return searchDescriptorsList;
    }
    ////

    public SearchDescriptor(VodDescriptor descriptor) {
        this(descriptor.getVodAddress(), descriptor.getAge(), descriptor.isConnected());
    }

    public SearchDescriptor(se.sics.gvod.net.VodAddress vodAddress) {
        this(vodAddress, 0, false);
    }

    public SearchDescriptor(se.sics.gvod.net.VodAddress vodAddress, int age) {
        this(vodAddress, age, false);
    }

    public SearchDescriptor(se.sics.gvod.net.VodAddress vodAddress, SearchDescriptor searchDescriptor) {
        this(vodAddress, searchDescriptor.getAge(), searchDescriptor.isConnected());
    }

    public SearchDescriptor(se.sics.gvod.net.VodAddress vodAddress, int age, boolean connected) {
        this.vodAddress = vodAddress;
        setAge(age);
        this.connected = connected;
    }

    public VodAddress getVodAddress() {
        return vodAddress;
    }

    public int getAge() {
        return age;
    }

    public int getId() {
        return vodAddress.getId();
    }

    public int incrementAndGetAge() {
        return ++age;
    }

    public boolean isConnected() {
        return connected;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    public void setAge(int age) {
        if (age > 65535) {
            age = 65535;
        }
        if (age < 0) {
            age = 0;
        }
        this.age = age;
    }

    @Override
    public int compareTo(SearchDescriptor that) {
        if (this.age > that.age) {
            return 1;
        }
        if (this.age < that.age) {
            return -1;
        }
        return 0;
    }

    @Override
    public int hashCode() {
        final int prime = 87;
        int result = 1;
        result = prime * result + ((vodAddress == null) ? 0 : vodAddress.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SearchDescriptor other = (SearchDescriptor) obj;
        if (vodAddress == null) {
            if (other.vodAddress != null) {
                return false;
            }
        } else if (other.vodAddress == null) {
            return false;
        }

        return vodAddress.equals(other.getVodAddress());
    }

    @Override
    public String toString() {
        return vodAddress.toString();
    }
}