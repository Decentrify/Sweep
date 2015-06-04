//package se.sics.ms.common;
//
//import se.sics.gvod.common.SelfImpl;
//import se.sics.gvod.common.VodDescriptor;
//import se.sics.gvod.common.vod.VodView;
//import se.sics.gvod.config.VodConfig;
//import se.sics.gvod.net.Nat;
//import se.sics.gvod.net.VodAddress;
//import se.sics.ms.types.OverlayAddress;
//import se.sics.ms.types.OverlayId;
//import se.sics.ms.types.SearchDescriptor;
//import se.sics.ms.util.OverlayIdHelper;
//
//import java.net.InetAddress;
//import java.util.concurrent.atomic.AtomicLong;
//
///**
// * Main container class in the application containing the information used by the
// * node to identify its utility and its identity in the system. <br/>
// *
// * CAUTION: I can see a lot of duplication and unnecessary variable creation.
// *
// * @author Steffen Grohsschmiedt
// */
//public class MsSelfImpl extends SelfImpl {
//
//    private AtomicLong numberOfIndexEntries = new AtomicLong();
//    private boolean isLGMember;
//
//    public MsSelfImpl(VodAddress addr) {
//        super(addr);
//        this.isLGMember = false;
//    }
//
//    public MsSelfImpl(Nat nat, InetAddress ip, int port, int nodeId, int overlayId) {
//        super(nat, ip, port, nodeId, overlayId);
//        this.isLGMember = false;
//    }
//
//    public MsSelfImpl(Nat nat, InetAddress ip, int port, int nodeId, int overlayId, long indexEntries, boolean isLGMember){
//        super(nat, ip, port, nodeId, overlayId);
//        this.numberOfIndexEntries.set(indexEntries);
//        this.isLGMember = isLGMember;
//    }
//
//    public MsSelfImpl(VodAddress address, long indexEntries, boolean isLGMember){
//        super(address);
//        this.numberOfIndexEntries.set(indexEntries);
//        this.isLGMember = isLGMember;
//    }
//
//    public boolean isLGMember() {
//        return isLGMember;
//    }
//
//    public void setLGMember(boolean isLGMember) {
//        this.isLGMember = isLGMember;
//    }
//
//    public void setNumberOfIndexEntries(long numberOfIndexEntries) {
//        this.numberOfIndexEntries.set(numberOfIndexEntries);
//    }
//
//    public void incrementNumberOfIndexEntries() {
//        this.numberOfIndexEntries.incrementAndGet();
//    }
//
//    /**
//     * The use of the descriptor needs to be deprecated as it is not linked
//     * with the current application.<br/>
//     *
//     * Use: {@link #getSelfDescriptor()}
//     * @deprecated
//     * @return VodDescriptor
//     */
//    @Override
//    public VodDescriptor getDescriptor() {
//        int age = 0;
//        return  new VodDescriptor(getAddress(), VodView.getPeerUtility(this), age, VodConfig.LB_MTU_MEASURED,
//                numberOfIndexEntries.get());
//    }
//
//    /**
//     * Construct a descriptor based on the information contained in the object.
//     *
//     * @return Self Descriptor.
//     */
//    public SearchDescriptor getSelfDescriptor(){
//        return new SearchDescriptor(new OverlayAddress(null , this.overlayId), false, this.numberOfIndexEntries.get(), this.isLGMember, null);
//    }
//
//    public MsSelfImpl clone() {
//
//        MsSelfImpl clonedObj = new MsSelfImpl(getAddress(), this.numberOfIndexEntries.get(), isLGMember);
//        return clonedObj;
//    }
//
//
//    public void setOverlayId(int overlayId) {
//        this.overlayId = overlayId;
//    }
//
//    public int getCategoryId() {
//        return OverlayIdHelper.getCategoryId(overlayId);
//    }
//
//    public int getPartitionId() {
//        return OverlayIdHelper.getPartitionId(overlayId);
//    }
//
//    public int getPartitionIdDepth() {
//        return OverlayIdHelper.getPartitionIdDepth(overlayId);
//    }
//
//    public VodAddress.PartitioningType getPartitioningType() {
//        return OverlayIdHelper.getPartitioningType(overlayId);
//    }
//
//    public long getNumberOfIndexEntries(){
//        return numberOfIndexEntries.get();
//    }
//
//
//
//
//
//}
