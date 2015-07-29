package se.sics.ms.gradient.gradient;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.co.FailureDetectorPort;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.*;
import se.sics.kompics.timer.Timer;
import se.sics.ms.aggregator.port.StatusAggregatorPort;
import se.sics.ms.common.ApplicationSelf;
import se.sics.ms.common.RoutingTableContainer;
import se.sics.ms.common.RoutingTableHandler;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.data.*;
import se.sics.ms.gradient.control.CheckLeaderInfoUpdate;
import se.sics.ms.gradient.control.ControlMessageInternal;
import se.sics.ms.gradient.events.*;
import se.sics.ms.gradient.misc.SimpleUtilityComparator;
import se.sics.ms.gradient.ports.GradientRoutingPort;
import se.sics.ms.gradient.ports.GradientViewChangePort;
import se.sics.ms.gradient.ports.LeaderStatusPort;
import se.sics.ms.messages.*;
import se.sics.ms.ports.SelfChangedPort;
import se.sics.ms.types.*;
import se.sics.ms.types.OverlayId;

import java.security.PublicKey;
import java.util.*;
import java.util.UUID;

import se.sics.ms.util.CommonHelper;
import se.sics.ms.util.ComparatorCollection;
import se.sics.p2ptoolbox.croupier.CroupierPort;
import se.sics.p2ptoolbox.croupier.msg.CroupierSample;
import se.sics.p2ptoolbox.election.api.msg.LeaderState;
import se.sics.p2ptoolbox.election.api.msg.LeaderUpdate;
import se.sics.p2ptoolbox.election.api.ports.LeaderElectionPort;
import se.sics.p2ptoolbox.gradient.GradientPort;
import se.sics.p2ptoolbox.gradient.msg.GradientSample;
import se.sics.p2ptoolbox.util.Container;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;
import se.sics.p2ptoolbox.util.network.impl.BasicContentMsg;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedHeader;


/**
 * The component is responsible for routing the requests and creating 
 * a wrapper over the routing table which keeps track of nodes in other
 * shards.
 *
 */
public final class Routing extends ComponentDefinition {

    private static final Logger logger = LoggerFactory.getLogger(Routing.class);
    Positive<Network> networkPort = positive(Network.class);
    Positive<Timer> timerPort = positive(Timer.class);

    Positive<GradientViewChangePort> gradientViewChangePort = positive(GradientViewChangePort.class);
    Positive<FailureDetectorPort> fdPort = requires(FailureDetectorPort.class);
    Positive<LeaderStatusPort> leaderStatusPort = requires(LeaderStatusPort.class);
    Positive<LeaderElectionPort> electionPort = requires(LeaderElectionPort.class);
    
    Negative<GradientRoutingPort> gradientRoutingPort = negative(GradientRoutingPort.class);
    Positive<SelfChangedPort> selfChangedPort = positive(SelfChangedPort.class);

    Positive<StatusAggregatorPort> statusAggregatorPortPositive = positive(StatusAggregatorPort.class);
    Positive<GradientPort> gradientPort = positive(GradientPort.class);
    Positive<CroupierPort> croupierPort = positive(CroupierPort.class);

    private ApplicationSelf self;
    private GradientConfiguration config;
    private Random random;

    private boolean leader;
    private DecoratedAddress leaderAddress;
    private PublicKey leaderPublicKey;
    String compName;

    private TreeSet<PeerDescriptor> gradientEntrySet;
    private SimpleUtilityComparator utilityComparator;

    private IndexEntry indexEntryToAdd;
    private UUID addIndexEntryRequestTimeoutId;
    final private HashSet<PeerDescriptor> queriedNodes = new HashSet<PeerDescriptor>();

    final private HashMap<UUID, PeerDescriptor> openRequests = new HashMap<UUID, PeerDescriptor>();
    final private HashMap<BasicAddress, Pair<DecoratedAddress, Integer>> locatedLeaders = new HashMap<BasicAddress, Pair<DecoratedAddress, Integer>>();
    private List<BasicAddress> leadersAlreadyComunicated = new ArrayList<BasicAddress>();


    // Routing Table Update Information.
    private RoutingTableHandler routingTableHandler;
    private Comparator<RoutingTableContainer> invertedAgeComparator;

    public Routing(RoutingInit init) {

        doInit(init);
        subscribe(handleStart, control);
        subscribe(handleLeaderLookupRequest, networkPort);
        subscribe(handleLeaderLookupResponse, networkPort);
        subscribe(handleLeaderUpdate, leaderStatusPort);
        subscribe(handleAddIndexEntryRequest, gradientRoutingPort);

        // New Leader update protocol.
        subscribe(electedAsLeaderHandler, electionPort);
        subscribe(terminateBeingLeaderHandler, electionPort);
        subscribe(leaderUpdateHandler, electionPort);

        subscribe(handleIndexHashExchangeRequest, gradientRoutingPort);
        subscribe(handleLeaderLookupTimeout, timerPort);
        subscribe(handleFailureDetector, fdPort);
        subscribe(handlerControlMessageExchangeInitiation, gradientRoutingPort);

        subscribe(handlerControlMessageInternalRequest, gradientRoutingPort);
        subscribe(handlerSelfChanged, selfChangedPort);
        subscribe(gradientSampleHandler, gradientPort);
        subscribe(croupierSampleHandler, croupierPort);

        subscribe(searchRequestHandler, gradientRoutingPort);
    }

    /**
     * Initialize the state of the component.
     */
    private void doInit(RoutingInit init) {

        self = init.getSelf().shallowCopy();
        config = init.getConfiguration();
        random = new Random(init.getSeed());

        leader = false;
        leaderAddress = null;

        compName = "(" + self.getId() + ", " + self.getOverlayId() + ") ";
        utilityComparator = new SimpleUtilityComparator();
        gradientEntrySet = new TreeSet<PeerDescriptor>(utilityComparator);


        this.routingTableHandler = new RoutingTableHandler(config.getMaxNumRoutingEntries());
        this.invertedAgeComparator = new ComparatorCollection.InvertedAgeComparator();
    }

    public Handler<Start> handleStart = new Handler<Start>() {

        @Override
        public void handle(Start e) {
            logger.info("Pseudo Gradient Component Started ...");
        }
    };


    final Handler<FailureDetectorPort.FailureDetectorEvent> handleFailureDetector = new Handler<FailureDetectorPort.FailureDetectorEvent>() {

        @Override
        public void handle(FailureDetectorPort.FailureDetectorEvent event) {
            logger.debug("Need to implement this functionality");
        }
    };



    /**
     * Helper Method to test the instance type of entries in a list.
     * If match is found, then process the entry by adding to result list.
     *
     * @param baseList   List to append entries to.
     * @param sampleList List to iterate over.
     */
    private void checkInstanceAndAdd(Collection<PeerDescriptor> baseList, Collection<Container> sampleList) {

        for (Container container : sampleList) {

            if (container.getContent() instanceof PeerDescriptor) {

                PeerDescriptor currentDescriptor = (PeerDescriptor) container.getContent();
                baseList.add(currentDescriptor);
            }
        }
    }


    /**
     * Clear the parameters associated with the index entry addition and leader
     * look up mechanism part of the protocol.
     */
    private void clearLookupParameters(){
        
        queriedNodes.clear();
        openRequests.clear();
        leadersAlreadyComunicated.clear();
        locatedLeaders.clear();
    }
    

    /**
     * Received an add entry request event from the application which requires the gradient
     * component to identify the parameters of the index entry to be added and take appropriate action.
     */
    
    final Handler<GradientRoutingPort.AddIndexEntryRequest> handleAddIndexEntryRequest = new Handler<GradientRoutingPort.AddIndexEntryRequest>() {
        @Override
        public void handle(GradientRoutingPort.AddIndexEntryRequest event) {

            MsConfig.Categories selfCategory = categoryFromCategoryId(self.getCategoryId());
            MsConfig.Categories addCategory = event.getEntry().getCategory();

            indexEntryToAdd = event.getEntry();
            addIndexEntryRequestTimeoutId = event.getTimeoutId();       // At a time client can add only one index entry in the system.
            clearLookupParameters();


            // If the node is from the same category.
            if (addCategory == selfCategory) {
                
                // If self is the leader.
                if (leader) {
                    
                    logger.debug ("Triggering entry addition request to self.");
                    DecoratedHeader<DecoratedAddress> header = new DecoratedHeader<DecoratedAddress>(self.getAddress(), self.getAddress(), Transport.UDP);
                    AddIndexEntry.Request request = new AddIndexEntry.Request(event.getTimeoutId(), event.getEntry());
                    
                    trigger(CommonHelper.getDecoratedContentMsg(header, request), networkPort);
                }

                // If we have direct pointer to the leader.
                else if (leaderAddress != null) {
                    
                    logger.debug ("Triggering the entry request to leader: {}", leaderAddress);
                    DecoratedHeader<DecoratedAddress> header = new DecoratedHeader<DecoratedAddress>(self.getAddress(), leaderAddress, Transport.UDP);
                    AddIndexEntry.Request request = new AddIndexEntry.Request(event.getTimeoutId(), event.getEntry());

                    trigger(CommonHelper.getDecoratedContentMsg(header, request), networkPort);
                }

                // Ask nodes above me for the leader pointer. ( Fix this. )
                else {

                    logger.debug ("Triggering the entry request to nodes above for finding the leader ... ", leaderAddress);
                    NavigableSet<PeerDescriptor> startNodes = new TreeSet<PeerDescriptor>(utilityComparator);
                    startNodes.addAll(getGradientSample());

                    Iterator<PeerDescriptor> iterator = startNodes.descendingIterator();
                    for (int i = 0; i < LeaderLookup.QueryLimit && iterator.hasNext(); i++) {
                        PeerDescriptor node = iterator.next();
                        sendLeaderLookupRequest(node);
                    }
                }
            }
            // In case the request is to add entry for a different category.
            else {
                Map<Integer, Pair<Integer, HashMap<BasicAddress, RoutingTableContainer>>> partitions = routingTableHandler.getCategoryRoutingMap(addCategory);
                
                if (partitions == null || partitions.isEmpty()) {
                    logger.info("{} handleAddIndexEntryRequest: no partition for category {} ", self.getAddress(), addCategory);
                    return;
                }

                ArrayList<Integer> categoryPartitionsIds = new ArrayList<Integer>(partitions.keySet());
                int categoryPartitionId = (int) (Math.random() * categoryPartitionsIds.size());
                
                HashSet<PeerDescriptor> startNodes = getSearchDescriptorSet(partitions.get(categoryPartitionsIds.get(categoryPartitionId)).getValue1().values());
                if (startNodes == null || startNodes.isEmpty()) {
                    logger.info("{} handleAddIndexEntryRequest: no nodes for partition {} ", self.getAddress(), categoryPartitionsIds.get(categoryPartitionId));
                    return;
                }

                // Need to sort it every time because values like RTT might have been changed
                SortedSet<PeerDescriptor> sortedStartNodes = sortByConnectivity(startNodes);
                Iterator iterator = sortedStartNodes.iterator();

                for (int i = 0; i < LeaderLookup.QueryLimit && iterator.hasNext(); i++) {
                    PeerDescriptor node = (PeerDescriptor) iterator.next();
                    sendLeaderLookupRequest(node);
                }
            }
        }
    };

    /**
     * Helper method to construct a set of the search descriptor collection from the
     * @param cpvCollection collection
     * @return Set of Descriptors.
     */
    private HashSet<PeerDescriptor> getSearchDescriptorSet(Collection<RoutingTableContainer> cpvCollection){
     
        HashSet<PeerDescriptor> descriptorSet = new HashSet<PeerDescriptor>();
        for(RoutingTableContainer container : cpvCollection){
            descriptorSet.add(container.getContent());
        }
        
        return descriptorSet;
    }


    /**
     * Leader lookup request timed out. Check if the request is still open and then 
     * in order to prevent memory leak, remove the request from the open request list.
     */
    final Handler<LeaderLookup.Timeout> handleLeaderLookupTimeout = new Handler<LeaderLookup.Timeout>() {
        @Override
        public void handle(LeaderLookup.Timeout timeout) {
            
            logger.debug("{}: Leader lookup timeout triggered.", self.getId());
            PeerDescriptor descriptor = openRequests.remove(timeout.getTimeoutId());
            
            if(descriptor != null) {
                logger.debug("{}: Node with Id: {} unresponsive in replying for leader response.", self.getId(), descriptor.getId());
            }
        }
    };


    /**
     * Handler for the leader lookup request from a peer in the system.
     * If leader let the peer know about you being the leader, elese point to the nearest nodes to leader 
     * above in the gradient.
     * 
     */
    ClassMatchedHandler<LeaderLookup.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, LeaderLookup.Request>> handleLeaderLookupRequest = new ClassMatchedHandler<LeaderLookup.Request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, LeaderLookup.Request>>() {
        @Override
        public void handle(LeaderLookup.Request request, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, LeaderLookup.Request> event) {
            
            logger.debug("{}: Received leader lookup request from : {}", self.getId(), event.getSource().getId());
            
            TreeSet<PeerDescriptor> higherNodes = new TreeSet<PeerDescriptor>(getHigherUtilityNodes());
            ArrayList<PeerDescriptor> searchDescriptors = new ArrayList<PeerDescriptor>();

            Iterator<PeerDescriptor> iterator = higherNodes.descendingIterator();
            while (searchDescriptors.size() < LeaderLookup.ResponseLimit && iterator.hasNext()) {
                searchDescriptors.add(iterator.next());
            }

            if (searchDescriptors.size() < LeaderLookup.ResponseLimit) {

                TreeSet<PeerDescriptor> lowerNodes = new TreeSet<PeerDescriptor>(getLowerUtilityNodes());
                iterator = lowerNodes.iterator();
                while (searchDescriptors.size() < LeaderLookupMessage.ResponseLimit && iterator.hasNext()) {
                    searchDescriptors.add(iterator.next());
                }
            }
            
            LeaderLookup.Response response = new LeaderLookup.Response(request.getLeaderLookupRound(), leader, searchDescriptors);
            trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), event.getSource(), Transport.UDP, response), networkPort);
        }
    };


    /**
     * Handler for the leader lookup response from the nodes in the system. 
     * Capture the response and then analyze that the responses point to which leader in the system.
     * If initial criteria gets satisfied and the quorum is reached, then send request to the leader for the entry.
     *
     */
    ClassMatchedHandler<LeaderLookup.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, LeaderLookup.Response>> handleLeaderLookupResponse = new ClassMatchedHandler<LeaderLookup.Response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, LeaderLookup.Response>>() {
        @Override
        public void handle(LeaderLookup.Response response, BasicContentMsg<DecoratedAddress, DecoratedHeader<DecoratedAddress>, LeaderLookup.Response> event) {
            logger.debug("{}: Received leader lookup response from the node: {} ", self.getId(), event.getSource());
            
            if(!openRequests.containsKey(response.getLeaderLookupRound())) {
                logger.warn("Look up request timed out.");
                return;
            }

            openRequests.remove(response.getLeaderLookupRound());
            se.sics.kompics.timer.CancelTimeout cancelTimeout = new se.sics.kompics.timer.CancelTimeout(response.getLeaderLookupRound());
            trigger(cancelTimeout, timerPort);
            
            if(response.isLeader()) {
                
                DecoratedAddress source = event.getSource();
                Integer numberOfAnswers;
                if (locatedLeaders.containsKey(source)) {
                    numberOfAnswers = locatedLeaders.get(event.getSource().getBase()).getValue1() + 1;
                } else {
                    numberOfAnswers = 1;
                }
                locatedLeaders.put(event.getSource().getBase(), Pair.with(event.getSource(),numberOfAnswers));
            }

            else {
                List<PeerDescriptor> higherUtilityNodes = response.getSearchDescriptors();

                if (higherUtilityNodes.size() > LeaderLookup.QueryLimit) {
                    Collections.sort(higherUtilityNodes, utilityComparator);
                    Collections.reverse(higherUtilityNodes);
                }

                if (higherUtilityNodes.size() > 0) {
                    PeerDescriptor first = higherUtilityNodes.get(0);
                    if (locatedLeaders.containsKey(first.getVodAddress())) {
                        Integer numberOfAnswers = locatedLeaders.get(first.getVodAddress().getBase()).getValue1() + 1;
                        locatedLeaders.put(first.getVodAddress().getBase(), Pair.with(first.getVodAddress(),numberOfAnswers));
                    }
                }

                Iterator<PeerDescriptor> iterator = higherUtilityNodes.iterator();
                for (int i = 0; i < LeaderLookupMessage.QueryLimit && iterator.hasNext(); i++) {
                    PeerDescriptor node = iterator.next();
                    // Don't query nodes twice
                    if (queriedNodes.contains(node)) {
                        i--;
                        continue;
                    }
                    sendLeaderLookupRequest(node);
                }
            }

            // Check it a quorum was reached
            for (BasicAddress locatedLeader : locatedLeaders.keySet()) {
                
                if (locatedLeaders.get(locatedLeader).getValue1() > LeaderLookupMessage.QueryLimit / 2) {
                    if (!leadersAlreadyComunicated.contains(locatedLeader)) {
                        
                        AddIndexEntry.Request entryAddRequest = new AddIndexEntry.Request(addIndexEntryRequestTimeoutId, indexEntryToAdd);
                        trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), locatedLeaders.get(locatedLeader).getValue0(), Transport.UDP, entryAddRequest), networkPort);
                        leadersAlreadyComunicated.add(locatedLeader);
                    }
                }
            }

        }
    };

    /**
     * Relaying of the look up request. I am not leader or doesn't know anyone therefore I route the lookup request,
     * higher in the gradient in hope of other nodes knowing the information.
     *
     * @param node Peer
     */
    private void sendLeaderLookupRequest(PeerDescriptor node) {
        
        ScheduleTimeout st = new ScheduleTimeout(config.getLeaderLookupTimeout());
        st.setTimeoutEvent(new LeaderLookup.Timeout(st));
        UUID leaderLookupRoundId = st.getTimeoutEvent().getTimeoutId();
        
        openRequests.put(leaderLookupRoundId, node);
        queriedNodes.add(node);
        
        LeaderLookup.Request request = new LeaderLookup.Request(leaderLookupRoundId);
        trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), node.getVodAddress(), Transport.UDP, request), networkPort);
        trigger(st, timerPort);
        
    }

    /**
     * Index Exchange mechanism requires the information of the higher utility nodes,
     * which have high chances of having the data as they are already above in the gradient.
     */
    final Handler<GradientRoutingPort.IndexHashExchangeRequest> handleIndexHashExchangeRequest = new Handler<GradientRoutingPort.IndexHashExchangeRequest>() {
        @Override
        public void handle(GradientRoutingPort.IndexHashExchangeRequest event) {

            logger.debug("{}: Request for initiating index hash exchange with round Id: {}", self.getId(), event.getTimeoutId());

            ArrayList<PeerDescriptor> nodes = new ArrayList<PeerDescriptor>(getHigherUtilityNodes());
            if (nodes.isEmpty() || nodes.size() < event.getNumberOfRequests()) {
                logger.debug(" {}: Not enough nodes to perform Index Hash Exchange.", self.getAddress().getId());
                return;
            }

            IndexHashExchange.Request request = new IndexHashExchange.Request(event.getTimeoutId(), event.getLowestMissingIndexEntry(), event.getExistingEntries(), self.getOverlayId());
            for (int i = 0; i < event.getNumberOfRequests(); i++) {
                int n = random.nextInt(nodes.size());
                PeerDescriptor node = nodes.get(n);
                nodes.remove(node);
                logger.debug("{}: Sending exchange request to :{} with round {}", new Object[]{ self.getId(), node.getId(), event.getTimeoutId()});
                trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), node.getVodAddress(), Transport.UDP, request), networkPort);
            }

        }
    };

    /**
     * During searching for text, request sent to look into the routing table and
     * fetch the nodes from the neighbouring partitions and also from other categories.
     */
    final Handler<GradientRoutingPort.SearchRequest> searchRequestHandler = new Handler<GradientRoutingPort.SearchRequest>() {
        @Override
        public void handle(GradientRoutingPort.SearchRequest event) {

            MsConfig.Categories category = event.getPattern().getCategory();
            Map<Integer, Pair<Integer, HashMap<BasicAddress, RoutingTableContainer>>> categoryRoutingMap = routingTableHandler.getCategoryRoutingMap(category);

            int parallelism = event.getFanoutParameter() != null ? event.getFanoutParameter() : config.getSearchParallelism();
            logger.warn("Updated the fanout parameter to: {}", parallelism);

            if (categoryRoutingMap == null) {
                logger.warn("Unable to locate nodes for the category :{}, from the local routing table", category);
                return;
            }
            trigger(new NumberOfPartitions(event.getTimeoutId(), categoryRoutingMap.keySet().size()), gradientRoutingPort);

            for (Integer partition : categoryRoutingMap.keySet()) {
                if (partition == self.getPartitionId()
                        && category == categoryFromCategoryId(self.getCategoryId())) {

                    SearchQuery.Request searchQuery = new SearchQuery.Request(event.getTimeoutId(), partition, event.getPattern());
                    trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), self.getAddress(), Transport.UDP, searchQuery), networkPort);
                    continue;
                }

                Collection<RoutingTableContainer> bucket = sortCollection(categoryRoutingMap.get(partition).getValue1().values(), invertedAgeComparator);
                Iterator<RoutingTableContainer> iterator = bucket.iterator();
                for (int i = 0; i < parallelism && iterator.hasNext(); i++) {

                    RoutingTableContainer container = iterator.next();
                    PeerDescriptor searchDescriptor = container.getContent();

                    SearchQuery.Request request = new SearchQuery.Request(event.getTimeoutId(), partition, event.getPattern());
                    trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), container.getSource(), Transport.UDP, request), networkPort);
                    searchDescriptor.setConnected(true);
                }
            }
        }
    };

    private MsConfig.Categories categoryFromCategoryId(int categoryId) {
        return MsConfig.Categories.values()[categoryId];
    }

    /**
     * Need to sort it every time because values like MsSelfImpl.RTT might have been changed
     *
     * @param searchDescriptors Descriptors
     * @return Sorted Set.
     */
    private TreeSet<PeerDescriptor> sortByConnectivity(Collection<PeerDescriptor> searchDescriptors) {
        return new TreeSet<PeerDescriptor>(searchDescriptors);
    }

    /**
     * Generic method used to return a sorted list.
     * @param collection Any Collection of samples.
     * @param comparator Comparator for sorting.
     * @param <E> Collection Type
     *
     * @return Sorted Collection
     */
    private <E> Collection<E> sortCollection(Collection<E> collection, Comparator<E> comparator){

        List<E> list = new ArrayList<E>();
        list.addAll(collection);
        Collections.sort(list, comparator);

        return list;
    }

    // Control Message Exchange Code.
    /**
     * Received the command to initiate the pull based control message exchange mechanism.
     */
    Handler<GradientRoutingPort.InitiateControlMessageExchangeRound> handlerControlMessageExchangeInitiation = new Handler<GradientRoutingPort.InitiateControlMessageExchangeRound>() {
        @Override
        public void handle(GradientRoutingPort.InitiateControlMessageExchangeRound event) {

            ArrayList<PeerDescriptor> preferredNodes = new ArrayList<PeerDescriptor>(getHigherUtilityNodes());

            if (preferredNodes.size() < event.getControlMessageExchangeNumber()){
                logger.debug("{}: Not enough higher nodes to start the control pull mechanism.", self.getId());
                return;
            }

            ControlInformation.Request request = new ControlInformation.Request(event.getRoundId(), new OverlayId(self.getOverlayId()));

            Collections.reverse(preferredNodes); // Talking to highest nodes for faster fetch of data.
            Iterator<PeerDescriptor> iterator = preferredNodes.iterator();
            
            for (int i = 0; i < event.getControlMessageExchangeNumber() && iterator.hasNext(); i++) {
                DecoratedAddress destination = iterator.next().getVodAddress();
                trigger(CommonHelper.getDecoratedContentMessage(self.getAddress(), destination, Transport.UDP, request), networkPort);
            }
        }
    };


    Handler<LeaderInfoUpdate> handleLeaderUpdate = new Handler<LeaderInfoUpdate>() {
        @Override
        public void handle(LeaderInfoUpdate leaderInfoUpdate) {
            
            logger.debug("{}: Received the leader address through pull with address:{} ", self.getId(), leaderInfoUpdate.getLeaderAddress());
            leaderAddress = leaderInfoUpdate.getLeaderAddress();
            leaderPublicKey = leaderInfoUpdate.getLeaderPublicKey();
        }
    };

    /**
     * Request received as part of control message pull mechanism initiated by the nodes in the system.
     * The main component requests control message information from this component.
     * <br/>
     * <b>NOTE: </b> The component can receive multiple requests asking for different control information.
     */
    Handler<ControlMessageInternal.Request> handlerControlMessageInternalRequest = new Handler<ControlMessageInternal.Request>() {
        @Override
        public void handle(ControlMessageInternal.Request event) {

            if (event instanceof CheckLeaderInfoUpdate.Request)
                handleCheckLeaderInfoInternalControlMessage((CheckLeaderInfoUpdate.Request) event);
        }
    };

    /**
     * Check for the leader information that the component contains.
     * @param event Leader Info Event.
     */
    private void handleCheckLeaderInfoInternalControlMessage(CheckLeaderInfoUpdate.Request event) {

        logger.debug("Check Leader Update Received.");

        trigger(new CheckLeaderInfoUpdate.Response(event.getRoundId(), event.getSourceAddress(),
                leader ? self.getAddress() : leaderAddress, leaderPublicKey), gradientRoutingPort);
    }

    Handler<SelfChangedPort.SelfChangedEvent> handlerSelfChanged = new Handler<SelfChangedPort.SelfChangedEvent>() {
        @Override
        public void handle(SelfChangedPort.SelfChangedEvent event) {
            self = event.getSelf().shallowCopy();
        }
    };


    /**
     * Croupier used to supply information regarding the <b>nodes in other partitions</b>,
     * incorporate the sample in the <b>routing table</b>.
     */
    Handler<CroupierSample<PeerDescriptor>> croupierSampleHandler = new Handler<CroupierSample<PeerDescriptor>>() {
        @Override
        public void handle(CroupierSample<PeerDescriptor> event) {
            logger.trace("{}: Pseudo Gradient Received Croupier Sample", self.getId());

            if (event.publicSample.isEmpty())
                logger.info("{}: Pseudo Gradient Received Empty Sample: " + self.getId());

            Collection<Container> rawCroupierSample = new ArrayList<Container>();
            rawCroupierSample.addAll(event.publicSample);
            rawCroupierSample.addAll(event.privateSample);

            routingTableHandler.addEntriesToRoutingTable(rawCroupierSample);
            routingTableHandler.incrementRoutingTableDescriptorAges();

            publishRoutingTable();
        }
    };


    Handler<GradientSample> gradientSampleHandler = new Handler<GradientSample>() {
        @Override
        public void handle(GradientSample event) {

            logger.debug("{}: Received gradient sample", self.getId());
            gradientEntrySet.clear();
            checkInstanceAndAdd(gradientEntrySet, event.gradientSample);
            performAdditionalHouseKeepingTasks();

        }
    };


    /**
     * After every sample merge, perform some additional tasks
     * in a <b>predefined order</b>.
     *
     */
    private void performAdditionalHouseKeepingTasks() {

        publishSample();

    }


    private void publishSample() {

        Set<PeerDescriptor> nodes = getGradientSample();
        StringBuilder sb = new StringBuilder("Neighbours: { ");
        
        for (PeerDescriptor d : nodes) {
            
            sb.append(d.getVodAddress().getId() + ":" 
                    + d.getNumberOfIndexEntries() + ":" 
                    + d.getPartitioningDepth() + ":" 
                    + d.isLeaderGroupMember()).append(" , ");
            
        }
        
        sb.append("}");
        logger.debug(compName + sb);
    }

    /**
     * Fetch the current gradient sample.
     *
     * @return Most Recent Gradient Sample.
     */
    private SortedSet<PeerDescriptor> getGradientSample() {
        return gradientEntrySet;
    }

    /**
     * Get the nodes which have the higher utility from the node.
     *
     * @return The Sorted Set.
     */
    private SortedSet<PeerDescriptor> getHigherUtilityNodes() {
        return gradientEntrySet.tailSet(self.getSelfDescriptor());
    }

    /**
     * Get the nodes which have lower utility as compared to node.
     *
     * @return Lower Utility Nodes.
     */
    private SortedSet<PeerDescriptor> getLowerUtilityNodes() {
        return gradientEntrySet.headSet(self.getSelfDescriptor());
    }

    private void publishRoutingTable() {

        for (Map<Integer, Pair<Integer, HashMap<BasicAddress, RoutingTableContainer>>> categoryMap : routingTableHandler.values()) {

            for (Map.Entry<Integer, Pair<Integer, HashMap<BasicAddress, RoutingTableContainer>>> bucket : categoryMap.entrySet()) {

                Pair<Integer, HashMap<BasicAddress, RoutingTableContainer>> depthBucket = bucket.getValue();

                for (BasicAddress addr : depthBucket.getValue1().keySet()) {
                    logger.debug(" Updated RoutingTable: PartitionId: {} PartitionDepth: {}  NodeId: {}", new Object[]{bucket.getKey(), depthBucket.getValue0(), addr.getId()});
                }
            }
        }
    }
    
    
    // ==== LEADER ELECTION PROTOCOL HANDLERS.

    /**
     * The node is being elected as the leader in the partition.
     * Update the information locally in order to reflect the change.
     */
    Handler<LeaderState.ElectedAsLeader> electedAsLeaderHandler = new Handler<LeaderState.ElectedAsLeader>() {
        @Override
        public void handle(LeaderState.ElectedAsLeader event) {
            logger.debug("{}: Node elected as leader", self.getId());
            leader = true;
        }
    };

    /**
     * The node is no longer the leader and therefore the information needs to be removed.
     */
    Handler<LeaderState.TerminateBeingLeader> terminateBeingLeaderHandler = new Handler<LeaderState.TerminateBeingLeader>() {
        @Override
        public void handle(LeaderState.TerminateBeingLeader event) {
            logger.debug("{}: Terminate being leader", self.getId());
            leader = false;
        }
    };

    /**
     * Once the leader gets elected, the component signals the information about the current
     * leader through an indication event.
     */
    Handler<LeaderUpdate> leaderUpdateHandler = new Handler<LeaderUpdate>() {
        @Override
        public void handle(LeaderUpdate leaderUpdate) {

            logger.debug("{}: Information About the current leader received. {}", self.getId(), leaderUpdate.leaderAddress);
            
            leaderAddress = leaderUpdate.leaderAddress;
            leaderPublicKey = leaderUpdate.leaderPublicKey;
        }
    };
    
}


