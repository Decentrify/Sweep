//package se.sics.ms.simulator;
//
//import se.sics.gvod.config.GradientConfiguration;
//import se.sics.gvod.config.SearchConfiguration;
//import se.sics.kompics.Init;
//import se.sics.p2ptoolbox.chunkmanager.ChunkManagerConfig;
//import se.sics.p2ptoolbox.croupier.CroupierConfig;
//import se.sics.p2ptoolbox.election.core.ElectionConfig;
//import se.sics.p2ptoolbox.gradient.GradientConfig;
//import se.sics.p2ptoolbox.tgradient.TreeGradientConfig;
//
//public final class SearchSimulatorInit extends Init<SearchSimulator> {
//	private final CroupierConfig croupierConfiguration;
//	private final GradientConfiguration gradientConfiguration;
//	private final SearchConfiguration searchConfiguration;
//    private final ChunkManagerConfig chunkManagerConfiguration;
//    private final ElectionConfig electionConfig;
//
//    private final GradientConfig gradientConfig;
//    private final TreeGradientConfig treeGradientConfig;
//
//	public SearchSimulatorInit(CroupierConfig croupierConfiguration, GradientConfiguration gradientConfiguration,
//                               SearchConfiguration aggregationConfiguration,
//                               ChunkManagerConfig chunkManagerConfiguration, GradientConfig gradientConfig, ElectionConfig electionConfig, TreeGradientConfig treeGradientConfig) {
//		super();
//		this.croupierConfiguration = croupierConfiguration;
//		this.gradientConfiguration = gradientConfiguration;
//		this.searchConfiguration = aggregationConfiguration;
//        this.chunkManagerConfiguration = chunkManagerConfiguration;
//        this.gradientConfig = gradientConfig;
//        this.electionConfig = electionConfig;
//        this.treeGradientConfig = treeGradientConfig;
//	}
//
//    public TreeGradientConfig getTreeGradientConfig() {
//        return treeGradientConfig;
//    }
//
//    public SearchConfiguration getSearchConfiguration() {
//		return searchConfiguration;
//	}
//
//	public CroupierConfig getCroupierConfiguration() {
//		return this.croupierConfiguration;
//	}
//
//	public GradientConfiguration getGradientConfiguration() {
//		return this.gradientConfiguration;
//	}
//
//    public ChunkManagerConfig getChunkManagerConfiguration() {
//        return chunkManagerConfiguration;
//    }
//
//    public GradientConfig getGradientConfig() {
//        return gradientConfig;
//    }
//
//    public ElectionConfig getElectionConfig(){
//        return this.electionConfig;
//    }
//}
