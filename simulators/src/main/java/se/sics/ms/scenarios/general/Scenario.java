package se.sics.ms.scenarios.general;

import java.util.Random;

public class Scenario {

	private static Random random;
	protected ThreadedSimulationScenario scenario;

	public Scenario(ThreadedSimulationScenario scenario) {
		this.scenario = scenario;
		this.scenario.setSeed(System.currentTimeMillis());
		random = scenario.getRandom();
	}

	public void setSeed(long seed) {
		this.scenario.setSeed(seed);
	}

	public static Random getRandom() {
		return random;
	}

	public static void setRandom(Random r) {
		random = r;
	}

	public ThreadedSimulationScenario getScenario() {
		return scenario;
	}
}
