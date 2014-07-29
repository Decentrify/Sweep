package se.sics.ms.main;

import se.sics.gvod.config.Configuration;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.scenarios.Scenario;
import se.sics.ms.scenarios.Scenario2;
import se.sics.ms.simulator.SearchSimulationMain;

import java.io.IOException;

/**
 * The prototype can be started by executing the main function in this file.
 * There are currently two scenarios which can be executed. Scenario 1 simply
 * adds some nodes. Scenario 2 also adds entries and a second bunch of nodes
 * later. Scenario 3 introduces a leader crash. Scenario 4 introduces churn.
 * Scenario 5 was implemented to test our system with real world data from The
 * Pirate Bay. It uses Magnet links from an xml file (available on [5]) to add
 * them to the system. The file needs to be located in the search/resources
 * folder of the source code and need to be named poor3.xml. The path can be
 * adjusted by editing the AddMagnetEntry handler in
 * search.simulator.core.SearchSimulator. Please be aware that the xml file
 * might include invalid characters which lead to a
 * crashing xml parser. We used the linux command line instructions below to
 * clean up the file. Because of time constraints, scenario 2 was not as well
 * tested as scenario 1 and errors might still occur. Executing the scenarios
 * with wrong configurations might lead to no entries being added. For example
 * if the configuration is set to require 5 nodes to acknowledge a new entry but
 * less than 5 nodes per bucket are available. Please look at the documentation
 * of the configuration files if you change scenarios.
 * <p/>
 * sed -i ‘s/&/&amp;/g’ INPUT_FILE CHARS=$(python -c 'print
 * u"\u0016\u000e".encode("utf8")') sed 's/['"$CHARS"']//g' < INPUT_FILE >
 * OUTPUT_FILE
 * <p/>
 * Searching and adding can be manually executed when the system is running
 * using the HTTP GET requests http://127.0.1.1:9999/node_id/search-KEYWORDS and
 * http://127.0.1.1:9999/node_id/add-KEYWORD_STRING-MAGNET_LINK. Keywords can be
 * separated by spaces.
 */
public class Main {

    /**
     * Starts the execution of the program
     *
     * @param args the command line arguments
     * @throws IOException in case the configuration file couldn't be created
     */
    public static void main(String[] args) throws IOException {
        MsConfig.init(args);
        Configuration config = new Configuration();
        config.store();

        Scenario scenario = new Scenario2();
        scenario.setSeed(MsConfig.getSeed());
        scenario.getScenario().simulateThreaded(SearchSimulationMain.class);
    }
}
