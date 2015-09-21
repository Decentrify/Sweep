package se.sics.ms.helper;

import se.sics.ktoolbox.aggregator.server.util.DesignProcessor;
import se.sics.ms.aggregator.processor.ReplicationLagDesignProcessor;

/**
 * Designer Enum used by the visualizer component
 * to match the processor with the event to process.W
 *
 * Created by babbarshaer on 2015-09-20.
 */
public enum SimDesignerEnum {

    ReplicationLagDesigner("replicationLag", new ReplicationLagDesignProcessor());

    private String name;
    private DesignProcessor processor;

    private SimDesignerEnum(String name, DesignProcessor processor){
        this.name = name;
        this.processor = processor;
    }

    public String getName() {
        return name;
    }

    public DesignProcessor getProcessor() {
        return processor;
    }

}
