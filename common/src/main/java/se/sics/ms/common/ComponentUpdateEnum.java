package se.sics.ms.common;

import se.sics.ms.data.ComponentUpdate;
import se.sics.ms.data.GradientComponentUpdate;
import se.sics.ms.data.SearchComponentUpdate;

/**
 * Enum providing information regarding the component key name 
 * and update type.
 *  
 * Created by babbarshaer on 2015-02-27.
 */

public enum ComponentUpdateEnum {
    
    SEARCH("search", SearchComponentUpdate.class),
    GRADIENT("gradient", GradientComponentUpdate.class);

    String name;
    Class<? extends ComponentUpdate> updateType;
    
    ComponentUpdateEnum(String name , Class<? extends ComponentUpdate> updateType){
        this.name = name;
        this.updateType = updateType;
    }

    public String getName() {
        return name;
    }

    public Class<? extends ComponentUpdate> getUpdateType() {
        return updateType;
    }
}
