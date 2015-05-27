package search.system.peer.util;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.ms.types.BaseLeaderUnit;
import se.sics.ms.types.LeaderUnit;
import se.sics.ms.util.TimeLine;

/**
 * Testing various scenarios as part of the
 *
 * Created by babbarshaer on 2015-05-26.
 */
public class TimeLineTest {

    private static TimeLine timeLine;
    private static final long INITIAL_EPOCH_ID = 0;
    private static final int DEFAULT_LEADER = 100;

    @BeforeClass
    public static void beforeClass(){
        timeLine = new TimeLine();
    }
    
    @After
    public void afterTest(){
        timeLine.cleanInternalState();
    }
    
    
    @Test
    public void testNextInOrder(){

        LeaderUnit defaultLeaderUnit = new BaseLeaderUnit(INITIAL_EPOCH_ID, DEFAULT_LEADER);
        timeLine.addLeaderUnit(defaultLeaderUnit);
        
        defaultLeaderUnit = new BaseLeaderUnit(INITIAL_EPOCH_ID, DEFAULT_LEADER, 100);
        timeLine.addLeaderUnit(defaultLeaderUnit);
        
        defaultLeaderUnit = timeLine.markUnitComplete(defaultLeaderUnit);
        
        LeaderUnit nextLeaderUnit = new BaseLeaderUnit(INITIAL_EPOCH_ID + 1, DEFAULT_LEADER);
        timeLine.addLeaderUnit(nextLeaderUnit);
        
        LeaderUnit result = timeLine.getNextUnitToTrack(defaultLeaderUnit);
        Assert.assertEquals("Next Entry Track check", nextLeaderUnit, result);
    }
    
}
