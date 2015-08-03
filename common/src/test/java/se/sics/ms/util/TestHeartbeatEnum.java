/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.sics.ms.util;

import org.junit.Test;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TestHeartbeatEnum {
    @Test
    public void test() {
        System.out.println(HeartbeatServiceEnum.CROUPIER.getServiceId());
        HeartbeatServiceEnum.CROUPIER.setServiceId((byte)11);
        System.out.println(HeartbeatServiceEnum.CROUPIER.getServiceId());
    }
}
