/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.sics.ms.web;

import io.dropwizard.Application;
import java.util.concurrent.atomic.AtomicLong;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
/**
 *
 * @author jdowling
 */
public class WebService extends Application<Configuration> {
    public static void main(String[] args) throws Exception {
        new WebService().run(new String[]{"server"});
    }

    @Override
    public void initialize(Bootstrap<Configuration> bootstrap) {
//        bootstrap.setName("dw-server"); // name must match the yaml config file
    }

    @Override
    public void run(Configuration configuration, Environment environment) {
        environment.jersey().register(new SearchResource());
    }


    @Path("/search")
    @Produces(MediaType.APPLICATION_JSON)
    public static class SearchResource {

        @GET
        public List<String> search(@QueryParam("term") String term) {
            List<String> res = new ArrayList<>();
            return res;
        }
    }
}