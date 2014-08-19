/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.sics.ms.webservice;

import se.sics.kompics.Init;

/**
 *
 * @author alidar
 */
public class SearchWebServiceMiddlewareInit extends Init<SearchWebServiceMiddleware> {

    private SearchDelegate searchDelegate;

    public SearchWebServiceMiddlewareInit(SearchDelegate searchDelegate) {
        this.searchDelegate = searchDelegate;
    }

    /**
     * @return the searchDelegate
     */
    public SearchDelegate getSearchDelegate() {
        return searchDelegate;
    }

    /**
     * @param searchDelegate the searchDelegate to set
     */
    public void setSearchDelegate(SearchDelegate searchDelegate) {
        this.searchDelegate = searchDelegate;
    }

}
