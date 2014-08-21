/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.sics.ms.webservice;

import se.sics.ms.types.IndexEntry;

import java.util.ArrayList;

/**
 *
 * @author alidar
 */
public interface SearchDelegate {
    
    public void didSearch(ArrayList<IndexEntry> results);
    public void didAddIndex();
    public void didFailToAddIndex();
}
