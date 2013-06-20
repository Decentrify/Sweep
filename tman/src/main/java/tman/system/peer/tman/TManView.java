package tman.system.peer.tman;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

import se.sics.kompics.address.Address;

import common.peer.PeerDescriptor;

/**
 * Class representing the TMan view. It selects nodes according to the
 * preference function for a given node and offers functions to find the optimal
 * exchange partners for a given node.
 */
public class TManView {
	private TreeMap<Address, PeerDescriptor> entries;
	private Address self;
	private int size;
	private Comparator<Address> closerComparator;
	private boolean converged;
	private final double convergenceSimilarity;

	/**
	 * @param self
	 *            the address of the local node
	 * @param size
	 *            the maximum size of this view
	 * @param convergenceSimilarity
	 *            the percentage of nodes allowed to change in order to be
	 *            converged
	 */
	public TManView(Address self, int size, double convergenceSimilarity) {
		this.entries = new TreeMap<Address, PeerDescriptor>();
		this.closerComparator = new Closer(self);
		this.self = self;
		this.size = size;
		this.converged = false;
		this.convergenceSimilarity = convergenceSimilarity;
	}

	/**
	 * Add a new node to the view and drop the least preferred one if the view
	 * is full.
	 * 
	 * @param address
	 *            the node to be added
	 */
	public void add(Address address) {
		entries.put(address, new PeerDescriptor(address));

		if (entries.size() > size) {
			List<Address> list = getClosestNodes(size);
			Address leastPreferred = list.get(0);
			remove(leastPreferred);
		}
	}

	/**
	 * Remove a node from the view.
	 * 
	 * @param address
	 *            the node to be removed
	 */
	public void remove(Address address) {
		entries.remove(address);
	}

	/**
	 * Return the node with the oldest age.
	 * 
	 * @return the address of the node with the oldest age
	 */
	public Address selectPeerToShuffleWith() {
		if (entries.isEmpty()) {
			return null;
		}

		incrementDescriptorAges();
		PeerDescriptor oldestEntry = Collections.max(entries.values());

		return oldestEntry.getAddress();
	}

	/**
	 * Merge a collection of nodes in the view and drop the least preferred
	 * nodes if the size limit is reached.
	 * 
	 * @param addresses
	 *            the nodes to be merged
	 */
	public void merge(Collection<Address> addresses) {
		Collection<Address> old = new ArrayList<Address>(entries.keySet());
		int oldSize = old.size();

		for (Address address : addresses) {
			add(address);
		}

		old.retainAll(entries.keySet());
		if (oldSize == entries.size() && old.size() > convergenceSimilarity * entries.size()) {
			converged = true;
		} else {
			converged = false;
		}
	}

	/**
	 * Return the number most preferred nodes for the given address.
	 * 
	 * @param address
	 *            the address to compare with
	 * @param number
	 *            the maximum number of entries to return
	 * @return a collection of the most preferred nodes
	 */
	public Collection<Address> getExchangeNodes(Address address, int number) {
		List<Address> list = getClosestNodes(address, number);
		list.add(self);
		Collections.sort(list, new Closer(address));
		list.remove(address);
		return list.subList(0, number < list.size() ? number : list.size());
	}

	/**
	 * @return all nodes with a higher preference value than self
	 */
	public ArrayList<Address> getHigherNodes() {
		return new ArrayList<Address>(entries.headMap(self).keySet());
	}

	/**
	 * @return all nodes with a lower preference value than self
	 */
	public ArrayList<Address> getLowerNodes() {
		return new ArrayList<Address>(entries.tailMap(self).keySet());
	}

	/**
	 * @return a list of all entries in the view
	 */
	public ArrayList<Address> getAll() {
		return new ArrayList<Address>(entries.keySet());
	}

	/**
	 * @return true if the view is empty
	 */
	public boolean isEmpty() {
		return entries.isEmpty();
	}

	/**
	 * @return the size of the view
	 */
	public int getSize() {
		return entries.size();
	}

	/**
	 * @return true if the convergence criteria are reached for this view
	 */
	public boolean isConverged() {
		return converged;
	}

	/**
	 * @return true if the size limit was reached
	 */
	public boolean isFull() {
		return this.size <= entries.size();
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for (Address node : entries.keySet()) {
			builder.append(node.getId() + " ");
		}
		return builder.toString();
	};
	
	/**
	 * Increment the age of all descriptors in the view
	 */
	private void incrementDescriptorAges() {
		for (PeerDescriptor descriptor : entries.values()) {
			descriptor.incrementAndGetAge();
		}
	}

	/**
	 * Compare nodes according to their utility. Nodes with smaller IDs but
	 * closer to the base are the best once. Closer nodes are preferred to nodes
	 * further away.
	 */
	private class Closer implements Comparator<Address> {
		private Address base;

		public Closer(Address base) {
			super();
			this.base = base;
		}

		@Override
		public int compare(Address o1, Address o2) {
			assert (o1.getId() == o2.getId());

			if (o1.getId() < base.getId() && o2.getId() > base.getId()) {
				return 1;
			} else if (o1.getId() < base.getId() && o2.getId() < base.getId()
					&& o1.getId() > o2.getId()) {
				return 1;
			} else if (o1.getId() > base.getId() && o2.getId() > base.getId()
					&& o1.getId() < o2.getId()) {
				return 1;
			}
			return -1;
		}
	}
	
	/**
	 * Get a sorted list of the nodes that are the closest to self.
	 * 
	 * @param number
	 *            the maximum number of nodes to return
	 * @return the list of the closest nodes to self
	 */
	private List<Address> getClosestNodes(int number) {
		return getClosestNodes(self, number, closerComparator);
	}

	/**
	 * Get a sorted list of the nodes that are the closest to the given address.
	 * 
	 * @param address
	 *            the address to compare with
	 * @param number
	 *            the maximum number of nodes to return
	 * @return the list of the closest nodes to the given address
	 */
	private List<Address> getClosestNodes(Address address, int number) {
		return getClosestNodes(address, number, new Closer(address));
	}

	/**
	 * Get a sorted list of the nodes that are the closest to the given address.
	 * 
	 * @param address
	 *            the address to compare with
	 * @param number
	 *            the maximum number of nodes to return
	 * @param c
	 *            the comparator to use
	 * @return the list of the closest nodes to the given address
	 */
	private List<Address> getClosestNodes(Address address, int number, Comparator<Address> c) {
		ArrayList<Address> addresses = getAll();
		Collections.sort(addresses, new Closer(address));
		return addresses.subList(0, number < addresses.size() ? number : addresses.size());
	}
}
