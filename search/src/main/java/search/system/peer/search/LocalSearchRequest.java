package search.system.peer.search;

import java.util.UUID;

import se.sics.kompics.web.WebRequest;

/**
 * Stores information about a currently executed search request.
 */
public class LocalSearchRequest {
	private final WebRequest webRequest;
	private final String query;
	private final UUID searchId;
	private int nodesQueried;
	private int nodesAnswered;
	private UUID timeoutId;

	/**
	 * Craete a new instance for the given request and query.
	 * 
	 * @param webRequest
	 *            the {@link WebRequest} of the issuing client
	 * @param query
	 *            the query string of the search
	 */
	public LocalSearchRequest(WebRequest webRequest, String query) {
		super();
		this.webRequest = webRequest;
		this.query = query;
		// Create a unique id for each request
		this.searchId = UUID.randomUUID();
	}

	/**
	 * @return the {@link WebRequest} of the issuing client
	 */
	public WebRequest getWebRequest() {
		return webRequest;
	}

	/**
	 * @return the query string of the search
	 */
	public String getQuery() {
		return query;
	}

	/**
	 * @return the UUID of the search request
	 */
	public UUID getSearchId() {
		return searchId;
	}

	/**
	 * @return the amount of nodes queried
	 */
	public int getNodesQueried() {
		return nodesQueried;
	}

	/**
	 * @param sent
	 *            the amount of nodes queried
	 */
	public void setNodesQueried(int sent) {
		this.nodesQueried = sent;
	}

	/**
	 * Increment the amount of nodes queried.
	 */
	public void incrementNodesQueried() {
		this.nodesQueried++;
	}

	/**
	 * @return the amount of answers received
	 */
	public int getNodesAnswered() {
		return nodesAnswered;
	}

	/**
	 * @param received
	 *            the amount of answers received
	 */
	public void setNodesAnswered(int received) {
		this.nodesAnswered = received;
	}

	/**
	 * Increment the amount of answers received.
	 */
	public void incrementReceived() {
		this.nodesAnswered++;
	}

	/**
	 * @return true if the amount of answers matches the amount of requests sent
	 */
	public boolean receivedAll() {
		return nodesQueried <= nodesAnswered;
	}

	/**
	 * @param timeoutId
	 *            the id of the timeout related to this search
	 */
	public void setTimeoutId(UUID timeoutId) {
		this.timeoutId = timeoutId;
	}

	/**
	 * @return the id of the timeout related to this search
	 */
	public UUID getTimeoutId() {
		return timeoutId;
	}
}
