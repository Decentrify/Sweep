package se.sics.ms.election;

/**
 * A simple counter class that contains an int and has an increment function
 */
public class SynchronizedCounter {
	private int counter;

	/**
	 * Default constructor that will set the counter to 0
	 */
	public SynchronizedCounter() {
		this.counter = 0;
	}

	/**
	 * Constructor that sets the counter to be equal to the in parameter
	 * 
	 * @param startingValue
	 *            the value that the counter should be initialised to
	 */
	public SynchronizedCounter(int startingValue) {
		this.counter = startingValue;
	}

	/**
	 * Getter for the counter's value
	 * 
	 * @return the value of the counter
	 */
	public int getValue() {
		return counter;
	}

	/**
	 * Sets the value of the counter to be equal to the value of the parameter
	 * 
	 * @param newValue
	 *            the value that the counter should be set to
	 */
	public synchronized void setValue(int newValue) {
		this.counter = newValue;
	}

	/**
	 * Increments the counter by one
	 */
	public synchronized void incrementValue() {
		this.counter++;
	}
}
