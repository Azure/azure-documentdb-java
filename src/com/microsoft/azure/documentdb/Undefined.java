package com.microsoft.azure.documentdb;

/**
 * Represents the 'Undfined' partition key. 
 */
public class Undefined {
	
	private final static Undefined value = new Undefined();
	
	/**
     * Constructor. Create a new instance of the Undefined object.
    */
    private Undefined() {
    }
    
    /**
     * Returns the singleton value of Undfined.
    */
    public static Undefined Value() {
    	return value;
    }

    /**
     * Returns the string representation of Undfined.
    */
    public String toString() {
    	return "{}";
    }
}
