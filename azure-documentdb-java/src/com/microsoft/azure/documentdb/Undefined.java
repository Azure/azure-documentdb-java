package com.microsoft.azure.documentdb;

/**
 * Represents the 'Undfined' partition key. 
 */
public class Undefined {
	
	private final static Undefined value = new Undefined();
	
	/**
     * Creates a new instance of the Undefined class.
    */
    private Undefined() {
    }
    
    /**
     * @return the singleton value of Undfined.
    */
    public static Undefined Value() {
    	return value;
    }

    /**
     * @return the string representation of Undfined.
    */
    public String toString() {
    	return "{}";
    }
}
