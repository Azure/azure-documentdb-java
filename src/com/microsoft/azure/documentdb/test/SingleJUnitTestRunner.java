package com.microsoft.azure.documentdb.test;

import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;

public class SingleJUnitTestRunner {
    public static void main(String... args) throws ClassNotFoundException {
        // Run the specified test case
    	String packageName = SingleJUnitTestRunner.class.getPackage().getName();
        String[] classAndMethod = args[0].split("#");
        String className = packageName + "." + classAndMethod[0];
        Request request = Request.method(Class.forName(className), classAndMethod[1]);
        Result result = new JUnitCore().run(request);
        
        // Prints error message and trace if the test fails
        if(!result.wasSuccessful())
        {
            System.out.println("Test failed: " + result.getFailures().get(0).getMessage());
            System.out.println(result.getFailures().get(0).getTrace());
        }
        
        System.exit(result.wasSuccessful() ? 0 : 1);
    }
}
