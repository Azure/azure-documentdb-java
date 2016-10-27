package com.microsoft.azure.documentdb;

import java.util.ArrayList;

import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;

public class SingleJUnitTestRunner {
    public static void main(String... args) throws ClassNotFoundException {
        boolean wasSuccessful = false;
        String packageName = SingleJUnitTestRunner.class.getPackage().getName();
        String[] classAndMethod = args[0].split("#");
        String className = packageName + "." + classAndMethod[0];
        Class<?> classType = Class.forName(className);

        if (ParameterizedGatewayTestBase.class.isAssignableFrom(classType)) {
            ArrayList<Description> children = Request.aClass(classType).getRunner().getDescription().getChildren();
            for (Description child : children) {
                String testName = String.format("%s%s", classAndMethod[1], child.getDisplayName());
                wasSuccessful = runOneTest(classType, testName);
                if (!wasSuccessful) {
                    break;
                }
            }
        } else {
            String testName = classAndMethod[1];
            wasSuccessful = runOneTest(classType, testName);
        }

        System.exit(wasSuccessful ? 0 : 1);
    }

    private static boolean runOneTest(Class<?> classType, String testName) {
        System.out.println(String.format("Executing test case: %s", testName));
        Request request = Request.method(classType, testName);
        Result result = new JUnitCore().run(request);

        if (!result.wasSuccessful()) {
            System.out.println(String.format("Test %s failed: %s", testName, result.getFailures().get(0).getMessage()));
            System.out.println(result.getFailures().get(0).getTrace());
        } else {
            System.out.println(String.format("Test %s succeeded.", testName));
        }

        return result.wasSuccessful();
    }
}
