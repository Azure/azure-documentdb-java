package com.globalstrongdemo;

public class Program {

    public static void main(String[] args) {
        
        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "OFF");
        
        if (args.length != 6) {
            System.out.println("Usage: producerEndpoint producerKey consumerEndpoint consumerKey producer/consumer/both currentLocation");
            System.out.println("Please specify whether to run \"producer\", \"consumer\", or \"both\", as well as current location");
            return;
        }

        switch (args[4]) {
            case "producer":

                new Thread(new Producer(args[0], args[1], args[2], args[3], args[5])).start();
                break;
            case "consumer":

                new Thread(new Consumer(args[0], args[1], args[2], args[3], args[5])).start();
                break;
            case "both":

                new Thread(new Producer(args[0], args[1], args[2], args[3], args[5])).start();
                new Thread(new Consumer(args[0], args[1], args[2], args[3], args[5])).start();
                break;
            default:
                System.out.println("Please specify whether to run \"producer\", \"consumer\", or \"both\", as well as current location.");
                return;
        }

    }

}
