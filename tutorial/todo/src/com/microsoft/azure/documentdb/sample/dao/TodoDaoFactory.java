package com.microsoft.azure.documentdb.sample.dao;

public class TodoDaoFactory {
    private static TodoDao myTodoDao;

    public static TodoDao getDao() {
        if (myTodoDao == null) {
            myTodoDao = new DocDbDao();
        }
        return myTodoDao;
    }
}
