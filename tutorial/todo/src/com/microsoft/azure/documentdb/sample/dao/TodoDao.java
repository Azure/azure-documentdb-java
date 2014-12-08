package com.microsoft.azure.documentdb.sample.dao;

import java.util.List;

import com.microsoft.azure.documentdb.sample.model.TodoItem;

public interface TodoDao {
    /**
     * @return A list of TodoItems
     */
    public List<TodoItem> readTodoItems();

    /**
     * @param todoItem
     * @return whether the todoItem was persisted.
     */
    public TodoItem createTodoItem(TodoItem todoItem);

    /**
     * @param id
     * @return the TodoItem
     */
    public TodoItem readTodoItem(String id);

    /**
     * @param id
     * @return the TodoItem
     */
    public TodoItem updateTodoItem(String id, boolean isComplete);

    /**
     *
     * @param id
     * @return whether the delete was successful.
     */
    public boolean deleteTodoItem(String id);
}
