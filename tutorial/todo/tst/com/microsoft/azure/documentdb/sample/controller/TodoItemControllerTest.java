package com.microsoft.azure.documentdb.sample.controller;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.microsoft.azure.documentdb.sample.dao.DocDbDao;
import com.microsoft.azure.documentdb.sample.model.TodoItem;

public class TodoItemControllerTest {
    private static final String anyTodoItemName = "myName";
    private static final String anyTodoItemCategory = "myCategory";
    private static final boolean anyTodoItemComplete = true;

    private TodoItemController todoItemController;

    @Before
    public void setup() {
        todoItemController = new TodoItemController(new DocDbDao());
    }

    @Test
    public void testCreateTodoItem() {
        // Count how many items are exist in the datastore.
        List<TodoItem> listOfTodoItems = todoItemController.getTodoItems();
        int numberOfExistingItems = listOfTodoItems.size();

        // Create a random new item.
        Random randy = new Random();
        int randomInt = randy.nextInt();

        String testTodoItemId = todoItemController.createTodoItem(
                anyTodoItemName + randomInt, anyTodoItemCategory + randomInt,
                randomInt % 2 == 0).getId();

        // Check one of the randomly created items.
        TodoItem testTodoItem = todoItemController
                .getTodoItemById(testTodoItemId);

        assertNotNull(testTodoItem);
        assertEquals(anyTodoItemName + randomInt, testTodoItem.getName());
        assertEquals(anyTodoItemCategory + randomInt,
                testTodoItem.getCategory());
        assertEquals(randomInt % 2 == 0, testTodoItem.isComplete());

        // Check that the total item makes sense.
        listOfTodoItems = todoItemController.getTodoItems();
        assertEquals(numberOfExistingItems + 1, listOfTodoItems.size());
    }

    @Test
    public void testDeleteTodoItem() {
        // Create an item to delete.
        String testTodoItemId = todoItemController.createTodoItem(
                anyTodoItemName, anyTodoItemCategory, anyTodoItemComplete)
                .getId();

        // Sanity Check
        TodoItem actualTodoItem = todoItemController
                .getTodoItemById(testTodoItemId);
        assertNotNull(actualTodoItem);

        // Delete the item.
        todoItemController.deleteTodoItem(testTodoItemId);

        // Check to see if the item was deleted
        actualTodoItem = todoItemController.getTodoItemById(testTodoItemId);
        assertNull(actualTodoItem);
    }

    @Test
    public void testUpdateTodoItem() {
        // Create an item to update.
        boolean oldValue = anyTodoItemComplete;
        TodoItem testTodoItem = todoItemController.createTodoItem(
                anyTodoItemName, anyTodoItemCategory, oldValue);

        // Sanity Check
        boolean actualValue = todoItemController.getTodoItemById(
                testTodoItem.getId()).isComplete();
        assertEquals(oldValue, actualValue);

        // Update whether the item is complete.
        boolean newValue = !oldValue;
        todoItemController.updateTodoItem(testTodoItem.getId(), newValue);

        // Check to see if the value was updated.
        actualValue = todoItemController.getTodoItemById(testTodoItem.getId())
                .isComplete();
        assertEquals(newValue, actualValue);
    }
}
