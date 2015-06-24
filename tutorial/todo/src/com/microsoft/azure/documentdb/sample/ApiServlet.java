package com.microsoft.azure.documentdb.sample;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;
import com.microsoft.azure.documentdb.sample.controller.TodoItemController;

/**
 * API Frontend Servlet
 */
@WebServlet("/api")
public class ApiServlet extends HttpServlet {
	// API Keys
	public static final String API_METHOD = "method";

	// API Methods
	public static final String CREATE_TODO_ITEM = "createTodoItem";
	public static final String GET_TODO_ITEMS = "getTodoItems";
	public static final String UPDATE_TODO_ITEM = "updateTodoItem";

	// API Parameters
	public static final String TODO_ITEM_ID = "todoItemId";
	public static final String TODO_ITEM_NAME = "todoItemName";
	public static final String TODO_ITEM_CATEGORY = "todoItemCategory";
	public static final String TODO_ITEM_COMPLETE = "todoItemComplete";

	public static final String MESSAGE_ERROR_INVALID_METHOD = "{'error': 'Invalid method'}";

	private static final long serialVersionUID = 1L;
	private static final Gson gson = new Gson();

	@Override
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {

		String apiResponse = MESSAGE_ERROR_INVALID_METHOD;

		TodoItemController todoItemController = TodoItemController
				.getInstance();

		String id = request.getParameter(TODO_ITEM_ID);
		String name = request.getParameter(TODO_ITEM_NAME);
		String category = request.getParameter(TODO_ITEM_CATEGORY);
		boolean isComplete = StringUtils.equalsIgnoreCase("true",
				request.getParameter(TODO_ITEM_COMPLETE)) ? true : false;

		switch (request.getParameter(API_METHOD)) {
		case CREATE_TODO_ITEM:
			apiResponse = gson.toJson(todoItemController.createTodoItem(name,
					category, isComplete));
			break;
		case GET_TODO_ITEMS:
			apiResponse = gson.toJson(todoItemController.getTodoItems());
			break;
		case UPDATE_TODO_ITEM:
			apiResponse = gson.toJson(todoItemController.updateTodoItem(id,
					isComplete));
			break;
		default:
			break;
		}

		response.setCharacterEncoding("UTF-8");
		response.getWriter().println(apiResponse);
	}

	@Override
	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}
}
