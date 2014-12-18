/**
 * ToDo App
 */

var todoApp = {
  /*
   * API methods to call Java backend.
   */
  apiEndpoint: "api",

  createTodoItem: function(name, category, isComplete) {
    $.post(todoApp.apiEndpoint, {
        "method": "createTodoItem",
        "todoItemName": name,
        "todoItemCategory": category,
        "todoItemComplete": isComplete
      },
      function(data) {
        var todoItem = data;
        todoApp.addTodoItemToTable(todoItem.id, todoItem.name, todoItem.category, todoItem.complete);
      },
      "json");
  },

  getTodoItems: function() {
    $.post(todoApp.apiEndpoint, {
        "method": "getTodoItems"
      },
      function(data) {
        var todoItemArr = data;
        $.each(todoItemArr, function(index, value) {
          todoApp.addTodoItemToTable(value.id, value.name, value.category, value.complete);
        });
      },
      "json");
  },

  updateTodoItem: function(id, isComplete) {
    $.post(todoApp.apiEndpoint, {
        "method": "updateTodoItem",
        "todoItemId": id,
        "todoItemComplete": isComplete
      },
      function(data) {},
      "json");
  },

  /*
   * UI Methods
   */
  addTodoItemToTable: function(id, name, category, isComplete) {
    var rowColor = isComplete ? "active" : "warning";

    todoApp.ui_table().append($("<tr>")
      .append($("<td>").text(name))
      .append($("<td>").text(category))
      .append($("<td>")
        .append($("<input>")
          .attr("type", "checkbox")
          .attr("id", id)
          .attr("checked", isComplete)
          .attr("class", "isComplete")
        ))
      .addClass(rowColor)
    );
  },

  /*
   * UI Bindings
   */
  bindCreateButton: function() {
    todoApp.ui_createButton().click(function() {
      todoApp.createTodoItem(todoApp.ui_createNameInput().val(), todoApp.ui_createCategoryInput().val(), false);
      todoApp.ui_createNameInput().val("");
      todoApp.ui_createCategoryInput().val("");
    });
  },

  bindUpdateButton: function() {
    todoApp.ui_updateButton().click(function() {
      // Disable button temporarily.
      var myButton = $(this);
      var originalText = myButton.text();
      $(this).text("Updating...");
      $(this).prop("disabled", true);

      // Call api to update todo items.
      $.each(todoApp.ui_updateId(), function(index, value) {
        todoApp.updateTodoItem(value.name, value.value);
        $(value).remove();
      });

      // Re-enable button.
      setTimeout(function() {
        myButton.prop("disabled", false);
        myButton.text(originalText);
      }, 500);
    });
  },

  bindUpdateCheckboxes: function() {
    todoApp.ui_table().on("click", ".isComplete", function(event) {
      var checkboxElement = $(event.currentTarget);
      var rowElement = $(event.currentTarget).parents('tr');
      var id = checkboxElement.attr('id');
      var isComplete = checkboxElement.is(':checked');

      // Togle table row color
      if (isComplete) {
        rowElement.addClass("active");
        rowElement.removeClass("warning");
      } else {
        rowElement.removeClass("active");
        rowElement.addClass("warning");
      }

      // Update hidden inputs for update panel.
      todoApp.ui_updateForm().children("input[name='" + id + "']").remove();

      todoApp.ui_updateForm().append($("<input>")
        .attr("type", "hidden")
        .attr("class", "updateComplete")
        .attr("name", id)
        .attr("value", isComplete));

    });
  },

  /*
   * UI Elements
   */
  ui_createNameInput: function() {
    return $(".todoForm #inputItemName");
  },

  ui_createCategoryInput: function() {
    return $(".todoForm #inputItemCategory");
  },

  ui_createButton: function() {
    return $(".todoForm button");
  },

  ui_table: function() {
    return $(".todoList table tbody");
  },

  ui_updateButton: function() {
    return $(".todoUpdatePanel button");
  },

  ui_updateForm: function() {
    return $(".todoUpdatePanel form");
  },

  ui_updateId: function() {
    return $(".todoUpdatePanel .updateComplete");
  },

  /*
   * Install the TodoApp
   */
  install: function() {
    todoApp.bindCreateButton();
    todoApp.bindUpdateButton();
    todoApp.bindUpdateCheckboxes();

    todoApp.getTodoItems();
  }
};

$(document).ready(function() {
  todoApp.install();
});
