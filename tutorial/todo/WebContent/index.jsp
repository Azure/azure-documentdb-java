<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <meta http-equiv="X-UA-Compatible" content="IE=edge;" />
  <title>Azure DocumentDB Java Sample</title>

  <!-- Bootstrap -->
  <link href="//ajax.aspnetcdn.com/ajax/bootstrap/3.2.0/css/bootstrap.min.css" rel="stylesheet">

  <style>
    /* Add padding to body for fixed nav bar */
    body {
      padding-top: 50px;
    }
  </style>
</head>
<body>
  <!-- Nav Bar -->
  <div class="navbar navbar-inverse navbar-fixed-top" role="navigation">
    <div class="container">
      <div class="navbar-header">
        <a class="navbar-brand" href="#">My Tasks</a>
      </div>
    </div>
  </div>

  <!-- Body -->
  <div class="container">
    <h1>My ToDo List</h1>

    <hr/>

    <!-- The ToDo List -->
    <div class = "todoList">
      <table class="table table-bordered table-striped" id="todoItems">
        <thead>
          <tr>
            <th>Name</th>
            <th>Category</th>
            <th>Complete</th>
          </tr>
        </thead>
        <tbody>
        </tbody>
      </table>

      <!-- Update Button -->
      <div class="todoUpdatePanel">
        <form class="form-horizontal" role="form">
          <button type="button" class="btn btn-primary">Update Tasks</button>
        </form>
      </div>

    </div>

    <hr/>

    <!-- Item Input Form -->
    <div class="todoForm">
      <form class="form-horizontal" role="form">
        <div class="form-group">
          <label for="inputItemName" class="col-sm-2">Task Name</label>
          <div class="col-sm-10">
            <input type="text" class="form-control" id="inputItemName" placeholder="Enter name">
          </div>
        </div>

        <div class="form-group">
          <label for="inputItemCategory" class="col-sm-2">Task Category</label>
          <div class="col-sm-10">
            <input type="text" class="form-control" id="inputItemCategory" placeholder="Enter category">
          </div>
        </div>

        <button type="button" class="btn btn-primary">Add Task</button>
      </form>
    </div>

  </div>

  <!-- Placed at the end of the document so the pages load faster -->
  <script src="//ajax.aspnetcdn.com/ajax/jQuery/jquery-2.1.1.min.js"></script>
  <script src="//ajax.aspnetcdn.com/ajax/bootstrap/3.2.0/bootstrap.min.js"></script>
  <script src="assets/todo.js"></script>
</body>
</html>
