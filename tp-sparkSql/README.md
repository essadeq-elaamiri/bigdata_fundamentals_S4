### TP 6 SparkSQL 
[REF Apache](https://spark.apache.org/docs/2.1.0/sql-programming-guide.html)
*Assignment*
```diff
! Using Dataframes (Datasets without typing) then, with Datasest 
Employee: 
+ id long 
+ name String  
+ phone String  
+ salary double  
+ age int 
+ departement String 
Questions: 
- Afficher les employés ayant une age entre 30 et 35  - Afficher la moyenne des salaires de chaque département 
- Afficher le nombre des salariés par departement 
- Afficher le salaire maximum de tous les départements
```
**Dependencies**
```xml
<dependencies>
	<dependency>
	    <groupId>org.apache.spark</groupId>
	    <artifactId>spark-sql_2.12</artifactId>
	    <version>3.0.1</version>
	</dependency>
	<dependency>
	    <groupId>org.apache.spark</groupId>
	    <artifactId>spark-core_2.12</artifactId>
	    <version>3.0.1</version>
	</dependency>
  </dependencies>
```



#### Using Dataframes (with data as Json)

```java
//setup session
		SparkSession sparkSession = SparkSession.builder().appName("TP8").master("local[*]").getOrCreate();

		// creating dataframe from json data
		Dataset<Row> employeesData = sparkSession.read().option("multiline", true).json(jsonDataFilePath);

		// display dataframe
		employeesData.show();

		//printing schema
		employeesData.printSchema();

		//2. show employees of 30<=age<=35
		employeesData.filter(col("age").gt(30).and(col("age").lt(35))).show();
		//3. Show the average salary of each department
		employeesData.groupBy(col("department")).avg("salary").show();
		//4.  Show the number of employees in each department
		employeesData.groupBy(col("department")).count().show();
		//5. Show the highest salary in all departments
		// Register the DataFrame as a SQL temporary view
		employeesData.createOrReplaceTempView("employees");
		sparkSession.sql("select max(salary)as max_salary from employees").show();
```

#### Using Dataframes (with data as CSV)
```java

		//setup session
		SparkSession sparkSession = SparkSession.builder().appName("TP8").master("local[*]").getOrCreate();

		// creating dataframe from json data
		Dataset<Row> employeesData = sparkSession.read().option("header", true).csv(csvDataFilePath);

		// display dataframe
		employeesData.show();

		//printing schema
		employeesData.printSchema();

		//2. show employees of 30<=age<=35 (it cast by default, I used cast just for clarification)
		employeesData.filter(col("age").cast("int").gt(30).and(col("age").cast("int").lt(35))).show();
		//3. Show the average salary of each department
		Dataset<Row> employeesData2Typed = employeesData.select(
				col("id").cast("long"),
				col("name"),
				col("phone"),
				col("salary").cast("double"),
				col("age").cast("long"),
				col("department")
				);
		employeesData2Typed.groupBy(col("department")).mean("salary").show();
		//4.  Show the number of employees in each department
		employeesData.groupBy(col("department")).count().show();
		//5. Show the highest salary in all departments
		// Register the DataFrame as a SQL temporary view
		employeesData2Typed.createOrReplaceTempView("employees");
		sparkSession.sql("select max(salary) from employees").show();


```
#### Using Dataframes (with data as XML)

#### Using Datasets
1. Employee Model
```java
import java.io.Serializable;

public class Employee implements Serializable {
	/**
	 * 
	 */
	//private static final long serialVersionUID = 1L;
	private Long id; // Long class is nullable
	private String name;
	private String phone;
	private Double salary;
	private Long age;
	private String department;

	public Employee() {
		// TODO Auto-generated constructor stub
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public double getSalary() {
		return salary;
	}

	public void setSalary(double salary) {
		this.salary = salary;
	}

	public long getAge() {
		return age;
	}

	public void setAge(long age) {
		this.age = age;
	}

	public String getdepartment() {
		return department;
	}

	public void setdepartment(String department) {
		this.department = department;
	}



}

```

2.code 
```java

		//setup session
		SparkSession sparkSession = SparkSession.builder().appName("TP8").master("local[*]").getOrCreate();

		// creating encoders: object-table mapping between the the JavaBean and Spark
		// sql entries
		Encoder<Employee> employeEncoder = Encoders.bean(Employee.class);

		// creating dataset from json data
		Dataset<Employee> employeesData = sparkSession.read().option("multiline", true).json(jsonDataFilePath).as(employeEncoder);
		// multiline to avoid _corrupt_record error
		// display dataset
		employeesData.show();

		//printing schema
		employeesData.printSchema();

		//2. show employees of 30<=age<=35 (it cast by default, I used cast just for clarification)
		employeesData.filter(new FilterFunction<Employee>() {

			public boolean call(Employee employee) throws Exception {
				// TODO Auto-generated method stub
				return employee.getAge()>30 && employee.getAge()<35;
			}
		}).show();
		//3. Show the average salary of each department
		employeesData.groupBy(col("department")).mean("salary").show();
		//4.  Show the number of employees in each department
		employeesData.groupBy(col("department")).count().show();
		//5. Show the highest salary in all departments
		// Register the DataFrame as a SQL temporary view
		employeesData.createOrReplaceTempView("employees");
		sparkSession.sql("select max(salary)as max_salary from employees").show();
		
```

#### Output
1. show employees of 30<=age<=35
2. Show the average salary of each department
3.  Show the number of employees in each department
4. Show the highest salary in all departments 
