import static org.apache.spark.sql.functions.col;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;



public class App1 {
	private static final String jsonDataFilePath ="employees-tp8-sparksql_200.json";
	private static final String csvDataFilePath ="employees-tp8-sparksql_200.csv";

	static void runAppDataFrameJson() {
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
	}
	static void runAppDataFrameCSV() {
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


	}
	static void runAppDataSets() {
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
		/*
		ds.printSchema();
		// Encoders
		ds.filter(
				(FilterFunction<Employee> e-> e.getAge() >= 30 && e.getAge() < 32)).show()
		 */
	}
}
