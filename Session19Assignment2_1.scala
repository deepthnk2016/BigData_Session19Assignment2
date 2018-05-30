/* Session19 Assignment 2 - UDFs on Dataframe */

//Problem 1 - Use UDF to concatenate first two chararcters of firstname and lastname
import spark.implicits._

//Read the input file
val sportsdata=spark.read.csv("/home/bigdata/deepak/docs/Acadgild/Session19Assignment2/Sports_data.txt").toDF("firstname","lastname","sports","medal_type","age","year","country")

//Get the header and skip it
val header = sportsdata.first()
val sportsdataDF = sportsdata.filter(line => line != header)

//UDF to perform the concatenation
def concatFirstNameLastName(fname: String, lname: String) = "Mr."+fname.substring(0,2) +" "+ lname

//Register the UDF
val output = udf(concatFirstNameLastName(_:String,_:String))

//Call the UDF to display the result
val result=sportsdataDF.select($"firstname",$"lastname",output(sportsdataDF("firstname"),sportsdataDF("lastname")))
result.show
