import java.io.*;
import java.util.*;
import java.text.*;
import java.math.*;
import java.sql.*;
class problem
{
public static void main(String gg[])
{
try
{
File file=new File("ramu.kaka");
if(file.exists()==false) return;
RandomAccessFile randomAccessFile;
randomAccessFile=new RandomAccessFile(file,"rw");
if(randomAccessFile.length()==0)
{
randomAccessFile.close();
return;
}
try
{
Class.forName("com.mysql.cj.jdbc.Driver");
Connection connection=DriverManager.getConnection("jdbc:mysql://localhost:3306/incubyteDB","root","mundra");
Statement statement=connection.createStatement();
statement.executeUpdate("drop table hospital;");
statement.executeUpdate("create table hospital(Name Varchar(255) primary key,Cust_I Varchar(18) not null,Open_Dt Date not null,Consul_Dt Date not null,VAC_ID char(5),DR_Name char(255), State char(5),Country char(5),DOB Date, FLAG char(1));");
statement.close();
PreparedStatement preparedStatement=null;
ResultSet resultSet=null;

String header=randomAccessFile.readLine();
String column_name[]=header.split("\\|");
String patient_data[];
SimpleDateFormat simpleDateFormatO;
simpleDateFormatO=new SimpleDateFormat("yyyyMMdd");
SimpleDateFormat simpleDateFormatC;
simpleDateFormatC=new SimpleDateFormat("yyyyMMdd");
SimpleDateFormat simpleDateFormatD;
simpleDateFormatD=new SimpleDateFormat("ddMMyyyy");
String name="";
String customer_id="";
java.util.Date open_date_util=null;
java.util.Date consl_date_util=null;
java.sql.Date open_date_sql=null;
java.sql.Date consl_date_sql=null;
java.util.Date dob_util=null;
java.sql.Date dob_sql=null;
String dob_str="";
String consl_date_str="";
String open_date_str="";
String vac_id="";
String dr_name="";
String state="";
String country="";
String active="";
String p="";

while(randomAccessFile.getFilePointer()<randomAccessFile.length())
{
p=randomAccessFile.readLine();
patient_data=p.split("\\|");
name=patient_data[2];
customer_id=patient_data[3];
try
{
open_date_util=new java.util.Date();
open_date_util=simpleDateFormatO.parse(patient_data[4]);
open_date_str=simpleDateFormatO.format(open_date_util);
open_date_sql=new java.sql.Date(open_date_util.getYear(),open_date_util.getMonth(),open_date_util.getDate());
}catch(ParseException pe)
{
}
try
{
consl_date_util=new java.util.Date();
consl_date_util=simpleDateFormatC.parse(patient_data[5]);
consl_date_str=simpleDateFormatC.format(consl_date_util);
consl_date_sql=new java.sql.Date(consl_date_util.getYear(),consl_date_util.getMonth(),consl_date_util.getDate());
}catch(ParseException pe)
{
}
vac_id=patient_data[6];
dr_name=patient_data[7];
state=patient_data[8];
country=patient_data[9];
try
{
dob_util=new java.util.Date();
dob_util=simpleDateFormatD.parse(patient_data[10]);
dob_str=simpleDateFormatD.format(dob_util);
dob_sql=new java.sql.Date(dob_util.getYear(),dob_util.getMonth(),dob_util.getDate());
}catch(ParseException pe)
{
}
active=patient_data[11];
/*
System.out.println("Name :"+name);
System.out.println("Customer id "+customer_id);
System.out.println("Open date : "+open_date_str);
System.out.println("Cons date : "+consl_date_str);
System.out.println("Vac id : "+vac_id);
System.out.println("Dr Name : "+dr_name);
System.out.println("State : "+state);
System.out.println("Country :"+country);
System.out.println("DOB : "+dob_str);
System.out.println("flag :"+active);
System.out.println("****************************************************************************");
*/
// insert the data into table :
preparedStatement=connection.prepareStatement("insert into hospital(Name,Cust_I,Open_Dt,Consul_Dt,VAC_ID,DR_Name,State,Country,DOB,FLAG) values(?,?,?,?,?,?,?,?,?,?)");
preparedStatement.setString(1,name);
preparedStatement.setString(2,customer_id);
preparedStatement.setDate(3,open_date_sql);
preparedStatement.setDate(4,consl_date_sql);
preparedStatement.setString(5,vac_id);
preparedStatement.setString(6,dr_name);
preparedStatement.setString(7,state);
preparedStatement.setString(8,country);
preparedStatement.setDate(9,dob_sql);
preparedStatement.setString(10,active);
preparedStatement.executeUpdate();
preparedStatement.close();
} // loop ends

statement=connection.createStatement();
resultSet=statement.executeQuery("select distinct Country from hospital;");
String country_name="";
Statement i_statement=null;
PreparedStatement i_preparedStatement=null;
ResultSet rs=null;
while(resultSet.next())
{
country_name=resultSet.getString("Country");
i_statement=connection.createStatement();
i_statement.executeUpdate("create table "+"Table_"+country_name+"(Name Varchar(255) primary key,Cust_I Varchar(18) not null,Open_Dt Date not null,Consul_Dt Date not null,VAC_ID char(5),DR_Name char(255), State char(5),Country char(5),DOB Date, FLAG char(1));");
preparedStatement=connection.prepareStatement("select * from hospital where Country= ?;");
preparedStatement.setString(1,country_name);
rs=preparedStatement.executeQuery();
while(rs.next())
{
i_preparedStatement=connection.prepareStatement("insert into "+"Table_"+country_name+"(Name,Cust_I,Open_Dt,Consul_Dt,VAC_ID,DR_Name,State,Country,DOB,FLAG) values(?,?,?,?,?,?,?,?,?,?);");
i_preparedStatement.setString(1,rs.getString("Name"));
i_preparedStatement.setString(2,rs.getString("Cust_I"));
i_preparedStatement.setDate(3,rs.getDate("Open_Dt"));
i_preparedStatement.setDate(4,rs.getDate("Consul_Dt"));
i_preparedStatement.setString(5,rs.getString("VAC_ID"));
i_preparedStatement.setString(6,rs.getString("DR_Name"));
i_preparedStatement.setString(7,rs.getString("State"));
i_preparedStatement.setString(8,rs.getString("Country"));
i_preparedStatement.setDate(9,rs.getDate("DOB"));
i_preparedStatement.setString(10,rs.getString("FLAG"));
i_preparedStatement.executeUpdate();
i_preparedStatement.close();
}
rs.close();
i_statement.close();
preparedStatement.close();
}
resultSet.close();
randomAccessFile.close();
statement.close();
connection.close();
System.out.println("Done");
}catch(SQLException sqlException)
{
System.out.println(sqlException.getMessage());
}
catch(ClassNotFoundException cnfe)
{
System.out.println(cnfe.getMessage());
}
}catch(IOException ioException)
{
System.out.println(ioException.getMessage());
}
}
}