package Database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import Database.*;

public class Main {

	
	
	
	public static void main(String[] args) throws Exception {
		getConnection();
		
	}

	



	public static Connection getConnection() throws Exception{
		Connection connection = null;
		try {
			String driver ="com.mysql.cj.jdbc.Driver";
			String url = "jdbc:mysql://localhost:3306/digitalcredx?autoReconnect=true&useSSL=false";
			String username = "root";
			String password = "Vivek@1986";
			
			String filePath = "E:\\DigitalCredentialXpress\\Data";
			
			Class.forName(driver);
			connection = DriverManager.getConnection(url, username, password);
			System.out.println("Connected");
			ReadDataFromExcel read = new ReadDataFromExcel();
            List<String> lines = read.readCSV();
            System.out.println("total lines from csv :"+lines.size());
            String sql = "Insert IGNORE into temporarydata(Institution_Name,Mobile_Number_1,Mobile_Number_2,Email_Id_1,Email_Id_2,Address_1,Address_2,Address_3,City,State,Postal_Code,Country) values(?,?,?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement ps = null;
            for(Integer i = 1 ; i<lines.size() ; i++){
				String line = lines.get(i);
            	if (line != null) 
            		
				{System.out.println(line);
					String[] fields = line.split(",");
					System.out.println("Length"+fields.length);
					Integer rowIndex = 1;
					 ps = connection.prepareStatement(sql);
					for(String fieldValue:fields)
					{
						if(fieldValue == null || fieldValue == "" || fieldValue == " " || fieldValue.isEmpty() || fieldValue.length() == 0 ) {
							System.out.println("Row index IN IF "+rowIndex);
							ps.setString(rowIndex,null);
							rowIndex++;
							continue;
					 	}
					    System.out.println("Row index"+rowIndex+": Result "+fieldValue);
						ps.setString(rowIndex,fieldValue);
						rowIndex++;

					}
					ps.executeUpdate();			
					
				}
			}
            
            String query = "SELECT Institution_Name, Mobile_Number_1, Mobile_Number_2, Email_Id_1, Email_Id_2 FROM temporarydata";

            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery(query);
            ArrayList al = null;
            al = new ArrayList();

            PreparedStatement ps1 = connection.prepareStatement("INSERT IGNORE INTO contact (Full_Name, Mobile_Number_1,Mobile_Number_2,Email_Id_1,Email_Id_2) values(?,?,?,?,?)");
            List lstInstituteNames = new ArrayList();
            while (rs.next()) {
            	String Full_Name = rs.getString("Institution_Name");
            	//System.out.println("Full_Name"+Full_Name);
            	String Mobile_Number_1 = rs.getString("Mobile_Number_1");
            	//System.out.println("Mobile_Number_1"+Mobile_Number_1);
            	String Mobile_Number_2 = rs.getString("Mobile_Number_2");
            	//System.out.println("Mobile_Number_2"+Mobile_Number_2);
            	String Email_Id_1 = rs.getString("Email_Id_1");
            	//System.out.println("Email_Id_1"+Email_Id_1);
            	String Email_Id_2 = rs.getString("Email_Id_2");
            	//System.out.println("Email_Id_2"+Email_Id_2);

            	if(Mobile_Number_1 == null && Mobile_Number_2 == null && Email_Id_1 == null && Email_Id_2 == null ){
            		lstInstituteNames.add(Full_Name);
            		continue;
            	}
            		
                ps1.setString(1, Full_Name);
                ps1.setString(2, Mobile_Number_1);
                ps1.setString(3, Mobile_Number_2);
                ps1.setString(4, Email_Id_1);
                ps1.setString(5, Email_Id_2);
                int status = ps1.executeUpdate();
                //System.out.println("status"+status);
                 }
            System.out.println("lstInstituteNames"+lstInstituteNames);
            String query1 = "SELECT Institution_Name, Mobile_Number_1, Mobile_Number_2, Email_Id_1, Email_Id_2, Address_1, Address_2, Address_3, City, State, Postal_Code, Country FROM temporarydata";

            Statement st1 = connection.createStatement();
            ResultSet rs1 = st1.executeQuery(query1);
            ArrayList al1 = null;
            al1 = new ArrayList();

            PreparedStatement ps2 = connection.prepareStatement("INSERT IGNORE INTO contact_address ( Address_1, Address_2, Address_3, City, State, Postal_Code, Country, Contact_Id) values(?,?,?,?,?,?,?,?)");
            PreparedStatement ps3 = connection.prepareStatement("INSERT IGNORE INTO institution ( Contact_Id , Institution_Name) values(?,?)");
            PreparedStatement ps4 = connection.prepareStatement("INSERT IGNORE INTO error_table ( Institution_Name, Mobile_Number_1, Mobile_Number_2, Email_Id_1, Email_Id_2, Address_1, Address_2, Address_3, City, State, Postal_Code, Country) values(?,?,?,?,?,?,?,?,?,?,?,?)");
            
            while (rs1.next()) {
            	String Address_1 = rs1.getString("Address_1");
            	System.out.println("Address_1"+Address_1);
            	String Address_2 = rs1.getString("Address_2");
            	System.out.println("Address_2"+Address_2);
            	String Address_3 = rs1.getString("Address_3");
            	System.out.println("Address_3"+Address_3);
            	String City = rs1.getString("City");
            	System.out.println("City"+City);
            	String State = rs1.getString("State");
            	System.out.println("State"+State);
            	String Postal_Code = rs1.getString("Postal_Code");
            	System.out.println("Postal_Code"+Postal_Code);
            	String Country = rs1.getString("Country");
            	System.out.println("Country"+Country);
            	
            	String Institution_Name = rs1.getString("Institution_Name");
            	System.out.println("Institution_Name"+Institution_Name);
            	String Mobile_Number_1 = rs1.getString("Mobile_Number_1");
            	System.out.println("Mobile_Number_1"+Mobile_Number_1);
            	String Mobile_Number_2 = rs1.getString("Mobile_Number_2");
            	System.out.println("Mobile_Number_2"+Mobile_Number_2);
            	String Email_Id_1 = rs1.getString("Email_Id_1");
            	System.out.println("Email_Id_1"+Email_Id_1);
            	String Email_Id_2 = rs1.getString("Email_Id_2");
            	System.out.println("Email_Id_2"+Email_Id_2);
            	
            	
            	
            	String sqlForType = "SELECT Contact_Id FROM contact WHERE Full_Name =?";
            	PreparedStatement prepareStmt = connection.prepareStatement(sqlForType);
            	prepareStmt.setString(1, Institution_Name);
            	ResultSet rset = prepareStmt.executeQuery();
            	Integer contactId = null;
            	
            	while (rset.next()) {contactId = rset.getInt("Contact_Id");}
            	System.out.println("contactId====>"+contactId);
            	if(contactId == null) {
            		
            		ps4.setString(1, Institution_Name);
            		ps4.setString(2, Mobile_Number_1);
            		ps4.setString(3, Mobile_Number_2);
            		ps4.setString(4, Email_Id_1);
            		ps4.setString(5, Email_Id_2);
            		ps4.setString(6, Address_1);
            		ps4.setString(7, Address_2);
            		ps4.setString(8, Address_3);
            		ps4.setString(9, City);
            		ps4.setString(10, State);
            		ps4.setString(11, Postal_Code);
            		ps4.setString(12, Country);
            		
                    
                    int status1 = ps4.executeUpdate();
            		System.out.println("status1"+status1);
            		
            		
            		
            		// add all statment here
            		
            		continue;
            	}
                ps2.setString(1, Address_1);
                ps2.setString(2, Address_2);
                ps2.setString(3, Address_3);
                ps2.setString(4, City);
                ps2.setString(5, State);
                ps2.setString(6, Postal_Code);
                ps2.setString(7, Country);
                ps2.setInt(8, contactId);
                
                int status = ps2.executeUpdate();
                System.out.println("status"+status);
                
                ps3.setInt(1,contactId);
                ps3.setString(2,Institution_Name);
                ps3.executeUpdate();
                
                String deletequery = "delete from temporarydata";
                PreparedStatement ps5 = connection.prepareStatement(deletequery); 
                ps5.executeUpdate();
                
                
            	
            }
            
          
            
            ps.close();
			} catch (Exception e) {
					//System.out.println(e.)
					System.out.println(e);
			}
		return connection;
	}
	

}
