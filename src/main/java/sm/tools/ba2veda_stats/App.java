package sm.tools.ba2veda_stats;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import sm.tools.veda_client.Individual;
import sm.tools.veda_client.Resources;
import sm.tools.veda_client.VedaConnection;

/**
 * Hello world!
 *
 */
public class App 
{
	static VedaConnection veda = null;
	static Connection docDbConn;
	static Connection vedaDbConn;
	
    public static void main( String[] args )
    {
    	HashMap<String, String> config = new HashMap<String, String>();
    	try {
    		BufferedReader br = new BufferedReader(new FileReader("config.conf"));
    		
    		int count = 1;
    	    for(String line; (line = br.readLine()) != null; count++) {
    	    	int idx = line.indexOf("#");
    	    	if (idx >= 0) {
    	    		line = line.substring(0, idx);
    	    	}
    	    
    	    	line = line.trim();
    	    	if (line.length() == 0)
    	    		continue;
    	    	
    	        idx = line.indexOf("=");
    	        if (idx < 0) {
    	        	System.err.println(String.format("ERR! Invalid line %d, "
    	        			+ "'=' was not found: %s", count, line));
    	        	return;
    	        }
    	        

    	        String paramName = line.substring(0, idx);
    	        String paramVal = line.substring(idx + 1);
    	        
    	        config.put(paramName, paramVal);
    	    }
    	} catch (Throwable t) {
    		System.err.println("ERR! Cannot read config file: " + t.getMessage());
    		return;
    	}
    	
    	String veda_url;

   		if (!config.containsKey("veda")) {
   			System.err.println("ERR! Config key 'veda' is not set");
   			return;
   		}
   		veda_url = config.get("veda");
    	
    	try {
    		veda = new VedaConnection(veda_url, "ImportDMSToVeda",
    			"a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3");
    	} catch (Exception e) {
    		System.err.println("ERR! Can not connect to Veda");
    		e.printStackTrace();
    		return;
    	}
    	
    	/*[2017-11-22 04:12:04.769236] 
    	 * connect_to_mysql:connection: 
    	 * cfg:conn_mysql1=[Individual("cfg:conn_mysql1", ["v-s:sql_database":[(String)veda_db], 
    	 * "v-s:password":[(String),f,ehtxyfz69], "v-s:updateCounter":[(Integer)3], 
    	 * "v-s:port":[(Integer)3306], "v-s:host":[(String)172.17.1.193], 
    	 * "v-s:login":[(String)ba], 
    	 * "rdf:type":[(Uri)v-s:Connection], 
    	 * "rdfs:isDefinedBy":[(Uri)cfg, "v-s:transport":[(String)mysql], 
    	 * "cfg:low_priority_user":[(Uri)cfg:ImportDMSToVeda], 
    	 * "rdfs:label":[(String)Connect to msql], 
    	 * "v-s:name":[(String)funout]], OK, CRC32(4294967295))]*/
    	
    	String fromClass, toClass;

   		if (!config.containsKey("from_class")) {
   			System.err.println("ERR! Config key 'from_class' is not set");
  			return;
   		}
   		fromClass = config.get("from_class");
    	
    	if (!config.containsKey("to_class")) {
    		System.err.println("ERR! Config key 'to_class' is not ser");
    		return;
    	}
    	toClass = config.get("to_class");
    	
    	String ba2vedaPath;
    	if (!config.containsKey("ba2veda_path")) {
    		System.err.println("ERR! Config key 'ba2veda_path' is not set");
    		return;
    	}
    	ba2vedaPath = config.get("ba2veda_path");
    	
    	String ba2vedaDir;
    	if (!config.containsKey("ba2veda_dir")) {
    		System.err.println("ERR! Config key 'ba2veda_path' is not set");
    		return;
    	}
    	ba2vedaDir = config.get("ba2veda_dir");
    	
    	
    	Boolean exportToVeda = false;
    	if (config.containsKey("export_to_veda")) {
    		if (config.get("export_to_veda").equals("true")) 
    			exportToVeda = true;
    	}
    	
    	Boolean exportToSql = false;
    	if (config.containsKey("export_to_sql"))
    		if (config.get("export_to_sql").equals("true"))
    			exportToSql = true;
    	
    	String docDbUser, docDbPassword, docDbUrl;

   		docDbUser = config.get("doc_db_user");
   		if (!config.containsKey("doc_db_user")) {
   			System.err.println("ERR! Config key 'doc_db_user' is not set");
   			return;
   		}
    	
   		if (!config.containsKey("doc_db_password")) {
   			System.err.println("ERR! Config key 'doc_db_password' is not set");
   			return;
   		}
   		docDbPassword = config.get("doc_db_password");
    	
    	if (!config.containsKey("doc_db_url")) {
       		System.err.println("ERR! Config key 'doc_db_url' is not set");
       		return;
     	}
   		docDbUrl = config.get("doc_db_url");

    	

    	try {
    		docDbConn = DriverManager.getConnection("jdbc:mysql://" + docDbUrl, docDbUser, docDbPassword);
    	} catch (Exception e) {
    		System.err.println("ERR! Can not connect to documents database");
    		e.printStackTrace();
    		return;
    	}
    	
    	String vedaDbUser, vedaDbPassword, vedaDbUrl;
    	if (!config.containsKey("veda_db_user")) {
    		System.err.println("ERR! Config key 'veda_db_user' is not set");
    		return;
    	}
    	vedaDbUser = config.get("veda_db_user");

    	

   		if (!config.containsKey("veda_db_password")) {
   			System.err.println("ERR! Config key 'veda_db_password' is not set");
   			return;
   		}
   		vedaDbPassword = config.get("veda_db_password");
    	
    	if (!config.containsKey("veda_db_url")) {
    		System.err.println("ERR! Config key 'veda_db_url' is not set");
    		return;
    	}
    	vedaDbUrl = config.get("veda_db_url");
    	

    	try {
    		vedaDbConn = DriverManager.getConnection("jdbc:mysql://" + vedaDbUrl, vedaDbUser, vedaDbPassword);
    		
    	} catch (Exception e) {
    		System.err.println("ERR! Can not connect to veda database");
    		e.printStackTrace();
    		return;
    	}
    	
    	Date dateFrom = null, dateTo = null;
    	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    	try {
	    	if (config.containsKey("date_from"))
	    		dateFrom = df.parse(config.get("date_from"));
	    	if (config.containsKey("date_to"))
	    		dateTo = df.parse(config.get("date_to")); 
    	} catch (Exception e) {
    		e.printStackTrace();
    		return;
    	}
    	
    	String queryStr = "SELECT recordId, objectId FROM objects WHERE isDraft = 0 AND templateId = ? "
    			+ "AND actual = 1 AND LOCATE('<active>true', LEFT(content,120))>0";
    	if (dateFrom != null && dateTo != null)
    		queryStr = "SELECT recordId, objectId FROM objects WHERE isDraft = 0 AND templateId = ? AND "
    				+ " timestamp > ? AND timestamp < ? AND actual = 1 "
    				+ "AND LOCATE('<active>true', LEFT(content,120))>0";
    	else if (dateFrom != null && dateTo == null)
    		queryStr = "SELECT recordId, objectId FROM objects WHERE isDraft = 0 AND templateId = ? AND "
    				+ "timestamp > ? AND actual = 1 AND LOCATE('<active>true', LEFT(content,120))>0";
    	else if (dateFrom == null && dateTo != null)
    		queryStr = "SELECT recordId, objectId FROM objects WHERE isDraft = 0 AND templateId = ? AND "
    				+ "timestamp < ? AND actual = 1 AND LOCATE('<active>true', LEFT(content,120))>0";
    	
    	ArrayList<String> docUris = new ArrayList<String>();
    	try {
			PreparedStatement ps = docDbConn.prepareStatement(queryStr);
			ps.setString(1, fromClass);
			if (dateFrom != null && dateTo != null) {
				ps.setLong(2, dateFrom.getTime());
				ps.setLong(3, dateTo.getTime());
			} else if (dateFrom != null && dateTo == null)
				ps.setLong(2, dateFrom.getTime());
			else if (dateFrom == null && dateTo != null)
				ps.setLong(2, dateTo.getTime());
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				String uri = rs.getString(2);
				docUris.add(uri);
			}
			
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
			return;
		}
    	
    	System.out.println(String.format("BA: %s | OF: %s", fromClass, toClass));
    	System.out.println(String.format("Number of uris: %d", docUris.size()));
    	for (int i = 0; i < docUris.size(); i++) {
    		System.out.println(String.format("Progress: [%d/%d/%s]", i+1, docUris.size(), docUris.get(i)));
    		try {
    			Individual indv = veda.getIndividual("d:" + docUris.get(i));
    			if (indv == null) {
    				System.out.println(String.format("d:%s not found in OF", docUris.get(i)));
    				if (exportToVeda) {
    					System.out.println("EXPORT TO VEDA");
    					String cmd = "java";
    					try {
    						String arg = String.format("%s/%s/%s", fromClass, toClass, docUris.get(i));
    						ProcessBuilder pb = new ProcessBuilder(cmd, "-Djava.library.path=/usr/local/lib", "-jar", 
    								"target/ba2veda.jar", arg);
    						pb.directory(new File(ba2vedaDir));
//    						p = pb.start();
    						Process ba2veda = pb.start();
    						BufferedReader errReader = new BufferedReader(new InputStreamReader(ba2veda.getErrorStream()));
    						BufferedReader outReader = new BufferedReader(new InputStreamReader(ba2veda.getInputStream()));
    						String line = null;
//    						ba2veda.wait();
    						ba2veda.waitFor();
    						System.out.println("ba2veda output: [");
    						while ((line = outReader.readLine()) != null) {
    							System.out.println("\t"+line);
    						}
    						System.out.println("]");
    						
    						System.out.println("ba2veda error output: [");
    						while ((line = errReader.readLine()) != null) {
    							System.out.println("\t"+line);
    						}
    						System.out.println("]");
    						
    					} catch (Exception e) {
    						e.printStackTrace();
    						return;
    					}
    				} 				
    				continue;
    			}
    			
    			Resources deleted = indv.getResources("v-s:deleted");
    			if (deleted != null) {
    				if (deleted.resources.get(0).getData() == "true")
    					System.out.println(String.format("d:%s deleted in OF", docUris.get(i)));
    				continue;
    			}
    			
    			String query;
    			query = "select doc_id from `rdfs:label` doc_id where doc_id=?";
    			PreparedStatement ps = vedaDbConn.prepareStatement(query);
    			ps.setString(1, "d:" + docUris.get(i));
    			ResultSet rs = ps.executeQuery();
    			
    			Boolean rdfs_label = true;
    			if (!rs.next()) {
    				System.out.println(String.format("d:%s not found in rdfs:label", docUris.get(i)));
    				rdfs_label = false;
    			}
    			rs.close();
    			ps.close();
    			
    			query = "select doc_id from `rdf:type` doc_id where doc_id=?";
    			ps = vedaDbConn.prepareStatement(query);
    			ps.setString(1, "d:" + docUris.get(i));
    			rs = ps.executeQuery();
    			
    			Boolean rdf_type = true;
    			if (!rs.next()) {
    				System.out.println(String.format("d:%s not found in rdf:type", docUris.get(i)));
    				rdf_type = false;
    			}
    			
    			if ((!rdf_type || !rdfs_label) && exportToSql) {
    				System.out.println("EXPORT TO SQL");
					String cmd = "java";
					try {
						String arg = String.format("%s/%s/%s", fromClass, toClass, docUris.get(i));
						ProcessBuilder pb = new ProcessBuilder(cmd, "-Djava.library.path=/usr/local/lib", "-jar",
								"target/ba2veda-jar-with-dependencies.jar", "-subsystem32", arg);
						pb.directory(new File(ba2vedaDir));
//						p = pb.start();
						Process ba2veda = pb.start();
						BufferedReader errReader = new BufferedReader(new InputStreamReader(ba2veda.getErrorStream()));
						BufferedReader outReader = new BufferedReader(new InputStreamReader(ba2veda.getInputStream()));
						String line = null;
//						ba2veda.wait();
						ba2veda.waitFor();
						System.out.println("ba2veda output: [");
						while ((line = outReader.readLine()) != null) {
							System.out.println("\t"+line);
						}
						System.out.println("]");
						
						System.out.println("ba2veda error output: [");
						while ((line = errReader.readLine()) != null) {
							System.out.println("\t"+line);
						}
						System.out.println("]");
						
					} catch (Exception e) {
						e.printStackTrace();
						return;
					}
    				int a = 2 + 2;
    				a = 2 + a;
    			}
    			rs.close();
    			ps.close();
    		} catch (Exception e) {
    			System.err.println("ERR! Can not get individual: d:" + docUris.get(i));
    			e.printStackTrace();
    		}
    	}
    	
    	System.out.println("DONE!");
    }
}
