/**
 * 
 */
package com.wbbaseballstats.yearlystats;

import java.util.List;

import com.datastax.driver.core.HostDistance;
//import com.datastax.driver.core.QueryOptions.*;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
//import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.policies.ConstantSpeculativeExecutionPolicy;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
//import com.datastax.driver.core.utils.UUIDs;
//import com.datastax.driver.core.policies.*;
//import com.datastax.spark.connector.*;
/*
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
//import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
*/
import scala.runtime.*;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;

//import java.util.concurrent.TimeUnit;
import java.text.DecimalFormat;


/**
 * @author John Walker
 *
 */
public class YearlyStats {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DseSession session = createConnection();
		createKeyspaceAndTables(session);
		//insertData(session);
		
		readData(session);
		session.close();
		System.exit(0);
	}

	private static DseSession createConnection() {
		DseCluster cluster  = null;
		
		// Load balancing strategy
		TokenAwarePolicy policy = new TokenAwarePolicy(
				DCAwareRoundRobinPolicy.builder()
				.withLocalDc("DC1")
				.withUsedHostsPerRemoteDc(2)
				.build());
		
		/**
		 * 
		 *  Pooling 
		 *  This is a good working example of how Connection Pooling Options can be configured. This is a
		 *  very simple example of how connections can be supported by a remote host.
		 *  
		 */
		PoolingOptions poolingOptions = new PoolingOptions()
				.setConnectionsPerHost(HostDistance.LOCAL, 1, 3)
				.setConnectionsPerHost(HostDistance.REMOTE, 1, 1);

		// Create cluster instances
		cluster = DseCluster.builder()
				   .addContactPoints(new String[] {"34.223.206.174"})
				   .withClusterName("Test Cluster")
				   .withLoadBalancingPolicy(policy)
				   .withPort(9042)
				   .withSpeculativeExecutionPolicy(
				     new ConstantSpeculativeExecutionPolicy(
				          500, // delay before a new execution is launched
				           2 // maximum number of executions
				      ))
				   .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM))
				   //.withCredentials(cqlConfig.db.username, cqlConfig.db.password)
				   .withPoolingOptions(poolingOptions)
				   .withSocketOptions(new SocketOptions()
				        .setConnectTimeoutMillis(5000)
				        .setReadTimeoutMillis(12000))
				   .withReconnectionPolicy(new ExponentialReconnectionPolicy(500, 300000))
				   .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
				   //.withSSL()
				   .build();
		
		
		// you can get lots of meta data, the below shows the keyspaces it can find out about
		// this is all part of the client gossip like query process
		System.out.println("The keyspaces known by Connection are: " + cluster.getMetadata().getKeyspaces().toString());
		
		// you don't have to specify a consistency level, there is always default 
		System.out.println("The Default Consistency Level is: "
				+ cluster.getConfiguration().getQueryOptions().getConsistencyLevel());
		
		// finally create a session to connect,  alternatively and what you normally will do is specify the keyspace
		// i.e. DseSession session = cluster.connect("keyspace_name");
		DseSession session = cluster.connect();
		
		return session;

	}

	private static void createKeyspaceAndTables(DseSession session) {
		// this is using a simple statement,  there are many other and better ways to execute against the cluster
		// my personal preferred method is using mappers, but since this is not about how to code my
		// examples are trying to use very simple methods
		Statement createKS = new SimpleStatement(
				"CREATE KEYSPACE IF NOT EXISTS ks_baseball WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'};");
		createKS.enableTracing();
		
		// Note the consistency level, just uses the default for the cluster object if not set on the statement
		System.out.println("The Consistency Level is: " + createKS.getConsistencyLevel());
		session.execute(createKS);

		Statement createTablePby = new SimpleStatement("CREATE TABLE IF NOT EXISTS ks_baseball.player_batting_by_year (" +
					    "playerid text," +
					    "yearid int," +
					    "ab int," +
					    "bb int," +
					    "cs int," +
					    "doubles int," +
					    "games int," +
					    "gidp int," +
					    "hbp int," +
					    "hits int," +
					    "hr int," +
					    "batting_avg decimal," +
					    "ibb int," +
					    "lgid text," +
					    "name text," +
					    "rbi int," +
					    "runs int," +
					    "sb int," +
					    "sf int," +
					    "sh int," +
					    "so int," +
					    "stint int," +
					    "teamid text," +
					    "triples int," +
					    "PRIMARY KEY (playerid, yearid));");
		
		// now we change the CL, and it should show up as part of this execution
		createTablePby.setConsistencyLevel(ConsistencyLevel.ALL);
		System.out.println("The Consistency Level is: " + createTablePby.getConsistencyLevel());
		session.execute(createTablePby);
		
		Statement createTablePi = new SimpleStatement("CREATE TABLE IF NOT EXISTS ks_baseball.player_info (" +
			    		"playerid text," +
					    "birthyear int," +
					    "birthmonth int," +
					    "birthday int," +
					    "birthcountry text," +
					    "birthstate text," +
					    "birthcity text," +
					    "deathyear int," +
					    "deathmonth int," +
					    "deathday int," +
					    "deathcountry text," +
					    "deathstate text," +
					    "deathcity text," +
					    "namefirst text," +
					    "namelast text," +
					    "namegiven text," +
					    "weight text," +
					    "height text," +
					    "bats text," +
					    "throws text," +
					    "debut date," +
					    "finalgame date," +
					    "retroID text," +
					    "bbrefID text," +
					    "PRIMARY KEY (playerid));");

		// now we change the CL, and it should show up as part of this execution
		createTablePi.setConsistencyLevel(ConsistencyLevel.ALL);
		System.out.println("The Consistency Level is: " + createTablePi.getConsistencyLevel());
		session.execute(createTablePi);
	}
	
	private static void readData(DseSession session) {
		//Statement read = new SimpleStatement("select * from ks_baseball.player_by_year where playerid='ruthba01';");
		
		Statement read = new SimpleStatement("select * from ks_baseball.player_batting_by_year;");
		System.out.println("Printing out all the data");
		ResultSet rs = session.execute(read);
		List<Row> allResults = rs.all();
		DecimalFormat df = new DecimalFormat("#.###");
		
		for(Row row: allResults) {
			double ba;
			int ab;
			int hits;
			
			System.out.print("\t" + row.getString("playerid"));
			System.out.print("\t" + row.getInt("yearid"));
			//System.out.print("\t" + row.getString("name"));
			System.out.print("\t" + row.getInt("hits"));
			System.out.print("\t" + row.getInt("ab"));
			
			hits = row.getInt("hits");
			ab = row.getInt("ab");
			
			System.out.print("\t Hits = " + Integer.toString(hits));
			System.out.print("\t At Bats = " + Integer.toString(ab));
			
			if ((hits==0)) {
				ba=0;
			} else {
				ba=(float)hits/ab;
			}
			
			//ba = (float)row.getInt("hits")/row.getInt("ab");
			System.out.print("\t" + df.format(ba));		
			System.out.println("");
			
			Statement insertOne = new SimpleStatement(
					"INSERT INTO ks_baseball.player_batting_by_year (playerid, yearid, batting_avg) VALUES ( '"+ row.getString("playerid")
							+ "', " + row.getInt("yearid") + "," + df.format(ba) + ");");
			System.out.println(insertOne.toString());
			session.execute(insertOne);
			
		}		
	}
	
/*
	private static void insertData(DseSession session) {
		Statement insertOne = new SimpleStatement(
				"INSERT INTO java_sample.simple_table (id, name, description) VALUES ( " + UUIDs.random()
						+ ", 'Bob', 'It is Bob!');");
		Statement insertTwo = new SimpleStatement(
				"INSERT INTO java_sample.simple_table (id, name, description) VALUES ( " + UUIDs.random()
						+ ", 'Nancy', 'It is not Bob!');");
		System.out.println("The Consistency Level is: " + insertOne.getConsistencyLevel());
		session.execute(insertOne);
		
		insertTwo.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
		System.out.println("The Consistency Level is: " + insertTwo.getConsistencyLevel());
		session.execute(insertTwo);

	} 

	*/
}


