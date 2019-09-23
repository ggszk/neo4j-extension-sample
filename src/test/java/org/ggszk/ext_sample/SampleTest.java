package org.ggszk.ext_sample;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.harness.junit.Neo4jRule;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.ggszk.ext_sample.Sample;

public class SampleTest
{
    // This rule starts a Neo4j instance for us
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()

            // This is the Procedure we want to test
            .withProcedure( Sample.class );

    @Test
    public void sample4_1test() throws Throwable
    {
        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {
        	session.run( "CREATE (p:Player {f_name:'Karl', l_name:'Leister'})" );
        	String out = session.run( "MATCH (n:Player) CALL example.sample4_1(n.f_name, n.l_name) yield out as out return out" ).single().get(0).asString();
        	assertThat( out, equalTo("hello: Karl Leister"));
        }    	
    }
    @Test
    public void sample4_2test() throws Throwable
    {
        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {
            long nodeId = session.run( "CREATE (p:Player {l_name:'Leister', f_name:'Karl'}) RETURN id(p)" )
                    .single()
                    .get( 0 ).asLong();
        	session.run( "CREATE (p:Orchestra {name:'Berlin Philharmonic'})" );
        	session.run( "match (a:Player), (b:Orchestra) create (a)-[r:BELONG_TO]->(b)");
        	Path p = session.run( "CALL example.sample4_2(" + nodeId + ") yield path as path" ).single().get(0).asPath();
        	// CAUTION!: Node id might change depending to circumstances  
        	assertThat( p.toString(), equalTo("path[(0)-[0:BELONG_TO]->(20)]"));
        }    	
    }
    @Test
    public void sample6_1test() throws Throwable
    {
        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {
            long nodeId = session.run( "CREATE (p:S3 {no:0, visited:false}) RETURN id(p)" )
                    .single()
                    .get( 0 ).asLong();
        	session.run( "match (n {no:0}) CREATE (n) -[r:CONNECT_TO]->(n1:S3 {no:1, visited:false})");
        	session.run( "match (n {no:0}) CREATE (n) -[r:CONNECT_TO]->(n1:S3 {no:2, visited:false})");
        	session.run( "match (n {no:1}) CREATE (n) -[r:CONNECT_TO]->(n1:S3 {no:3, visited:false})");
        	StatementResult r = session.run( "CALL example.sample6_1(" + nodeId + ") yield node, path return node.no, path" );
        	List<Integer> nos = new ArrayList<Integer>();
        	while(r.hasNext()) {
        		Record rec = r.next();
        		nos.add(rec.get(0).asInt());
        		System.out.println(rec.get(1).asPath().toString());        			
        	}
        	assertThat( nos.toString(), equalTo("[0, 2, 1, 3]"));
        }    	
    }
    @Test
    public void sample6_2test() throws Throwable
    {
        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {
            long nodeId = session.run( "CREATE (p:S3 {no:0, visited:false}) RETURN id(p)" )
                    .single()
                    .get( 0 ).asLong();
        	session.run( "match (n {no:0}) CREATE (n) -[r:CONNECT_TO]->(n1:S3 {no:1, visited:false})");
        	session.run( "match (n {no:0}) CREATE (n) -[r:CONNECT_TO]->(n1:S3 {no:2, visited:false})");
        	session.run( "match (n {no:1}) CREATE (n) -[r:CONNECT_TO]->(n1:S3 {no:3, visited:false})");
        	StatementResult r = session.run( "CALL example.sample6_2(" + nodeId + ") yield node as n return n.no" );
        	List<Integer> nos = new ArrayList<Integer>();
        	
        	while(r.hasNext()) {
        		nos.add(r.next().get(0).asInt());
        	}
        	assertThat( nos.toString(), equalTo("[0, 1, 3, 2]"));
        }    	
    }
    @Test
    public void sample8_1test() throws Throwable
    {
        // In a try-block, to make sure we close the driver and session after the test
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() )
        {
            long nodeId = session.run( "CREATE (p:S3 {no:0}) RETURN id(p)" )
                    .single()
                    .get( 0 ).asLong();
            long to_nodeId = session.run( "CREATE (p:S3 {no:1}) RETURN id(p)" )
                    .single()
                    .get( 0 ).asLong();
            
        	session.run( "match (n0 {no:0}),(n1 {no:1}) CREATE (n0) -[r:CONNECT_TO {cost:3.0}]->(n1)");
        	session.run( "match (n {no:0}) CREATE (n) -[r:CONNECT_TO {cost:4.0}]->(n1:S3 {no:2})");
        	session.run( "match (n {no:1}) CREATE (n) -[r:CONNECT_TO {cost:5.0}]->(n1:S3 {no:3})");
        	session.run( "match (n {no:2}) CREATE (n) -[r:CONNECT_TO {cost:6.0}]->(n1:S3 {no:4})");
        	StatementResult r = session.run( "CALL example.sample8_1(" + nodeId + ", " + to_nodeId + ") yield path, cost return path, cost" );
        	List<Double> costs = new ArrayList<Double>();
        	while(r.hasNext()) {
        		Record rec = r.next();
        		System.out.println(rec.get(0).asPath().toString());        			
        		costs.add(rec.get(1).asDouble());
        	}
        	assertThat( costs.toString(), equalTo("[3.0]"));
        }    	
    }
}
