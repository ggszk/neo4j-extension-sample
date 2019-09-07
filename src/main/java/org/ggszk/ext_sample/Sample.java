package org.ggszk.ext_sample;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.stream.Stream;

import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

/**
 * Samples for Neo4j graph algorithm
 */
public class Sample
{
    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    // result class for samples
    public class Output{
    	public String out;
    	public Node node;
    	public Path path;
    }

    // sample4_1
    @Procedure(value = "example.sample4_1")
    @Description("ggszk sample4_1")
    public Stream<Output> sample4_1( @Name("f_name") String f_name,
                                     @Name("l_name") String l_name )
    {
    	Output o = new Output();
    	o.out = "hello: " + f_name + " " + l_name;
    	return Arrays.asList(o).stream();
    }

    // sample4_2
    @Procedure(value = "example.sample4_2")
    @Description("sample4_2: return adjacent paths for given node id")
    public Stream<Output> sample4_2( @Name("id") Long id )
    {
		Node from_nd = db.getNodeById(id); 
		Iterable<Relationship> rels = from_nd.getRelationships();
		List<Output> o_l = new ArrayList<Output>();
		for(Relationship rel: rels){
			PathImpl.Builder builder = new PathImpl.Builder(from_nd);
			builder = builder.push(rel);
	    	Output o = new Output();
	    	o.path = builder.build();
	    	o_l.add(o);
		}
    	return o_l.stream();
    } 
         
    // sample6_1: BFS
    @Procedure(value = "example.sample6_1")
    @Description("sample6_1: BFS")
    public Stream<Output> sample6_1( @Name("id") Long id )
    {
    	int max_nodes = 100;
    	// array for checking whether the node was visited 
    	Boolean[] visited = new Boolean[max_nodes];
    	for(int i = 0; i < max_nodes ; ++i) {
    		visited[i] = false;
    	}
    	// array for keeping relationship to parent
    	Relationship[] parent_rel = new Relationship[max_nodes];
    	// queue
    	Queue<Node> queue = new ArrayDeque<>();
    	// start node
    	Node start_nd = db.getNodeById(id);
		// current node
		Node c_nd = start_nd;
		// list for result
		List<Output> o_l = new ArrayList<Output>();
		// end if queue is empty
		while(c_nd != null) {
			Iterable<Relationship> rels = c_nd.getRelationships();
			Integer c_nd_no = new Integer(c_nd.getProperty("no").toString());
			if(!(visited[c_nd_no])) {
				for(Relationship rel: rels){
					// if not visited add next node
					Node n_nd = rel.getOtherNode(c_nd);
					Integer n_nd_no = new Integer(n_nd.getProperty("no").toString());
					if(!(visited[n_nd_no])) {
						queue.add(n_nd);						
						parent_rel[n_nd_no] = rel;
					}
				}			
				visited[c_nd_no] = true;
			}
	    	Output o = new Output();
	    	o.node = c_nd;
	    	// get path from start_nd to o
	    	o.path = getPath(start_nd, c_nd, parent_rel);
	    	o_l.add(o);
	    	// get next node from queue
			c_nd = queue.poll();
		}
    	return o_l.stream();
    }
    
    // construct path from parent relationships
    public Path getPath(Node frm_nd, Node to_nd, Relationship[] prt_rel) {
    	PathImpl.Builder builder = new PathImpl.Builder(frm_nd);
    	List<Relationship> r_list = new ArrayList<Relationship>();
    	Node tmp_nd = to_nd;
		Integer to_nd_no = new Integer(to_nd.getProperty("no").toString());
    	Integer tmp_nd_no = to_nd_no;
    	while(tmp_nd.getId() != frm_nd.getId()) {
	    	r_list.add(prt_rel[tmp_nd_no]);
	    	tmp_nd = prt_rel[tmp_nd_no].getOtherNode(tmp_nd);
			tmp_nd_no = new Integer(tmp_nd.getProperty("no").toString());		    	
    	}
    	Collections.reverse(r_list);
    	for(Relationship r : r_list) {
	    	builder = builder.push(r);
    	};
    	return builder.build();
    }
    
    // sample6_2: DFS
    @Procedure(value = "example.sample6_2")
    @Description("sample6_2: DFS")
    public Stream<Output> sample6_2( @Name("id") Long id )
    {
    	int max_nodes = 100;
    	// array for checking whether the node was visited 
    	Boolean[] visited = new Boolean[max_nodes];
    	for(int i = 0; i < max_nodes ; ++i) {
    		visited[i] = false;
    	}
    	// queue
    	Deque<Node> deque = new ArrayDeque<>();
    	// start node
    	Node start_nd = db.getNodeById(id);
		// current node
		Node c_nd = start_nd;
		// list for result
		List<Output> o_l = new ArrayList<Output>();
		// end if queue is empty
		while(c_nd != null) {
			Iterable<Relationship> rels = c_nd.getRelationships();
			Integer c_nd_no = new Integer(c_nd.getProperty("no").toString());
			if(!(visited[c_nd_no])) {
				for(Relationship rel: rels){
					// if not visited add next node
					Node n_nd = rel.getOtherNode(c_nd);
					Integer n_nd_no = new Integer(n_nd.getProperty("no").toString());
					if(!(visited[n_nd_no])) {
						deque.push(n_nd);						
					}
				}			
				visited[c_nd_no] = true;
			}
	    	Output o = new Output();
	    	o.node = c_nd;
	    	o_l.add(o);
	    	// get next node from queue
			c_nd = deque.poll();
		}
    	return o_l.stream();
    } 
}
