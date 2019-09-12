package org.ggszk.ext_sample;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    	// map for keeping Node id and parent relationship
    	Map<Long, Relationship> parent = new LinkedHashMap<>();
		// to avoid coming back to start node
		parent.put(id, null);
    	// array for checking whether the node was visited 
    	List<Long> visited = new ArrayList<>(); 
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
			Long c_nd_no = c_nd.getId();
			if(!(visited.contains(c_nd_no))) {
				for(Relationship rel: rels){
					// if not visited add next node
					Node n_nd = rel.getOtherNode(c_nd);
					Long n_nd_no = n_nd.getId();
					if(!parent.containsKey(n_nd_no)) {
						queue.add(n_nd);
						parent.put(n_nd_no, rel);
					}
				}			
				visited.add(c_nd_no);
			}
	    	Output o = new Output();
	    	o.node = c_nd;
	    	// get path from start_nd to o
	    	o.path = getPath(start_nd, c_nd, parent);
	    	o_l.add(o);
	    	// get next node from queue
			c_nd = queue.poll();
		}
    	return o_l.stream();
    }
    
    // construct path from parent relationships
    public Path getPath(Node frm_nd, Node to_nd, Map<Long, Relationship> parent) {
    	PathImpl.Builder builder = new PathImpl.Builder(frm_nd);
    	List<Relationship> r_list = new ArrayList<Relationship>();
    	Node tmp_nd = to_nd;
    	while(tmp_nd.getId() != frm_nd.getId()) {
    		Relationship r = parent.get(tmp_nd.getId());
	    	r_list.add(r);
	    	tmp_nd = r.getOtherNode(tmp_nd);
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
    	// map for keeping Node id and parent relationship
    	Map<Long, Relationship> parent = new LinkedHashMap<>();
		// to avoid coming back to start node
		parent.put(id, null);
    	// array for checking whether the node was visited 
    	List<Long> visited = new ArrayList<>(); 
    	// queue
    	Deque<Node> queue = new ArrayDeque<>();
    	// start node
    	Node start_nd = db.getNodeById(id);
		// current node
		Node c_nd = start_nd;
		// list for result
		List<Output> o_l = new ArrayList<Output>();
		// end if queue is empty
		while(c_nd != null) {
			Iterable<Relationship> rels = c_nd.getRelationships();
			Long c_nd_no = c_nd.getId();
			if(!(visited.contains(c_nd_no))) {
				for(Relationship rel: rels){
					// if not visited add next node
					Node n_nd = rel.getOtherNode(c_nd);
					Long n_nd_no = n_nd.getId();
					if(!parent.containsKey(n_nd_no)) {
						queue.push(n_nd);
						parent.put(n_nd_no, rel);
					}
				}			
				visited.add(c_nd_no);
			}
	    	Output o = new Output();
	    	o.node = c_nd;
	    	// get path from start_nd to o
	    	o.path = getPath(start_nd, c_nd, parent);
	    	o_l.add(o);
	    	// get next node from queue
	    	c_nd = queue.poll();
		}
    	return o_l.stream();
    } 
}
