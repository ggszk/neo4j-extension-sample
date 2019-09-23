package org.ggszk.ext_sample;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
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
 * @author ggszk
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
    	public double cost;
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
    	// start node
    	Node start_nd = db.getNodeById(id);
    	// map for keeping Node and parent relationship
    	Map<Node, Relationship> parent = new HashMap<>();
		// to avoid coming back to start node
		parent.put(start_nd, null);
    	// array for checking whether the node was visited 
    	List<Node> visited = new ArrayList<>(); 
    	// queue
    	Queue<Node> queue = new ArrayDeque<>();
		// current node
		Node c_nd = start_nd;
		// list for result
		List<Output> o_l = new ArrayList<>();
		// end if queue is empty
		while(c_nd != null) {
			Iterable<Relationship> rels = c_nd.getRelationships();
			if(!(visited.contains(c_nd))) {
				for(Relationship rel: rels){
					// if not visited add next node
					Node n_nd = rel.getOtherNode(c_nd);
					if(!parent.containsKey(n_nd)) {
						queue.add(n_nd);
						parent.put(n_nd, rel);
					}
				}			
				visited.add(c_nd);
			}
	    	Output o = new Output();
	    	o.node = c_nd;
	    	// get path from start_nd to c_nd
	    	o.path = getPath(start_nd, c_nd, parent);
	    	o_l.add(o);
	    	// get next node from queue
			c_nd = queue.poll();
		}
    	return o_l.stream();
    }
     
    // construct path from parent relationships
    public Path getPath(Node frm_nd, Node to_nd, Map<Node, Relationship> parent) {
    	PathImpl.Builder builder = new PathImpl.Builder(frm_nd);
    	// queue
    	Deque<Relationship> queue = new ArrayDeque<>();

    	Node tmp_nd = to_nd;
    	while(!tmp_nd.equals(frm_nd)){
    		Relationship r = parent.get(tmp_nd);
    		queue.push(r);
	    	tmp_nd = r.getOtherNode(tmp_nd);
    	}
		Relationship tmp_r = queue.poll();
    	while(tmp_r != null){
	    	builder = builder.push(tmp_r);
    		tmp_r = queue.poll();
    	};
    	return builder.build();
    }
    
    // sample6_2: DFS
    @Procedure(value = "example.sample6_2")
    @Description("sample6_2: DFS")
    public Stream<Output> sample6_2( @Name("id") Long id )
    {
    	// start node
    	Node start_nd = db.getNodeById(id);
    	// map for keeping Node id and parent relationship
    	Map<Node, Relationship> parent = new HashMap<>();
		// to avoid coming back to start node
		parent.put(start_nd, null);
    	// array for checking whether the node was visited 
    	List<Node> visited = new ArrayList<>(); 
    	// queue
    	Deque<Node> queue = new ArrayDeque<>();
		// current node
		Node c_nd = start_nd;
		// list for result
		List<Output> o_l = new ArrayList<>();
		// end if queue is empty
		while(c_nd != null) {
			Iterable<Relationship> rels = c_nd.getRelationships();
			if(!(visited.contains(c_nd))) {
				for(Relationship rel: rels){
					// if not visited add next node
					Node n_nd = rel.getOtherNode(c_nd);
					if(!parent.containsKey(n_nd)) {
						queue.push(n_nd);
						parent.put(n_nd, rel);
					}
				}			
				visited.add(c_nd);
			}
	    	Output o = new Output();
	    	o.node = c_nd;
	    	// get path from start_nd to c_nd
	    	o.path = getPath(start_nd, c_nd, parent);
	    	o_l.add(o);
	    	// get next node from queue
	    	c_nd = queue.poll();
		}
    	return o_l.stream();
    }
    
    // sample8_1: djkstra
    @Procedure(value = "example.sample8_1")
    @Description("sample8_1: djkstra")
    public Stream<Output> sample8_1( @Name("from_id") Long from_id, @Name("to_id") Long to_id )
    {
    	Node from_nd = db.getNodeById(from_id);
    	Node to_nd = db.getNodeById(to_id);
        // Priority Queue by cost property value
    	PriorityQueue<NodeInfo> pq = new PriorityQueue<>((n1,n2)->Double.compare(n1.cost,n2.cost));
        // map for keeping node and cost
    	Map<Node, NodeInfo> nodes = new HashMap<>();
    	// map for keeping Node and parent relationship
    	Map<Node, Relationship> parent = new HashMap<>();
        
        // current node(info)
        NodeInfo cur_ni = new NodeInfo(from_nd, 0.0);
	    pq.add(cur_ni);
	    nodes.put(from_nd, cur_ni);

    	// if to_node's cost is fixed, exit
    	NodeInfo to_ni = null;
	    while(to_ni == null || to_ni.done != true){
	    	// if queue is empty, no route exit
	    	if(cur_ni == null){
	    		return new ArrayList<Output>().stream();
	    	}
		    // top node of queue's cost is fixed
	    	cur_ni = pq.poll();
	    	cur_ni.done = true;
	    	// get adjacent nodes and add them to queue
	    	// Property for cost: type must be double
	    	String cost_property = "cost";
		    Iterable<Relationship> rels = cur_ni.nd.getRelationships();
		    for(Relationship rel: rels){
		    	// get adjacent nodes and their costs
				Node o_nd = rel.getOtherNode(cur_ni.nd);
				double cost_rel = (double)rel.getProperty(cost_property);
				// check whether the node was found 
				NodeInfo next_ni = nodes.get(o_nd);
				// not found -> 1st appearance of the node, add it to map
				if(next_ni == null){
					next_ni = new NodeInfo(o_nd, cur_ni.cost+cost_rel);
					pq.add(next_ni);
				    nodes.put(o_nd, next_ni);
				    parent.put(o_nd, rel);
				}
				// found but cost is't fixed and has lower cost -> overwrite cost
				else if(next_ni.done == false){
					if(next_ni.cost > cur_ni.cost+cost_rel){
						next_ni.cost = cur_ni.cost+cost_rel;
					    parent.put(o_nd, rel);
					}
				}
				// found and cost was fixed -> do nothing
		    }	
	    	// get to_node from map
	    	to_ni = nodes.get(to_nd);
	    }
	    // output construction
    	Output o = new Output();
    	o.path = getPath(from_nd, to_nd, parent);
    	o.cost = to_ni.cost;
		// list for result
		List<Output> o_l = new ArrayList<>();
		o_l.add(o);
	    return o_l.stream();
	}
	
    // Node and cost to it 
    public class NodeInfo {
    	public Node nd; // Neo4j Node
    	public double cost; // cost summation from start node
    	public boolean done; // flag for cost fixed

    	// constructor
    	public NodeInfo(Node nd, double cost) {
    		super();
    		this.nd = nd;
    		this.cost = cost;
    		done = false;
    	}
    	public String toString() {
    		String s = "";
	    	Node nd_i = this.nd;
	    	s = s + nd_i.getId();
	    	s = s + ":";
	    	s = s + this.cost;
	    	s = s + ",";
	    	return s;
    	}
    }
}
