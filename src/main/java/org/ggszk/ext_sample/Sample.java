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
 * 
 * @author ggszk
 */
public class Sample {
	// This field declares that we need a GraphDatabaseService
	// as context when any procedure in this class is invoked
	@Context
	public GraphDatabaseService db;

	// This gives us a log instance that outputs messages to the
	// standard log, normally found under `data/log/console.log`
	@Context
	public Log log;

	// result class for samples
	public class Output {
		public String out;
		public Node node;
		public Path path;
		public double cost;
	}

	// sample4_1
	@Procedure(value = "example.sample4_1")
	@Description("ggszk sample4_1")
	public Stream<Output> sample4_1(@Name("f_name") String f_name, @Name("l_name") String l_name) {
		Output o = new Output();
		o.out = "hello: " + f_name + " " + l_name;
		return Arrays.asList(o).stream();
	}

	// sample4_2
	@Procedure(value = "example.sample4_2")
	@Description("sample4_2: return adjacent paths for given node id")
	public Stream<Output> sample4_2(@Name("id") Long id) {
		Node from_nd = db.getNodeById(id);
		Iterable<Relationship> rels = from_nd.getRelationships();
		List<Output> o_l = new ArrayList<Output>();
		for (Relationship rel : rels) {
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
	public Stream<Output> sample6_1(@Name("id") Long id) {
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
		while (c_nd != null) {
			Iterable<Relationship> rels = c_nd.getRelationships();
			if (!(visited.contains(c_nd))) {
				for (Relationship rel : rels) {
					// if not visited add next node
					Node n_nd = rel.getOtherNode(c_nd);
					if (!parent.containsKey(n_nd)) {
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
		while (!tmp_nd.equals(frm_nd)) {
			Relationship r = parent.get(tmp_nd);
			queue.push(r);
			tmp_nd = r.getOtherNode(tmp_nd);
		}
		Relationship tmp_r = queue.poll();
		while (tmp_r != null) {
			builder = builder.push(tmp_r);
			tmp_r = queue.poll();
		}
		return builder.build();
	}

	// sample6_2: DFS
	@Procedure(value = "example.sample6_2")
	@Description("sample6_2: DFS")
	public Stream<Output> sample6_2(@Name("id") Long id) {
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
		while (c_nd != null) {
			Iterable<Relationship> rels = c_nd.getRelationships();
			if (!(visited.contains(c_nd))) {
				for (Relationship rel : rels) {
					// if not visited add next node
					Node n_nd = rel.getOtherNode(c_nd);
					if (!parent.containsKey(n_nd)) {
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
	public Stream<Output> sample8_1(@Name("from_id") Long from_id, @Name("to_id") Long to_id) {
		Node from_nd = db.getNodeById(from_id);
		Node to_nd = db.getNodeById(to_id);
		// Priority Queue by cost property value
		PriorityQueue<NodeInfo> pq = new PriorityQueue<>((n1, n2) -> Double.compare(n1.cost, n2.cost));
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
		while (to_ni == null || to_ni.done != true) {
			// if queue is empty, no route exit
			if (cur_ni == null) {
				return new ArrayList<Output>().stream();
			}
			// top node of queue's cost is fixed
			cur_ni = pq.poll();
			cur_ni.done = true;
			// get adjacent nodes and add them to queue
			// Property for cost: type must be double
			String cost_property = "cost";
			Iterable<Relationship> rels = cur_ni.nd.getRelationships();
			for (Relationship rel : rels) {
				// get adjacent nodes and their costs
				Node o_nd = rel.getOtherNode(cur_ni.nd);
				double cost_rel = (double) rel.getProperty(cost_property);
				// check whether the node was found
				NodeInfo next_ni = nodes.get(o_nd);
				// not found -> 1st appearance of the node, add it to map
				if (next_ni == null) {
					next_ni = new NodeInfo(o_nd, cur_ni.cost + cost_rel);
					pq.add(next_ni);
					nodes.put(o_nd, next_ni);
					parent.put(o_nd, rel);
				}
				// found but cost is't fixed and has lower cost -> overwrite cost
				else if (next_ni.done == false) {
					if (next_ni.cost > cur_ni.cost + cost_rel) {
						next_ni.cost = cur_ni.cost + cost_rel;
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

	// sample8_2: bidirectional djkstra
	@Procedure(value = "example.sample8_2")
	@Description("sample8_2: bidirectional djkstra")
	public Stream<Output> sample8_2(@Name("from_id") Long from_id, @Name("to_id") Long to_id) {
		Node from_nd = db.getNodeById(from_id);
		Node to_nd = db.getNodeById(to_id);
		// Priority Queue by cost property value
		PriorityQueue<NodeInfo> pq_f = new PriorityQueue<>((n1, n2) -> Double.compare(n1.cost, n2.cost));
		PriorityQueue<NodeInfo> pq_t = new PriorityQueue<>((n1, n2) -> Double.compare(n1.cost, n2.cost));
		// map for keeping node and cost
		Map<Node, NodeInfo> nodes_f = new HashMap<>();
		Map<Node, NodeInfo> nodes_t = new HashMap<>();
		// map for keeping Node and parent relationship
		Map<Node, Relationship> parent_f = new HashMap<>();
		Map<Node, Relationship> parent_t = new HashMap<>();
		
		// current node(info)
		NodeInfo cur_ni_f = new NodeInfo(from_nd, 0.0);
		pq_f.add(cur_ni_f);
		nodes_f.put(from_nd, cur_ni_f);
		NodeInfo cur_ni_t = new NodeInfo(to_nd, 0.0);
		pq_t.add(cur_ni_t);
		nodes_t.put(to_nd, cur_ni_t);

		// node that f-side path and t-side path meets
		NodeInfo min_ni_f = null;
		NodeInfo min_ni_t = null;

		// variables for checking to exit
		double total_cost = 100000; // total cost

		// Result
		Output o = new Output();

		// Path finding
		while(true) {
			// if one queue is empty, no route exit
			if(cur_ni_f == null || cur_ni_t == null) {
				return new ArrayList<Output>().stream();
			}
			// exit when found to node in the from-side
			if(cur_ni_f.nd.equals(to_nd)) {
	    		o.path = getPath(from_nd, min_ni_f.nd, parent_f);				
	    		o.cost = cur_ni_f.cost;
	    		break;
			}
			// exit when found from node in the to-side
			if(cur_ni_t.nd.equals(from_nd)) {
	    		o.path = reverse(getPath(to_nd, min_ni_t.nd, parent_t));
	    		o.cost = cur_ni_t.cost;
	    		break;			
			}
		    // exit when cannot find shorter path (triangle inequality)
	    	// (total cost) < (current f-side cost) + (current t-side cost)
	    	if(cur_ni_f.cost + cur_ni_t.cost > total_cost){
	    		Path f_path = getPath(from_nd, min_ni_f.nd, parent_f);
	    		Path t_path = getPath(to_nd, min_ni_t.nd, parent_t);
	    		o.path = cat(f_path, reverse(t_path));
	    		o.cost = total_cost;
	    		break;
	    	}
			// top node of queue's cost is fixed
			cur_ni_f = pq_f.poll();
			cur_ni_f.done = true;
			cur_ni_t = pq_t.poll();
			cur_ni_t.done = true;
			
			// found total path
			// find the node in the other side
			NodeInfo ni_t = nodes_t.get(cur_ni_f.nd);
			NodeInfo ni_f = nodes_f.get(cur_ni_t.nd);	
			
			// the node is in the other side map and cost has been fixed (done)
			if(ni_t != null && ni_t.done == true) {
				min_ni_f = cur_ni_f;
				min_ni_t = ni_t;
				total_cost = cur_ni_f.cost + ni_t.cost;					
			}
			if(ni_f != null && ni_f.done == true) {
				min_ni_f = ni_f;
				min_ni_t = cur_ni_t;
				total_cost = ni_f.cost + cur_ni_t.cost;	
			}
			// get adjacent nodes and add them to queue
			// Property for cost: type must be double
			getAdjacentNodes(cur_ni_f, pq_f, nodes_f, parent_f);
			getAdjacentNodes(cur_ni_t, pq_t, nodes_t, parent_t);
		}
		// list for result
		List<Output> o_l = new ArrayList<>();
		o_l.add(o);
		return o_l.stream();
	}

	// reverse Path
	public Path reverse(Path p) {
		PathImpl.Builder builder = new PathImpl.Builder(p.endNode());		
		for(Relationship r: p.reverseRelationships()) {
			builder = builder.push(r);
		}
		return builder.build();
	}
	// concatenate two Paths 
	public Path cat(Path p1, Path p2) {
		PathImpl.Builder builder = new PathImpl.Builder(p1.startNode());
		Iterable<Relationship> rels1 = p1.relationships();
		Iterable<Relationship> rels2 = p2.relationships();
		for(Relationship r: rels1) {
			builder = builder.push(r);
		}
		for(Relationship r: rels2) {
			builder = builder.push(r);
		}
		return builder.build();
	}
	
	// get adjacent nodes and add them to queue
	public void getAdjacentNodes(NodeInfo cur_ni, PriorityQueue<NodeInfo> pq, Map<Node, NodeInfo> nodes, Map<Node, Relationship> parent) {
		String cost_property = "cost";
		Iterable<Relationship> rels = cur_ni.nd.getRelationships();
		for (Relationship rel : rels) {
			// get adjacent nodes and their costs
			Node o_nd = rel.getOtherNode(cur_ni.nd);
			double cost_rel = (double) rel.getProperty(cost_property);
			// check whether the node was found
			NodeInfo next_ni = nodes.get(o_nd);
			// not found -> 1st appearance of the node, add it to map
			if (next_ni == null) {
				next_ni = new NodeInfo(o_nd, cur_ni.cost + cost_rel);
				pq.add(next_ni);
				nodes.put(o_nd, next_ni);
				parent.put(o_nd, rel);
			}
			// found but cost is't fixed and has lower cost -> overwrite cost
			else if (next_ni.done == false) {
				if (next_ni.cost > cur_ni.cost + cost_rel) {
					next_ni.cost = cur_ni.cost + cost_rel;
					parent.put(o_nd, rel);
				}
			}
			// found and cost was fixed -> do nothing
		}
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
