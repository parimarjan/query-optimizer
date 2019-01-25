import org.zeromq.ZMQ;
import org.apache.calcite.util.ImmutableBitSet;
import java.io.Serializable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.*;
import java.util.*;

public class ZeroMQServer {

  boolean verbose;
  // ZMQ stuff for communication
  private ZMQ.Context context;
  private ZMQ.Socket responder;
  private String port;

  // FIXME: add reset option for internal state.

  // Essentially serves as the current 'state'
  public QueryGraph queryGraph = null;
  // Internal state for the query planning environment. Here, I just assume
  // that everything is very serial, so the states should be with correct
  // values for the current query.
  public int nextAction = -1;
  public boolean reset = false;
  public boolean END = false;
  public int episodeDone = 0;
  public double lastReward = 0;
  public double lastTrueReward = 0;
  public String query = "";
  public Serializable actions;

  public HashMap<String, HashMap<String, String>> optimizedPlans = new
                                            HashMap<String, HashMap<String, String>>();
  public HashMap<String, HashMap<String, Double>> optimizedCosts = new
                                            HashMap<String, HashMap<String, Double>>();
  //public HashMap<String, Double> optimizedCosts = new HashMap<String, Double>();
  private String BASE_COSTS_FILE_NAME = "optCosts.ser";
  private String COSTS_FILE_NAME;

  public ArrayList<Integer> curQuerySet;
  public ArrayList<Integer> joinOrderSeq = new ArrayList<Integer>();

  /* Utilizes the simplest ZeroMQ protocol (PAIR), to communicate resuts / and
   * synchronize with a Python client.
   * @port: used by ZeroMQ for communication. Must be same on the client
   * started on the other side as well.
   * @verbose: FIXME: remove this, and make centralized logging flags for the
   * query environment.
   *
   * Additional responsibilities of the server:
   *    - Maintaining execution costs for each query and planner pair.
   *    - FIXME: this should be separated out into a new query stats module
   */
  public ZeroMQServer(int port, boolean verbose) {
    COSTS_FILE_NAME = QueryOptExperiment.getCostModelName() + BASE_COSTS_FILE_NAME;
    this.port = Integer.toString(port);
    context = ZMQ.context(1);
    responder = context.socket(ZMQ.PAIR);
    responder.bind("tcp://*:" + this.port);
    this.verbose = verbose;
    HashMap<String, HashMap<String, Double>> oldCosts = (HashMap) loadCosts();
    if (oldCosts != null) {
      optimizedCosts = oldCosts;
      System.out.println("initializing saved costs ...");
      System.out.println("number of queries with saved costs: " + optimizedCosts.size());
    }
  }

  // Hacky interface for loading / saving costs so we don't have to recompute
  // every time. Particularly useful for expensive planners (ExhaustiveSearch)
  // FIXME: make these general purpose
  public void saveUpdatedCosts() {
    HashMap<String, HashMap<String, Double>> oldCosts = (HashMap) loadCosts();
    HashMap<String, HashMap<String, Double>> newCosts = new HashMap<String, HashMap<String, Double>>();
    if (oldCosts != null){
      // ignore this guy, file probably didn't exist.
      newCosts.putAll(oldCosts);
    }
    newCosts.putAll(optimizedCosts);
    saveCosts(newCosts);
  }

  public void saveCosts(Serializable obj)
  {
		try {
			ObjectOutputStream oos = new ObjectOutputStream(
							new FileOutputStream(COSTS_FILE_NAME)
			);
			oos.writeObject(obj);
			oos.flush();
			oos.close();
		} catch (Exception e) {
			System.out.println(e);
		}
  }

  public Serializable loadCosts() {
    try {
      FileInputStream fis = new FileInputStream(COSTS_FILE_NAME);
      ObjectInputStream ois = new ObjectInputStream(fis);
      HashMap<String, HashMap<String, Double>> costs = (HashMap) ois.readObject();
      ois.close();
      return costs;
    } catch (Exception e) {
      System.out.println(e);
    }
    return null;
  }

  /* The main routine which handles communication with the Python client.
   * Essentially a simple, bare bones, single threaded RPC interface. Most
   * commands are simple request <-> response pairs, but some require multiple
   * request <-> response pairs (e.g., getJoinsCost).
   *
   * Supported commands, with their return values, and expected arguments
   * (achieved with multiple request <-> response pairs) are below. Note: the
   * return values right now are always STRINGS, since that is the only thing
   * supported by the ZeroMQ protocol. But we can use it to encode more complex
   * objects: e.g., using JSON. For now, we use some dumb protocols to
   * represent more complex objects. e.g., to represent list of ints, we just
   * send the textual representation, and parse it into a list of ints on the
   * Python side.
   *  - void reset(): reset to a new query. ALWAYS required before starting the next
   *  query.
   *    - FIXME: hacky detail. if zmq.reset is set to true, then the calcite
   *    backend, at various stages, will just try to finish the current query
   *    (e.g., by choosing next actions randomly), and move on to the next
   *    query. But this still updates the costs for that particular execution
   *    etc. so this needs to be handled better.
   *  - [edges] getActions():
   *    - TODO: describe the representation of the edges.
   *  - void step (actionIndex)
   *    - updates the value of zmq.nextAction to the given index
   *  - END: exit the while True loop serving training samples
   *  - QueryGraph getQueryGraph()
   *    - TODO: describe representation
   *  - [int] joinOrderSeq: only updates for RL for now. (don't really need this,
   *  - boolean isDone
   *  since this information is available at the Python end as well...)
   *  - Double getJoinsCost()
   *  - Set getCurQuerySet()
   *  - String getOptPlan()
   *  - Int getAttrCount()
   *  - String curQuery()
   *    - returns the text of the current sql query
   */
  public String waitForCommand() {
    String msg;
    byte[] request = responder.recv(0);
    msg = new String(request);
    if (verbose) System.out.println("Received " + msg);
    Serializable resp = null;
    // this will be set to true ONLY after reset has been called.
    reset = false;
    String plannerName;
    switch (msg)
    {
      // park API based commands
      case "getQueryGraph":
        // First send the vertexes, then the edges
        resp = queryGraph.allVertexes;
        responder.send(resp.toString());
        // just wait for an ack, and then send edges
        request = responder.recv(0);
        resp = queryGraph.edges;
        break;
      // Old ones
      case "joinOrderSeq":
        resp = joinOrderSeq;
        break;
      case "END":
        if (verbose) System.out.println("got END command");
        END = true;
        resp = "";
        break;
      case "getJoinsCost":
        resp = 0.00;
        responder.send(resp.toString());
        request = responder.recv(0);
        plannerName = new String(request);
        Double totalCost = 0.00;
        if (optimizedCosts.get(query) == null) {
          // query hasn't been seen yet, we'll just return 0.00
          break;
        } else {
          totalCost = optimizedCosts.get(query).get(plannerName);
        }
        if (totalCost == null) {
            break;
        }
        if (verbose) System.out.println("totalCost was not null!");
        resp = (Serializable) (totalCost);
        break;
      case "getCurQuerySet":
        resp = curQuerySet;
        break;
      case "getOptPlan":
        if (verbose) System.out.println("getOptPlan");
        resp = "";
        responder.send(resp.toString());
        request = responder.recv(0);
        plannerName = new String(request);
        if (verbose) System.out.println("plannerName = " + plannerName);
        //resp = optimizedPlans.get(query).get(plannerName);
        //if (resp == null) resp = "";
        break;
      case "getAttrCount":
        resp = DbInfo.attrCount;
        break;
      case "reset":
        reset = true;
        resp = "";
        break;
      case "curQuery":
        resp = query;
        break;
      case "getActions":
        resp = actions;
        break;
      case "step":
        // here we might need to do a bunch of things to get all the feedback.
        try {
          resp = "";
          responder.send(resp.toString());
          request = responder.recv(0);
          String action = new String(request);
          nextAction = Integer.parseInt(action);
        } catch (Exception e) {
          e.printStackTrace();
        }
        break;
      case "getReward":
        resp = lastReward;
        break;
      case "getTrueReward":
        resp = lastTrueReward;
        break;
      case "isDone":
        resp = episodeDone;
        break;
      default:
        System.out.println("ZeroMQServer DEFAULT!!!");
        return msg;
    }

    if (verbose) System.out.println("resp is: " + resp);
		try {
      responder.send(resp.toString());
		} catch (Exception ex) {
				// ignore close exception
        System.out.println("there was an error while sending stuff!!");
        // at the least call close here.
        // FIXME: exiting from java in general seems to fail silently..
        System.out.println(resp);
        System.exit(-1);
    }
    return msg;
  }

  /* This method serves to ensure synchronization between the java backend, and
   * the python front end (e.g., the park API implementation), while
   * running in a single thread in Java.
   * @breakCommand One of the commands, documented in waitForCommand. Until, we
   * receive this command, we will not proceed further with the java execution
   * and will continue to serve other commands (with waitForCommand). Thus, the
   * internal state of the Calcite backend would remain constant until we get
   * a particular breakCommand.
   *
   * Usage:
   *      We have used this at various points: e.g., waitForClientTill("reset")
   *      at the end of the episode execution. Meanwhile, the client can
   *      continue requesting other information about the query that was
   *      executed: e.g., how long it took, what the rewards were, final plan
   *      chosen etc. This information may not be available in the Calcite
   *      backend exactly when Python asks for it - and until we respond, the
   *      Python execution would just halt as well. But the calcite backend
   *      will only process these commands serially once it finishes the
   *      episode, and comes into the waitForClientTill("reset") function. By
   *      this time, all the information about the current query execution
   *      would be up to date.
   */
  public void waitForClientTill(String breakCommand)
  {
    if (verbose) System.out.println("wait for client till: " + breakCommand);
    try {
      while (!reset) {
        String cmd = waitForCommand();
        if (cmd.equals(breakCommand)) {
          //System.out.println("breaking out of waitForClientTill " + breakCommand);
          break;
        }
      }
    } catch (Exception e) {
      System.out.println("caught exception in waitForClientTill " + breakCommand);
      System.out.println(e);
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
