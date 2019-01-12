import org.zeromq.ZMQ;
import org.apache.calcite.util.ImmutableBitSet;
import java.io.Serializable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.*;
import java.util.*;

import com.google.gson.Gson;
//import com.google.gson.GsonBuilder;


// FIXME: generalize this enough to handle different feature / state
// representations.

public class ZeroMQServer {

  boolean verbose;
  // ZMQ stuff for communication
  private ZMQ.Context context;
  private ZMQ.Socket responder;
  private String port;

  // FIXME: add reset option for internal state.
  public QueryGraph queryGraph = null;

  // Internal state for the query planning environment. Here, I just assume
  // that everything is very serial, so the states should be appropriately
  // updated whenever someone asks for it.
  public int episodeNum = 0;
  public int nextAction = -1;
  public boolean reset = false;
  public boolean END = false;
  public int episodeDone = 0;
  public double lastReward = 0;
  public double lastTrueReward = 0;
  public double scanCost = 0;
  public String query = "";

  public Serializable state;
  public Serializable actions;

  public HashMap<String, HashMap<String, String>> optimizedPlans = new HashMap<String, HashMap<String, String>>();
  public HashMap<String, HashMap<String, Double>> optimizedCosts = new HashMap<String, HashMap<String, Double>>();
  //public HashMap<String, Double> optimizedCosts = new HashMap<String, Double>();
  private String BASE_COSTS_FILE_NAME = "optCosts.ser";
  private String COSTS_FILE_NAME;

  public ArrayList<Integer> curQuerySet;

  // FIXME: get rid of this
  public ArrayList<Integer> joinOrderSeq = new ArrayList<Integer>();

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

  // FIXME: make these general purpose
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

  // returns the command string sent by the client.
  public String waitForCommand() {
    // FIXME: is it bad to reset the connection every time?
    // restart();
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
        if (verbose) System.out.println("getJoinsCost");
        //resp = "";
        resp = 0.00;
        responder.send(resp.toString());
        request = responder.recv(0);
        plannerName = new String(request);
        Double totalCost = optimizedCosts.get(query).get(plannerName);
        if (totalCost == null) {
          // has not been executed ...
          break;
        }
        if (verbose) System.out.println("totalCost was not null!");

        // FIXME: join costs don't consider scan cost (?)
        resp = (Serializable) (totalCost);
        //resp = (Serializable) (totalCost - scanCost);
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
        episodeNum = 0;
        reset = true;
        resp = query;
        break;
      case "getActions":
        resp = actions;
        break;
      case "getState":
        resp = state;
        break;
      case "end":
        resp = "";
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
        //System.exit(-1);
    }
    return msg;
  }

  public void waitForClientTill(String breakMsg)
  {
    if (verbose) System.out.println("wait for client till: " + breakMsg);
    try {
      while (!reset) {
        String cmd = waitForCommand();
        if (cmd.equals(breakMsg)) {
          break;
        }
      }
    } catch (Exception e) {

    }
  }
}

