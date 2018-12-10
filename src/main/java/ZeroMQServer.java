import org.zeromq.ZMQ;
import org.apache.calcite.util.ImmutableBitSet;
import java.io.Serializable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.*;
import java.util.*;

// FIXME: generalize this enough to handle different feature / state
// representations.

public class ZeroMQServer {

  boolean verbose;
  // ZMQ stuff for communication
  private ZMQ.Context context;
  private ZMQ.Socket responder;
  private String port;

  // Internal state for the query planning environment. Here, I just assume
  // that everything is very serial, so the states should be appropriately
  // updated whenever someone asks for it.
  public int episodeNum = 0;
  public int nextAction = -1;
  public boolean reset = false;
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
  private String COSTS_FILE_NAME = "optCosts.ser";

  public ArrayList<Integer> curQuerySet;

  public ZeroMQServer(int port, boolean verbose) {
    this.port = Integer.toString(port);
    context = ZMQ.context(1);
    responder = context.socket(ZMQ.PAIR);
    responder.bind("tcp://*:" + this.port);
    this.verbose = verbose;
    HashMap<String, HashMap<String, Double>> oldCosts = (HashMap) loadCosts();
    if (oldCosts != null) {
      System.out.println("old costs weren't null!");
      System.out.println(oldCosts);
      optimizedCosts = oldCosts;
    }
  }

  public void saveUpdatedCosts() {
    HashMap<String, HashMap<String, Double>> oldCosts = (HashMap) loadCosts();
    HashMap<String, HashMap<String, Double>> newCosts = new HashMap<String, HashMap<String, Double>>();
    //System.out.println(oldCosts);
    if (oldCosts != null){
      // ignore this guy, file probably didn't exist.
      System.out.println("oldCosts were not null");
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
  public String waitForCommand() throws Exception {
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
      case "getJoinsCost":
        if (verbose) System.out.println("getJoinsCost");
        //resp = "";
        resp = 0.00;
        responder.send(resp.toString());
        request = responder.recv(0);
        plannerName = new String(request);
        Double totalCost = optimizedCosts.get(query).get(plannerName);
        if (totalCost == null) {
          // FIXME: this should never happen, but need to figure out a way to
          // debug this (?)
          System.out.println("totalCost was null!");
          //System.exit(-1);
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
        // don't need to send any reply here.
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
        resp = "";
        responder.send(resp.toString());
        request = responder.recv(0);
        String action = new String(request);
        nextAction = Integer.parseInt(action);
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
        //close();
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

