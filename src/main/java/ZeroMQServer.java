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
  public double scanCost = 0;
  public String query = "";

  public Serializable state;
  public Serializable actions;

  public HashMap<String, String> optimizedPlans = new HashMap<String, String>();
  public HashMap<String, Double> optimizedCosts = new HashMap<String, Double>();
  public ArrayList<Integer> curQuerySet;

  public ZeroMQServer(int port) {
    this.port = Integer.toString(port);
  }

  public ZeroMQServer() {
    ZMQ.Context reqContext = ZMQ.context(1);
    ZMQ.Socket requester = reqContext.socket(ZMQ.REQ);
		//  Socket to talk to server
    requester.connect("tcp://localhost:2000");
    requester.send("java".getBytes(), 0);
    this.port = new String(requester.recv(0));
  }

  public void restart() throws Exception {
      context = ZMQ.context(ZMQ.REP);
      responder = context.socket(ZMQ.REP);
      responder.bind("tcp://*:" + this.port);
  }

  public void close() {
      responder.close();
      context.term();
  }

  // returns the command string sent by the client.
  public String waitForCommand() throws Exception {
    // FIXME: is it bad to reset the connection every time?
    restart();
    //System.out.println("java server waiting for a command");
    String msg;
    byte[] request = responder.recv(0);
    msg = new String(request);
    //System.out.println("Received " + msg);
    Serializable resp = null;
    // this will be set to true ONLY after reset has been called.
    reset = false;
    //System.out.println("waitForCommand: " + msg);
    String plannerName;
    switch (msg)
    {
      case "getJoinsCost":
        resp = "";
        responder.send(resp.toString());
        request = responder.recv(0);
        plannerName = new String(request);
        Double totalCost = optimizedCosts.get(plannerName);
        if (totalCost == null) break;
        // join costs don't consider scan cost.

        resp = (Serializable) (totalCost - scanCost);
        break;
      case "getCurQuerySet":
        resp = curQuerySet;
        break;
      case "getOptPlan":
        resp = "";
        responder.send(resp.toString());
        request = responder.recv(0);
        plannerName = new String(request);
        resp = optimizedPlans.get(plannerName);
        if (resp == null) resp = "";
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
      case "isDone":
        resp = episodeDone;
        break;
      default:
        close();
        return msg;
    }

    //System.out.println(resp);
		try {
      // FIXME: can also write out bytes like this, but it was a pain to deserialize
      // in python.
			//out = new ObjectOutputStream(bos);
			//out.writeObject(resp);
			//out.flush();
			//byte[] yourBytes = bos.toByteArray();
      //System.out.println("sending bytes");
      //responder.send(yourBytes);
      responder.send(resp.toString());
		} catch (Exception ex) {
				// ignore close exception
        System.out.println("there was an error while sending stuff!!");
        // at the least call close here.
        System.exit(-1);
    }

    close();
    return msg;
  }

  public void waitForClientTill(String breakMsg)
  {
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

