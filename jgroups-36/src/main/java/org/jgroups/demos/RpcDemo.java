package org.jgroups.demos;

import org.jgroups.*;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.common.demo.NumberPrinter;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;


public class RpcDemo extends ReceiverAdapter {
  JChannel           channel;
  RpcDispatcher      disp;
  RspList            rsp_list;

  private Set<Address> otherMembers = new HashSet<>();

  private NumberPrinter np = new NumberPrinter();

  public void viewAccepted(View new_view) {
    System.out.println("** view: " + new_view);
    for (Address member : new_view)
    {
      if (!member.equals(channel.getAddress())) {
        otherMembers.add(member);
      }
    }

    System.out.println("** other members: " + otherMembers);
  }

  public void start(String props, String name) throws Exception {

    channel=new JChannel(props);
    disp=new RpcDispatcher(channel, np) {
      @Override
      public Object handle(Message req) throws Exception {
        return super.handle(req);
      }
    };
    disp.setMembershipListener(this);
    channel.connect("RpcDispatcherTestGroup");
    Util.sleep(5000);

    RequestOptions opts=new RequestOptions(ResponseMode.GET_ALL, 5000);
    opts.setExclusionList(channel.getAddress());

    System.out.println("testing callRemoteMethods with exclusion list");
    for(int i=0; i < 10; i++) {
      MethodCall call=new MethodCall("print", new Object[] {i}, new Class[]{int.class});
      System.out.println("** Sending to " + otherMembers);
      rsp_list = disp.callRemoteMethods(otherMembers, call, opts);

      if (!rsp_list.isEmpty()) {
        System.out.println("** Responses: " + rsp_list);
      }
      Util.sleep(50);
    }

    if (!otherMembers.isEmpty()) {
      System.out.println("testing callRemoteMethod no exclusion list");
      opts=new RequestOptions(ResponseMode.GET_ALL, 5000);
      Address other = otherMembers.iterator().next();
      for(int i=0; i < 10; i++) {
        MethodCall call=new MethodCall("print", new Object[] {i}, new Class[]{int.class});
        System.out.println("** Sending to " + otherMembers);
        Integer integerResp = disp.callRemoteMethod(other, call, opts);

        if (integerResp!=null) {
          System.out.println("** Response: " + integerResp);
        }
        Util.sleep(50);
      }
    }

    //Enter data using BufferReader
    System.out.println("Press any key to continue...");
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    // Reading data using readLine
    reader.readLine();

    System.out.println("Closing channel...");
    channel.close();
    disp.stop();
    System.out.println("Bye!");
  }

  public static void main(String[] args) throws Exception {
    System.out.println("VERSION 3.6");
    String props = "udp.xml";
    String name=null;

    for(int i=0; i < args.length; i++) {
      if(args[i].equals("-props")) {
        props=args[++i];
        continue;
      }
      if(args[i].equals("-name")) {
        name=args[++i];
        continue;
      }
      help();
      return;
    }

    props="config.xml";
    System.out.println("props=>" + props);

    new RpcDemo().start(props, name);
  }

  protected static void help() {
    System.out.println("RPC [-props XML config] [-name name]");
  }
}