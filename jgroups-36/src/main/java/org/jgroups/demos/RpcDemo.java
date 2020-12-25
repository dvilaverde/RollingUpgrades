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


public class RpcDemo extends ReceiverAdapter {
  JChannel           channel;
  RpcDispatcher      disp;
  RspList            rsp_list;
  private NumberPrinter np = new NumberPrinter();

  public void viewAccepted(View new_view) {
    System.out.println("** view: " + new_view);
  }

  public void start(String props, String name) throws Exception {

    RequestOptions opts=new RequestOptions(ResponseMode.GET_ALL, 5000);
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
    for(int i=0; i < 10; i++) {
      MethodCall call=new MethodCall("print", new Object[] {i}, new Class[]{int.class});
      rsp_list = disp.callRemoteMethods(null, call, opts);

      System.out.println("Responses: " + rsp_list);
      Util.sleep(100);
    }
    Util.sleep(5000);

    channel.close();
    disp.stop();
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