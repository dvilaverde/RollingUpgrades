package org.jgroups.demos;

import org.jgroups.*;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.common.demo.NumberPrinter;
import org.jgroups.protocols.upgrade.VersionHeader;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import java.util.Arrays;

public class RpcDemo extends ReceiverAdapter  {
  protected JChannel channel;
  private NumberPrinter np = new NumberPrinter();

  RpcDispatcher      disp;
  RspList rsp_list;

  public RpcDemo() {
  }

  public void viewAccepted(View new_view) {
    System.out.println("** view: " + new_view);
  }

  private void start(String props, String name, boolean nohup) throws Exception {
    RequestOptions opts=new RequestOptions(ResponseMode.GET_ALL, 5000);
    channel=new JChannel(props);
    disp=new RpcDispatcher(channel, np) {
      @Override
      public Object handle(Message req) throws Exception {
        VersionHeader vh = req.getHeader((short)2342);
        if (Version.getMajor(vh.version) == 3) {
          byte[] raw = req.getRawBuffer();
          byte[] buffer = Arrays.copyOfRange(raw, 4, raw.length);
          MethodCall call = methodCallFromBuffer(buffer, 0, buffer.length, null);
          return call.invoke(server_obj);
        }

        return super.handle(req);
      }
    };
    disp.setMembershipListener(this);
    channel.connect("RpcDispatcherTestGroup");
    Util.sleep(5000);
    for(int i=0; i < 10; i++) {
      rsp_list = disp.callRemoteMethods(null,
              "print",
              new Object[]{i},
              new Class[]{int.class},
              opts);

      System.out.println("Responses: " + rsp_list);
      Util.sleep(100);
    }
    Util.sleep(5000);

    channel.close();
    disp.stop();
  }

  public static void main(String[] args) throws Exception {
    System.out.println("VERSION 4.x");

    String props = "udp.xml";
    String name = null;
    boolean nohup = false;

    for(int i = 0; i < args.length; ++i) {
      if (args[i].equals("-props")) {
        ++i;
        props = args[i];
      } else if (args[i].equals("-name")) {
        ++i;
        name = args[i];
      } else {
        if (!args[i].equals("-nohup")) {
          help();
          return;
        }

        nohup = true;
      }
    }
    props="config.xml";
    System.out.println("props->" + props);

    (new RpcDemo()).start(props, name, nohup);
  }

  protected static void help() {
    System.out.println("Chat [-props XML config] [-name name] [-nohup]");
  }
}
