package org.jgroups.upgrade_server;

import io.grpc.stub.StreamObserver;

import java.util.*;
import java.util.UUID;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Bela Ban
 * @since  1.0.0
 * todo: Add logging instead of System.err.printf
 */
public class UpgradeService extends UpgradeServiceGrpc.UpgradeServiceImplBase {
    protected final ConcurrentMap<String,SynchronizedMap> members=new ConcurrentHashMap<>();
    protected long                                        view_id=0; // global, for all clusters, but who cares

    @Override
    public StreamObserver<Request> connect(StreamObserver<Response> responseObserver) {
        return new StreamObserver<Request>() {
            public void onNext(Request req) {
                if(req.hasMessage()) {
                    handleMessage(req.getMessage());
                    return;
                }
                if(req.hasJoinReq()) {
                    handleJoinRequest(req.getJoinReq(), responseObserver);
                    return;
                }
                if(req.hasLeaveReq()) {
                    handleLeaveRequest(req.getLeaveReq(), responseObserver);
                    return;
                }
                System.err.printf("request not known: %s\n", req);
            }

            public void onError(Throwable t) {
                remove(responseObserver);
            }

            public void onCompleted() {
                remove(responseObserver);
            }
        };
    }

    @Override
    public void leave(LeaveRequest req, StreamObserver<Void> responseObserver) {
        final String  cluster=req.getClusterName();
        boolean       removed=false;
        Address       leaver=req.getLeaver();

        if(leaver == null)
            return;

        SynchronizedMap m=members.get(cluster);
        if(m != null) {
            Map<Address,StreamObserver<Response>> map=m.getMap();
            Lock lock=m.getLock();
            lock.lock();
            try {
                StreamObserver<Response> observer=map.remove(leaver);
                if(observer != null) {
                    removed=true;
                    observer.onCompleted();
                }
                if(removed)
                    postView(map);
            }
            finally {
                lock.unlock();
            }
        }
        responseObserver.onNext(Void.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void dump(Void request, StreamObserver<DumpResponse> responseObserver) {
        String result=dumpDiagnostics();
        responseObserver.onNext(DumpResponse.newBuilder().setDump(result).build());
        responseObserver.onCompleted();
    }

    public void terminate() {
        Command c = Command.newBuilder().setTerminate(TerminateCommand.newBuilder().build()).build();
        final Response response = Response.newBuilder().setCommand(c).build();

        sendCommand(response);
    }

    public void deactivate() {
        Command c = Command.newBuilder().setActivate(ActivateCommand.newBuilder().setActive(false).build()).build();
        final Response response = Response.newBuilder().setCommand(c).build();

        sendCommand(response);
    }

    public void activate() {
        Command c = Command.newBuilder().setActivate(ActivateCommand.newBuilder().setActive(true).build()).build();
        final Response response = Response.newBuilder().setCommand(c).build();

        sendCommand(response);
    }

    private void sendCommand(Response response) {
        members.forEach((cluster, nodes) -> {
            Map<Address, StreamObserver<Response>> nodesMap = nodes.getMap();
            Lock l = nodes.getLock();
            l.lock();

            try {
                nodesMap.values().forEach(obs -> {
                    try {
                        obs.onNext(response);
                    } catch (Exception e) {
                        remove(obs);
                    }
                });
            } finally {
                l.unlock();
            }
        });
    }


    protected void remove(StreamObserver<Response> observer) {
        if(observer == null)
            return;

        for(Map.Entry<String,SynchronizedMap> entry : members.entrySet()) {
            String cluster=entry.getKey();
            SynchronizedMap m=entry.getValue();
            Map<Address,StreamObserver<Response>> map=m.getMap();
            Lock lock=m.getLock();
            lock.lock();
            try {
                map.values().removeIf(val -> Objects.equals(val, observer));
                if(map.isEmpty())
                    members.remove(cluster);
                else
                    postView(map);
            }
            finally {
                lock.unlock();
            }
        }
    }

    protected void handleJoinRequest(JoinRequest join_req, StreamObserver<Response> responseObserver) {
        final String  cluster=join_req.getClusterName();
        final Address joiner=join_req.getAddress();

        System.out.printf("Join request for cluster %s @ address: %s, uuid: %s\n",
                cluster,
                joiner.getName(),
                new UUID(joiner.getUuid().getMostSig(), joiner.getUuid().getLeastSig()).toString()
        );

        SynchronizedMap m=members.computeIfAbsent(cluster, k -> new SynchronizedMap(new LinkedHashMap()));
        Map<Address,StreamObserver<Response>> map=m.getMap();
        Lock lock=m.getLock();
        lock.lock();
        try {
            if(map.putIfAbsent(joiner, responseObserver) == null)
                postView(map);
        }
        finally {
            lock.unlock();
        }
    }

    protected void handleLeaveRequest(LeaveRequest leave_req, StreamObserver<Response> responseObserver) {
        final String  cluster=leave_req.getClusterName();
        boolean       removed=false;
        Address       leaver=leave_req.getLeaver();


        System.out.printf("Leave request for cluster %s @ address: %s, uuid: %s\n",
                cluster,
                leaver.getName(),
                new UUID(leaver.getUuid().getMostSig(), leaver.getUuid().getLeastSig()).toString()
        );

        if(leaver == null)
            return;

        SynchronizedMap m=members.get(cluster);
        if(m != null) {
            Map<Address,StreamObserver<Response>> map=m.getMap();
            Lock lock=m.getLock();
            lock.lock();
            try {
                StreamObserver<Response> observer=map.remove(leaver);
                if(observer != null) {
                    removed=true;
                    observer.onCompleted();
                }
                if(removed && !map.isEmpty())
                    postView(map);
            }
            finally {
                lock.unlock();
            }
        }
    }

    protected void handleMessage(Message msg) {
        String cluster=msg.getClusterName();
        Address dest=msg.hasDestination()? msg.getDestination() : null;

        SynchronizedMap mbrs=members.get(cluster);
        if(mbrs == null) {
            System.err.printf("no members found for cluster %s\n", cluster);
            return;
        }

        if(dest == null)
            relayToAll(msg, mbrs);
        else
            relayTo(msg, mbrs);
    }

    protected String getMethodName(Message msg)
    {
       if (msg.getMethodCall() != null && msg.getMethodCall().getPayload() !=null)
       {
          byte[] payload = msg.getMethodCall().getPayload().toByteArray();
          byte[] methodName = new byte[payload.length];

          for (int j=0; j < methodName.length; j++)
             methodName[j] = ' ';

          boolean nameStarted = false;
          for (int i=0,j=0; i < payload.length; i++)
          {
             if (Character.isLetter(payload[i]) || payload[i] == '.')
             {
                methodName[j++] = payload[i];
                nameStarted = true;
             }
             else if (nameStarted)
             {
                break;
             }
          }

          return new String(methodName).trim();
       }

       return "";
    }

    protected String getMemberNames(Set<Address> members, List<Address> exclusions)
    {
       StringBuilder builder = new StringBuilder("[");
       for (Address addr: members)
       {
          if (exclusions == null || ! exclusions.contains(addr))
          {
             if (builder.length() > 1)
                builder.append(", ");
             builder.append(addr.getName());
          }
       }
       builder.append("]");
       return builder.toString();
    }

    protected String getTimestamp()
    {
       SimpleDateFormat sdfDate = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss.SSS");
       Date now = new Date();
       String strDate = sdfDate.format(now);
       return strDate;
    }

    protected void relayToAll(Message msg, SynchronizedMap m) {
        Map<Address,StreamObserver<Response>> map=m.getMap();
        Lock lock=m.getLock();
        lock.lock();
        try {
            if(!map.isEmpty()) {
                Response response=Response.newBuilder().setMessage(msg).build();

                // need to honor the exclusion list in the header if present
                RpcHeader rpcHeader = msg.getRpcHeader();
                String memberNames = getMemberNames(map.keySet(), rpcHeader.getExclusionListList());
                System.out.printf("-- %s relaying all msg %s to members %s for cluster %s\n", getTimestamp(), getMethodName(msg), memberNames, msg.getClusterName());

                Set<Address> exclusions = new HashSet<>();
                if (rpcHeader.getExclusionListList() != null && !rpcHeader.getExclusionListList().isEmpty()) {
                    exclusions.addAll(rpcHeader.getExclusionListList());
                }

                System.out.printf("-- relaying msg to %d members for cluster %s\n", map.size() - exclusions.size(), msg.getClusterName());

                for(Map.Entry<Address, StreamObserver<Response>> node: map.entrySet()) {
                    StreamObserver<Response> obs = node.getValue();
                    try {
                        if (!exclusions.contains(node.getKey())) {
                            obs.onNext(response);
                        }
                    }
                    catch(Throwable t) {
                        System.out.printf("%s exception relaying message (removing observer): %s\n", getTimestamp(), t);
                        remove(obs);
                    }
                }
            }
        }
        finally {
            lock.unlock();
        }
    }

    protected void relayTo(Message msg, SynchronizedMap m) {
        Address dest=msg.getDestination();
        Map<Address,StreamObserver<Response>> map=m.getMap();
        Lock lock=m.getLock();
        lock.lock();
        try {
            StreamObserver<Response> obs=map.get(dest);
            if(obs == null) {
                System.err.printf("%s unicast destination %s not found; dropping message\n", getTimestamp(), dest.getName());
                return;
            }

            System.out.printf("-- %s relaying msg %s to member %s for cluster %s\n", getTimestamp(), getMethodName(msg), dest.getName(), msg.getClusterName());
            Response response=Response.newBuilder().setMessage(msg).build();
            try {
                obs.onNext(response);
            }
            catch(Throwable t) {
                System.err.printf("%s exception relaying message to %s (removing observer): %s\n", getTimestamp(), dest.getName(), t);
                remove(obs);
            }
        }
        finally {
            lock.unlock();
        }
    }


    protected void postView(Map<Address,StreamObserver<Response>> map) {
        if(map == null || map.isEmpty())
            return;
        View.Builder view_builder=View.newBuilder();
        Address coord=null;
        for(Address mbr: map.keySet()) {
            view_builder.addMember(mbr);
            if(coord == null)
                coord=mbr;
        }
        view_builder.setViewId(ViewId.newBuilder().setCreator(coord).setId(getNewViewId()).build());

        View new_view=view_builder.build();
        Response response=Response.newBuilder().setView(new_view).build();

        for(Iterator<Map.Entry<Address,StreamObserver<Response>>> it=map.entrySet().iterator(); it.hasNext();) {
            Map.Entry<Address,StreamObserver<Response>> entry=it.next();
            StreamObserver<Response>                    val=entry.getValue();
            try {
                val.onNext(response);
            }
            catch(Throwable t) {
                it.remove();
            }
        }
    }

    protected String dumpDiagnostics() {
        StringBuilder sb=new StringBuilder();
        sb.append("members:\n");
        dumpViews(sb);
        return sb.append("\n").toString();
    }

    protected void dumpViews(final StringBuilder sb) {
        for(Map.Entry<String,SynchronizedMap> entry: members.entrySet()) {
            String cluster=entry.getKey();
            SynchronizedMap m=entry.getValue();
            Map<Address,StreamObserver<Response>> map=m.getMap();
            Lock lock=m.getLock();
            lock.lock();
            try {
                sb.append(cluster).append(": ").append(Utils.print(map.keySet())).append("\n");
            }
            finally {
                lock.unlock();
            }
        }
    }

    protected synchronized long getNewViewId() {return view_id++;}


    protected static class SynchronizedMap {
        protected final Map<Address,StreamObserver<Response>> map;
        protected final Lock                                  lock=new ReentrantLock();

        public SynchronizedMap(Map<Address,StreamObserver<Response>> map) {
            this.map=map;
        }

        protected Map<Address,StreamObserver<Response>> getMap()       {return map;}
        protected Lock                                  getLock()      {return lock;}
    }

}
