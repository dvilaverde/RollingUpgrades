package org.jgroups.protocols.upgrade;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.jgroups.*;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.RequestCorrelator;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.upgrade_server.*;
import org.jgroups.stack.Protocol;
import org.jgroups.upgrade_server.View;
import org.jgroups.upgrade_server.ViewId;
import org.jgroups.util.Bits;
import org.jgroups.util.NameCache;
import org.jgroups.util.UUID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Relays application messages to the UpgradeServer (when active). Should be the top protocol in a stack.
 * @author Bela Ban
 * @since  1.0
 * @todo: implement support for addresses other than UUIDs
 * @todo: implement reconnection to server (server went down and then up again)
 */
@MBean(description="Protocol that redirects all messages to/from an UpgradeServer")
public class UPGRADE extends Protocol {

    @Property(description="Whether or not to perform relaying via the UpgradeServer",writable=false)
    protected volatile boolean                          active;

    @Property(description="The IP address (or symbolic name) of the UpgradeServer")
    protected String                                    server_address="localhost";

    @Property(description="The port on which the UpgradeServer is listening")
    protected int                                       server_port=50051;

    @ManagedAttribute(description="The local address")
    protected Address                                   local_addr;

    @ManagedAttribute(description="Shows the local view")
    protected org.jgroups.View                          local_view;

    @ManagedAttribute(description="The global view (provided by the UpgradeServer)")
    protected org.jgroups.View                          global_view;

    @ManagedAttribute(description="The cluster this member is a part of")
    protected String                                    cluster;
    protected ManagedChannel                            channel;
    protected UpgradeServiceGrpc.UpgradeServiceStub     asyncStub;
    protected StreamObserver<Request>                   send_stream; // for sending of messages and join requests

    protected static final short                        REQ_ID=ClassConfigurator.getProtocolId(RequestCorrelator.class);

    @ManagedOperation(description="Enable forwarding and receiving of messages to/from the UpgradeServer")
    public synchronized void activate() {
        if(!active) {
            connect(cluster);
            active=true;
        }
    }

    @ManagedOperation(description="Disable forwarding and receiving of messages to/from the UpgradeServer")
    public synchronized void deactivate() {
        if(active) {
           disconnect();
           active=false;
        }
    }


    public void start() throws Exception {
        super.start();
        channel=ManagedChannelBuilder.forAddress(server_address, server_port)
          .usePlaintext()
          .build();
        asyncStub=UpgradeServiceGrpc.newStub(channel);
    }

    public void stop() {
        super.stop();
        channel.shutdown();
        try {
            channel.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // Ignore
        }
    }


    public Object down(Event evt) {
        switch(evt.type()) {
            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.arg();
                break;
            case Event.CONNECT:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                cluster=evt.arg();
                Object ret=down_prot.down(evt);
                if(active)
                    connect(cluster);
                return ret;
            case Event.DISCONNECT:
                ret=down_prot.down(evt);
                disconnect();
                return ret;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        switch(evt.type()) {
            case Event.VIEW_CHANGE:
                local_view=evt.arg();
                if(active)
                    return null;
                break;
        }
        return up_prot.up(evt);
    }

    public Object down(Message msg) {
        if(!active)
            return down_prot.down(msg);

        // else send to UpgradeServer
        if(send_stream != null) {
            if(msg.getSrc() == null)
                msg.setSrc(local_addr);
            Request req=Request.newBuilder().setMessage(jgroupsMessageToProtobufMessage(cluster, msg)).build();
            synchronized (send_stream)
            {
                send_stream.onNext(req);
            }
        }
        return null;
    }

    protected synchronized void connect(String cluster) {
        send_stream=asyncStub.connect(new StreamObserver<Response>() {
            public void onNext(Response rsp) {
                if(rsp.hasMessage()) {
                    handleMessage(rsp.getMessage());
                    return;
                }
                if(rsp.hasView()) {
                    handleView(rsp.getView());
                    return;
                }
                if (rsp.hasCommand()) {
                    if (rsp.getCommand().hasActivate()) {
                        log.warn("disabled UPGRADE protocol as requested by upgrade server.");
                        active = rsp.getCommand().getActivate().getActive();
                    }

                    if (rsp.getCommand().hasTerminate()) {
                        log.warn("shutdown UPGRADE protocol as requested by upgrade server.");
                        active = rsp.getCommand().getActivate().getActive();
                        disconnect();
                    }
                    return;
                }
                throw new IllegalStateException(String.format("response is illegal: %s", rsp));
            }

            public void onError(Throwable t) {
                log.error("exception from server: %s", t);
            }

            public void onCompleted() {
                log.debug("server is done");
            }
        });
        org.jgroups.upgrade_server.Address pbuf_addr=jgroupsAddressToProtobufAddress(local_addr);
        JoinRequest join_req=JoinRequest.newBuilder().setAddress(pbuf_addr).setClusterName(cluster).build();
        Request req=Request.newBuilder().setJoinReq(join_req).build();
        send_stream.onNext(req);
    }

    protected synchronized void disconnect() {
        if(send_stream != null) {
            if(local_addr != null && cluster != null) {
                org.jgroups.upgrade_server.Address local=jgroupsAddressToProtobufAddress(local_addr);
                LeaveRequest leave_req=LeaveRequest.newBuilder().setClusterName(cluster).setLeaver(local).build();
                Request request=Request.newBuilder().setLeaveReq(leave_req).build();
                send_stream.onNext(request);
            }
            send_stream.onCompleted();
        }
        global_view=null;
    }

    protected void handleView(View view) {
        // System.out.printf("received view %s\n", print(view));
        org.jgroups.View jg_view=protobufViewToJGroupsView(view);
        global_view=jg_view;
        up_prot.up(new Event(Event.VIEW_CHANGE, jg_view));

    }

    protected void handleMessage(org.jgroups.upgrade_server.Message m) {
        // System.out.printf("received message %s\n", print(m));
        Message msg=protobufMessageToJGroupsMessage(m);
        up_prot.up(msg);
    }

    protected static org.jgroups.upgrade_server.Address jgroupsAddressToProtobufAddress(Address jgroups_addr) {
        if(jgroups_addr == null)
            return org.jgroups.upgrade_server.Address.newBuilder().build();
        if(!(jgroups_addr instanceof org.jgroups.util.UUID))
            throw new IllegalArgumentException(String.format("JGroups address has to be of type UUID but is %s",
                                                             jgroups_addr.getClass().getSimpleName()));
        UUID uuid=(UUID)jgroups_addr;
        String name=NameCache.get(jgroups_addr);

        org.jgroups.upgrade_server.UUID pbuf_uuid=org.jgroups.upgrade_server.UUID.newBuilder()
          .setLeastSig(uuid.getLeastSignificantBits()).setMostSig(uuid.getMostSignificantBits()).build();
        return org.jgroups.upgrade_server.Address.newBuilder().setUuid(pbuf_uuid).setName(name).build();
    }

    protected static Address protobufAddressToJGroupsAddress(org.jgroups.upgrade_server.Address pbuf_addr) {
        if(pbuf_addr == null)
            return null;
        org.jgroups.upgrade_server.UUID pbuf_uuid=pbuf_addr.hasUuid()? pbuf_addr.getUuid() : null;
        UUID uuid = pbuf_uuid == null? null : new UUID(pbuf_uuid.getMostSig(), pbuf_uuid.getLeastSig());
        if (uuid != null) {
            NameCache.add(uuid, pbuf_addr.getName());
        }
        return uuid;
    }

    protected static org.jgroups.upgrade_server.Message jgroupsMessageToProtobufMessage(String cluster, Message jgroups_msg) {
        if(jgroups_msg == null)
            return null;
        Address destination=jgroups_msg.getDest(), sender=jgroups_msg.getSrc();
        byte[] payload=jgroups_msg.getBuffer();
        RequestCorrelator.Header hdr=jgroups_msg.getHeader(REQ_ID);

        org.jgroups.upgrade_server.Message.Builder msg_builder=org.jgroups.upgrade_server.Message.newBuilder()
          .setClusterName(cluster);
        if(destination != null)
            msg_builder.setDestination(jgroupsAddressToProtobufAddress(destination));
        if(sender != null)
            msg_builder.setSender(jgroupsAddressToProtobufAddress(sender));
        if(payload != null && hdr == null)
            msg_builder.setPayload(ByteString.copyFrom(payload));
        if(hdr != null) {
            // set the RPC header when sending RPC message out to another node in the cluster.
            RpcHeader pbuf_hdr=jgroupsReqHeaderToProtobufRpcHeader(hdr);
            msg_builder.setRpcHeader(pbuf_hdr);

            if (pbuf_hdr.getType() == RequestCorrelator.Header.REQ) {
                // Sending down RPC message to cluster, the payload[] should be a MethodCall.
                // Convert it to the protobuf MethodCall and let UPGRADE protocol on receiving
                // end re-assemble to the correct byte[].
                // In JGroups 4 the payload will have just the mode byte before the MethodCall
                // payload as follows
                // byte[0]    = mode (ID = 3, TYPES = 2, METHOD = 1)
                MethodCall.Builder builder = MethodCall.newBuilder();
                short mode = (short) Bits.makeInt(payload, 0, 1);
                builder.setMode(mode)
                        .setPayload(ByteString.copyFrom(payload, 1, payload.length - 1));
                msg_builder.setMethodCall(builder.build());
            } else {
                msg_builder.setPayload(ByteString.copyFrom(payload));
            }
        }

        // set the JGroups version in the protocol message
        Metadata meta = Metadata.newBuilder().setVersion(Version.version).build();
        msg_builder.setMetaData(meta);
        return msg_builder.build();
    }

    protected static Message protobufMessageToJGroupsMessage(org.jgroups.upgrade_server.Message msg) {
        Message jgroups_mgs=new Message();
        if(msg.hasDestination())
            jgroups_mgs.setDest(protobufAddressToJGroupsAddress(msg.getDestination()));
        if(msg.hasSender())
            jgroups_mgs.setSrc(protobufAddressToJGroupsAddress(msg.getSender()));

        ByteString payload=msg.getPayload();

        if(!payload.isEmpty())
            jgroups_mgs.setBuffer(payload.toByteArray());
        if(msg.hasRpcHeader()) {
            RequestCorrelator.Header hdr=protobufRpcHeaderToJGroupsReqHeader(msg.getRpcHeader());
            jgroups_mgs.putHeader(REQ_ID, hdr);

            if (hdr.type != RequestCorrelator.Header.REQ) {
                // handle the RPC response which has been potentially serialized differently by
                // different version of JGroups and may not be binary compatible.
                //
                // response types are serialized using Util.objectToBuffer and are using the same
                // leading byte between JGroups 3.6 and 4.x. One exception is that for type
                // String and byte[] the JGroups 4 version of the Util will send the length of the
                // array, where-as JGroups 3.x does not. It seems as JGroups 3.6 is sending
                // Util.objectToBuffer() and JGroups 4.x reads using Util.objectToStream() which
                // requires the length of the stream.

                // so first detect version of JGroups sending reply and if it is 3.x need to
                // add the length of the byte[] to the current byte array
                // since we know this version of UPGRADE protocol doesn't support JGroups 5.x
                // assume it's JGroups 3.6
                boolean compatible = Version.isBinaryCompatible((short) msg.getMetaData().getVersion());
                if (!compatible) {
                    if (payload.byteAt(0) == 19){
                        // handling byte[]
                        byte[] bytes = payload.toByteArray();
                        byte[] asStream = new byte[payload.size() + 4]; // add 4 bytes for the length int
                        asStream[0] = payload.byteAt(0);
                        bytes = Arrays.copyOfRange(bytes, 1, payload.size());
                        Bits.writeInt(bytes.length, asStream, 1);
                        // start at the 5th byte since the first 5 bytes are already
                        // allocated ( type (1-byte) + len (4-bytes) )
                        System.arraycopy(bytes, 0, asStream, 5, bytes.length);
                        jgroups_mgs.setBuffer(asStream);
                    } else if (payload.byteAt(0) == 18 || payload.byteAt(0) == 21) {
                        // String type will either be ascii (18) or UTF (21)
                        // format is:
                        // byte[0] = 18 (ascii) or 21 (UTF)
                        // byte[1...] = bytes representing string
                        // JGroups 4 just needs everything starting from byte[1]

                        // jgroups Util.objectToStream() wants string len for ascii
                        if (payload.byteAt(0) == 18) {
                            byte[] bytes = payload.toByteArray();
                            byte[] asStream = new byte[payload.size() + 4]; // add 4 bytes for the length int
                            asStream[0] = payload.byteAt(0);
                            bytes = Arrays.copyOfRange(bytes, 1, payload.size());
                            Bits.writeInt(bytes.length, asStream, 1);
                            // start at the 5th byte since the first 5 bytes are already
                            // allocated ( type (1-byte) + len (4-bytes) )
                            System.arraycopy(bytes, 0, asStream, 5, bytes.length);
                            jgroups_mgs.setBuffer(asStream);
                        } else {
                            jgroups_mgs.setBuffer(Arrays.copyOfRange(payload.toByteArray(), 1, payload.size()));
                        }
                    } else {
                        // no change needed, as primitive types are handled the same between both versions.
                    }
                }
            }

            // before sending the MethodCall up to the application convert the gRpc version
            // back to the a byte buffer recognized by the current version of JGroups.
            if (msg.hasMethodCall())
                jgroups_mgs.setBuffer(protobufMethodCallToBuffer(msg.getMethodCall()));
        }

        return jgroups_mgs;
    }

    protected static byte[] protobufMethodCallToBuffer(MethodCall m) {
        byte[] buffer = new byte[m.getPayload().size() + 1];
        buffer[0] = (byte) m.getMode(); // mode
        m.getPayload().copyTo(buffer, 1);
        return buffer;
    }

    protected static org.jgroups.View protobufViewToJGroupsView(View v) {
        ViewId pbuf_vid=v.getViewId();
        List<org.jgroups.upgrade_server.Address> pbuf_mbrs=v.getMemberList();
        org.jgroups.ViewId jg_vid=new org.jgroups.ViewId(protobufAddressToJGroupsAddress(pbuf_vid.getCreator()),
                                                         pbuf_vid.getId());
        List<Address> members=new ArrayList<>();
        pbuf_mbrs.stream().map(UPGRADE::protobufAddressToJGroupsAddress).forEach(members::add);
        return new org.jgroups.View(jg_vid, members);
    }

    protected static RpcHeader jgroupsReqHeaderToProtobufRpcHeader(RequestCorrelator.Header hdr) {
        RpcHeader.Builder builder = RpcHeader.newBuilder().setType(hdr.type).setRequestId(hdr.req_id).setCorrId(hdr.corrId);

        if (hdr instanceof RequestCorrelator.MultiDestinationHeader) {
            RequestCorrelator.MultiDestinationHeader mdhdr = (RequestCorrelator.MultiDestinationHeader) hdr;

            Address[] exclusions = mdhdr.exclusion_list;
            if (exclusions != null && exclusions.length > 0) {
                builder.addAllExclusionList(Arrays.stream(exclusions).map(UPGRADE::jgroupsAddressToProtobufAddress).collect(Collectors.toList()));
            }
        }

        return builder.build();
    }

    protected static RequestCorrelator.Header protobufRpcHeaderToJGroupsReqHeader(RpcHeader hdr) {
        byte type=(byte)hdr.getType();
        long request_id=hdr.getRequestId();
        short corr_id=(short)hdr.getCorrId();
        return (RequestCorrelator.Header)new RequestCorrelator.Header(type, request_id, corr_id).setProtId(REQ_ID);
    }

    protected static String print(org.jgroups.upgrade_server.Message msg) {
        return String.format("cluster: %s sender: %s dest: %s %d bytes\n", msg.getClusterName(),
                             msg.hasDestination()? msg.getDestination().getName() : "null",
                             msg.hasSender()? msg.getSender().getName() : "null",
                             msg.getPayload().isEmpty()? 0 : msg.getPayload().size());
    }

    public static String print(View v) {
        if(v.hasViewId()) {
            ViewId view_id=v.getViewId();
            return String.format("%s|%d [%s]",
                          view_id.getCreator().getName(), view_id.getId(),
                          v.getMemberList().stream().map(org.jgroups.upgrade_server.Address::getName)
                                   .collect(Collectors.joining(", ")));
        }
        return String.format("[%s]",
                             v.getMemberList().stream().map(org.jgroups.upgrade_server.Address::getName)
                               .collect(Collectors.joining(", ")));
    }


}
