package org.jgroups.protocols.upgrade;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.Version;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.RequestCorrelator;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.jgroups.upgrade_server.JoinRequest;
import org.jgroups.upgrade_server.LeaveRequest;
import org.jgroups.upgrade_server.Metadata;
import org.jgroups.upgrade_server.MethodCall;
import org.jgroups.upgrade_server.Request;
import org.jgroups.upgrade_server.Response;
import org.jgroups.upgrade_server.RpcHeader;
import org.jgroups.upgrade_server.UpgradeServiceGrpc;
import org.jgroups.upgrade_server.View;
import org.jgroups.upgrade_server.ViewId;
import org.jgroups.util.Bits;
import org.jgroups.util.UUID;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

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

    @Property(description = "Whether or not to connect to upgrade server on startup. The protocol will be active based on active property. This allows for connected but not active",writable = false)
    protected volatile boolean connectOnStart;

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

    protected static final short                        REQ_ID= ClassConfigurator.getProtocolId(RequestCorrelator.class);

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
        try
        {
           channel.awaitTermination(30, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
           e.printStackTrace();
        }
    }

    public Object down(Event evt) {
        switch(evt.type()) {
            case Event.MSG:
                if(!active)
                    return down_prot.down(evt);
                // else send to UpgradeServer
                if(send_stream != null) {
                    Message msg = (Message) evt.getArg();
                    if (msg.getSrc() == null)
                        msg.setSrc(local_addr);
                    Request req = Request.newBuilder().setMessage(jgroupsMessageToProtobufMessage(cluster, msg)).build();
                    synchronized (send_stream) {
                        send_stream.onNext(req);
                    }
                }
                return null;
            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.arg();
                break;
            case Event.CONNECT:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                cluster=evt.arg();
                Object ret=down_prot.down(evt);
                if(active || connectOnStart)
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
                        active = rsp.getCommand().getActivate().getActive();
                        log.warn(String.format("activation command (%s) UPGRADE protocol as requested by upgrade server.", active));
                    }

                    if (rsp.getCommand().hasTerminate()) {
                        log.warn("shutdown UPGRADE protocol as requested by upgrade server.");
                        active = false;
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
        Message msg = protobufMessageToJGroupsMessage(m);
        up_prot.up(new Event(Event.MSG, msg));
    }

    protected static org.jgroups.upgrade_server.Address jgroupsAddressToProtobufAddress(Address jgroups_addr) {
        if(jgroups_addr == null)
            return org.jgroups.upgrade_server.Address.newBuilder().build();
        if(!(jgroups_addr instanceof org.jgroups.util.UUID))
            throw new IllegalArgumentException(String.format("JGroups address has to be of type UUID but is %s",
                                                             jgroups_addr.getClass().getSimpleName()));
        UUID uuid=(UUID)jgroups_addr;
        String name=UUID.get(jgroups_addr);

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
            UUID.add(uuid, pbuf_addr.getName());
        }
        return uuid;
    }

    protected static org.jgroups.upgrade_server.Message jgroupsMessageToProtobufMessage(String cluster, Message jgroups_msg) {
        if(jgroups_msg == null)
            return null;
        Address destination=jgroups_msg.getDest(), sender=jgroups_msg.getSrc();
        byte[] payload=jgroups_msg.getBuffer();

        Header hdr = jgroups_msg.getHeader(REQ_ID);

        org.jgroups.upgrade_server.Message.Builder msg_builder=org.jgroups.upgrade_server.Message.newBuilder()
          .setClusterName(cluster);
        if(destination != null)
            msg_builder.setDestination(jgroupsAddressToProtobufAddress(destination));
        if(sender != null)
            msg_builder.setSender(jgroupsAddressToProtobufAddress(sender));
        // only write the payload for non-rpc messages
        if(payload != null && hdr == null)
            msg_builder.setPayload(ByteString.copyFrom(payload));
        if(hdr != null) {
            RpcHeader pbuf_hdr=jgroupsReqHeaderToProtobufRpcHeader((RequestCorrelator.Header)  hdr);
            msg_builder.setRpcHeader(pbuf_hdr);

            if (pbuf_hdr.getType() == RequestCorrelator.Header.REQ) {
                // Sending down RPC message to cluster, the payload[] should be a MethodCall.
                // convert it to the protobuf MethodCall and let UPGRADE protocol on receiving
                // end re-assemble to the correct byte[].
                // In JGroups 3.6 the payload will have the first 4 bytes before the MethodCall
                // payload as follows
                // byte[0]   = 1 if Method call implements the Streamable interface
                // byte[1]   = 0 if there is no payload, 1 if there is a payload
                // byte[2-3] = short value defining the magic_number number of the class, value seems
                //             when sending messages between versions jgroups
                // byte[4-N]  = MethodCall payload, including method_name, args and types
                // the 5th byte is part of MethodCall but is the method call mode
                // byte[5]    = mode (ID = 3, TYPES = 2, METHOD = 1)

                // check if there is a payload, skip the streamable interface byte
                int hasPayload = Bits.makeInt(payload, 1,1);
                MethodCall.Builder builder = MethodCall.newBuilder();
                if (hasPayload == 1) {
                    // only send the mode and payload (bytes) over gRpc, the receive will fill in the blanks
                    short mode = (short) Bits.makeInt(payload, 4, 1);
                    builder.setMode(mode)
                            .setPayload(ByteString.copyFrom(payload, 5, payload.length - 5));
                    msg_builder.setMethodCall(builder.build());
                } else {
                    msg_builder.setPayload(ByteString.copyFrom(payload));
                }
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

                // so first detect version of JGroups sending reply and if it is 4.x need to
                // remove the length of the byte[] to the current byte array
                // since we know this version of UPGRADE protocol doesn't support JGroups 5.x
                // assume it's JGroups 3.6
                boolean compatible = Version.isBinaryCompatible((short) msg.getMetaData().getVersion());
                if (!compatible) {
                    // got a message from 4.x node
                    byte type = payload.byteAt(0);
                    if (type == 19){
                        // handling byte[]
                        byte[] bytes = payload.toByteArray();
                        byte[] asBuffer = new byte[payload.size() - 4]; // subtract 4 bytes for the length int
                        asBuffer[0] = type;
                        bytes = Arrays.copyOfRange(bytes, 5, payload.size());
                        // start at the 2nd byte since the first byte is already
                        // allocated ( type (1-byte) )
                        System.arraycopy(bytes, 0, asBuffer, 1, bytes.length);
                        jgroups_mgs.setBuffer(asBuffer);
                    } else if ( type == 18 || type == 21) {
                        // String type will either be ascii (18) or UTF (21)
                        // if type == 18 there will a string length
                        // byte[1-4] if byte 1 is 18 then there will be a length, otherwise byte 2 is a UTF string

                        if (type == 18) {
                            // strip off the length as JGroups 3 doesn't need it.
                            byte[] bytes = payload.toByteArray();
                            byte[] asBuffer = new byte[payload.size() - 4]; // subtract 4 bytes for the length int
                            asBuffer[0] = type;
                            bytes = Arrays.copyOfRange(bytes, 5, payload.size());
                            // start at the 2nd byte since the first byte is already
                            // allocated ( type (1-byte) )
                            System.arraycopy(bytes, 0, asBuffer, 1, bytes.length);
                            jgroups_mgs.setBuffer(asBuffer);
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
        byte[] buffer;
        if (m.getPayload().isEmpty()) {
            buffer = new byte[2];
            buffer[0] = 1; // Streamable type
            buffer[1] = 0; // no payload, Method call was null from sender's RpcDispatcher.
        } else {
            buffer = new byte[m.getPayload().size() + 5];
            buffer[0] = 1; // Streamable type
            buffer[1] = 1; // method call is not null
            short magic_number =ClassConfigurator.getMagicNumber(org.jgroups.blocks.MethodCall.class);
            Bits.writeShort(magic_number, buffer, 2);
            buffer[4] = (byte) m.getMode();
            m.getPayload().copyTo(buffer, 5);
        }
        return buffer;
    }

    protected static org.jgroups.View protobufViewToJGroupsView(View v) {
        ViewId pbuf_vid=v.getViewId();
        List<org.jgroups.upgrade_server.Address> pbuf_mbrs=v.getMemberList();
        org.jgroups.ViewId jg_vid=new org.jgroups.ViewId(protobufAddressToJGroupsAddress(pbuf_vid.getCreator()),
                                                         pbuf_vid.getId());
        Collection<Address> members=new ArrayList<>();
        pbuf_mbrs.stream().map(UPGRADE::protobufAddressToJGroupsAddress).forEach(members::add);
        return new org.jgroups.View(jg_vid, members);
    }

    protected static RpcHeader jgroupsReqHeaderToProtobufRpcHeader(RequestCorrelator.Header hdr) {
        RpcHeader.Builder rpcHeader = RpcHeader.newBuilder().setType(hdr.type).setRequestId(hdr.req_id).setCorrId(hdr.corrId);
        if (hdr instanceof RequestCorrelator.MultiDestinationHeader) {
            RequestCorrelator.MultiDestinationHeader mdhdr = (RequestCorrelator.MultiDestinationHeader) hdr;

            Address[] exclusions = mdhdr.exclusion_list;
            if (exclusions != null && exclusions.length > 0) {
                rpcHeader.addAllExclusionList(Arrays.stream(exclusions).map(UPGRADE::jgroupsAddressToProtobufAddress).collect(Collectors.toList()));
            }
        }

        return rpcHeader.build();
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
