package cs249;
import edu.sjsu.cs249.chain.*;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


public class ChainClient extends TailClientGrpc.TailClientImplBase implements Watcher {
	private Server server;
	
	private String host;
	private String zk_directory;
	private int port;
	
    private ZooKeeper zk;
    private int cxid;
    private HashMap<Integer,com.google.protobuf.GeneratedMessageV3> sentRequest;
    private long currentChainTail;
    private long currentChainHead;
    private TailChainReplicaGrpc.TailChainReplicaBlockingStub  chainTail;
    private TailChainReplicaGrpc.TailChainReplicaBlockingStub  chainHead;
    private AtomicBoolean retryChainCheck = new AtomicBoolean();

    private void setChainHead(long session) throws KeeperException, InterruptedException {
        if (currentChainHead == session) {
            return;
        }
        chainHead = null;
        chainHead = getStub(session);
        currentChainHead = session;
        System.out.println("Head is " + String.valueOf(session));
    }

    private TailChainReplicaGrpc.TailChainReplicaBlockingStub getStub(long session) throws KeeperException, InterruptedException {
        byte data[] = zk.getData( this.zk_directory + "/" + Long.toHexString(session), false, null);
        InetSocketAddress addr = str2addr(new String(data).split("\n")[0]);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(addr.getHostName(), addr.getPort()).usePlaintext().build();
        return TailChainReplicaGrpc.newBlockingStub(channel);
    }

    private void setChainTail(long session) throws KeeperException, InterruptedException {
        if (currentChainTail == session) {
            return;
        }
        chainTail = null;
        chainTail = getStub(session);
        currentChainTail = session;
        System.out.println("Tail is " + String.valueOf(session));
    }

    private static InetSocketAddress str2addr(String addr) {
        int colon = addr.lastIndexOf(':');
        return new InetSocketAddress(addr.substring(0, colon), Integer.parseInt(addr.substring(colon+1)));
    }
    
    private ChainClient(String zkServer, String _directory, String _myHost, int _myPort) throws KeeperException, InterruptedException, IOException {
    	this.host = _myHost;
    	this.port = _myPort;
    	
    	this.zk_directory = _directory;
		this.server = ServerBuilder.forPort(_myPort).addService(this).build();
		this.cxid = 0;
		this.sentRequest = new HashMap<Integer,com.google.protobuf.GeneratedMessageV3>();
        zk = new ZooKeeper(zkServer, 2181, this);
        
        while ( Long.toHexString(this.zk.getSessionId()).matches("0")) {
        	Thread.sleep(50);
        }
        
        System.out.println("ZooKeeper connected with session ID: " + Long.toHexString(this.zk.getSessionId()));
        
        this.chainHead = null;
        this.chainHead = null;
        
        checkHeadTail();
        new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(
                () -> { if (retryChainCheck.getAndSet(false)) checkHeadTail();},
                5,
                5,
                TimeUnit.SECONDS
        );
    }
    
    private int getNewCxid() {
    	return this.cxid ++;
    }

    private void get(String key) {
    	
    	if ( this.chainTail != null ) {
	        GetResponse rsp = chainTail.get(GetRequest.newBuilder().setKey(key).build());
	        if (rsp.getRc() != 0) {
	            System.out.printf("Error %d occurred\n", rsp.getRc());
	        } else {
	            System.out.printf("%d\n", rsp.getValue());
	        }
    	} // if 
    	else {
    		System.out.println("Tail is unavailable.");
    	}
    }

    private void del(String key) {
    	
    	if ( this.chainHead != null ) { 
	    	int value = this.getNewCxid();
	    	TailDeleteRequest rsq = TailDeleteRequest.newBuilder().setKey(key).setCxid(value).setHost(this.host).setPort(this.port).build();
	        this.sentRequest.put(value, rsq);
	        HeadResponse rsp = chainHead.delete(rsq);

	        
	        if (rsp.getRc() != 0) {
	            System.out.printf("Error %d occurred\n", rsp.getRc());
	        } else {
	            System.out.println("%s request accept with cxid:" + String.valueOf(value ));
	        }
    	} // if 
    	else {
    		System.out.println("Head is unavailable.");
    	}
    }

    private void inc(String key, int val) {
    	if ( this.chainHead != null ) {
	    	int value = this.getNewCxid();
	    	TailIncrementRequest req = TailIncrementRequest.newBuilder().setKey(key).setIncrValue(val).setCxid(value).setHost(this.host).setPort(this.port).build();
	        this.sentRequest.put(value, req);
	        HeadResponse rsp = chainHead.increment(req);

	        if (rsp.getRc() != 0) {
	            System.out.printf("Error %d occurred\n", rsp.getRc());
	        } else {
	            System.out.println("request accept with cxid:" + String.valueOf(value ));
	        }
    	} // if 
    	else {
    		System.out.println("Head is unavailable.");
    	}
    }

    @Override
    public void cxidProcessed(edu.sjsu.cs249.chain.CxidProcessedRequest request,
            io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.ChainResponse> responseObserver) {
    	int cxid = request.getCxid();
    	ChainResponse rsp;
    	if ( this.sentRequest.containsKey(cxid)) {
    		 rsp = ChainResponse.newBuilder().setRc(0).build();
    		 com.google.protobuf.GeneratedMessageV3 req = this.sentRequest.get(cxid);
    		 if ( req.getClass() == TailIncrementRequest.class) {
    			 System.out.printf("Increment Request with cxid:%d, Key:%s, Incr:%d\n", cxid, ((TailIncrementRequest) req).getKey(), ((TailIncrementRequest)req).getIncrValue());
    			 this.sentRequest.remove(cxid);
    		 } // if 
    		 else {
    			 System.out.printf("Delete Request with cxid:%d, Key:%s\n", cxid, ((TailDeleteRequest) req).getKey());
    			 this.sentRequest.remove(cxid);    			 
    		 } // else
    		 
    	} // if 
    	else {
    		rsp = ChainResponse.newBuilder().setRc(1).build();
    	} // else
    	
    	responseObserver.onNext(rsp);
    	responseObserver.onCompleted();
    }
    
    public static void main(String args[]) throws Exception {
    	
    	// args[0] for zookeeper IP, port is 2180 as default
    	// args[1] for zookeeper directory
    	// args[2] for local host for this client 
    	// args[3] for listening port for this client 
    	System.out.println("An application made by Ching-Chan Lee for CS249 HW2 Chain Replication-Tail");
    	ChainClient client = new ChainClient(args[0], args[1], args[2], Integer.valueOf(args[3]));
    	// ChainClient client = new ChainClient("10.10.10.1", "/tailchain_hello", "10.10.10.1", Integer.valueOf("60000"));
    	client.server.start();
        Scanner scanner = new Scanner(System.in);
        String line;
        System.out.println("Pleese Enter Command!");
        while ((line = scanner.nextLine()) != null) {
            String parts[] = line.split(" ");
            if (parts[0].equals("get")) {
                client.get(parts[1]);
            } else if (parts[0].equals("inc")) {
            	if ( parts.length == 3 ) 
            		client.inc(parts[1], Integer.parseInt(parts[2]));
            	else 
            		System.out.println("Incorrect format! >>INC KEY VALUE<<");
            } else if (parts[0].equals("del")) {
                client.del(parts[1]);
            } else {
                System.out.println("don't know " + parts[0]);
                System.out.println("i know:");
                System.out.println("get key");
                System.out.println("inc key value");
                System.out.println("del key");
            }
        }
        client.server.awaitTermination();
    }

    private void checkHeadTail() {
        try {
            List<String> children = zk.getChildren(this.zk_directory, true);
            ArrayList<Long> sessions = new ArrayList<Long>();
            for (String child: children) {
                try {
                	if ( child.startsWith("0x") ) 
                		child = child.substring(2);
                	System.out.println(Long.parseLong(child, 16));
                    sessions.add(Long.parseLong(child, 16));
                } catch (NumberFormatException e) {
                	e.printStackTrace();
                }
            }
            
            if ( sessions.size() == 0) {
            	return ;
            } // if
            else if ( sessions.size() == 1 ) {
	            long head = sessions.get(0);
	            long tail = sessions.get(children.size() - 1);
	            setChainHead(head);
	            setChainTail(tail);
            } // else if 
            else {
            	System.out.println("Enter else");
	            sessions.sort(Long::compare);
	            long head = sessions.get(0);
	            long tail = sessions.get(children.size() - 1);
	            setChainHead(head);
	            setChainTail(tail);
            } // else 
        } catch (KeeperException | InterruptedException e) {
            retryChainCheck.set(true);
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getState().equals(Event.KeeperState.Expired) || watchedEvent.getState().equals(Event.KeeperState.Closed)) {
            System.err.println("disconnected from zookeeper");
            System.exit(2);
        }
        checkHeadTail();
    }
}
