package cs249;
import edu.sjsu.cs249.chain.*;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.CreateMode;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChainServer extends TailChainReplicaGrpc.TailChainReplicaImplBase implements Watcher {
	private ZooKeeper zk;
	private String zookeeper_ip;
	private String zk_directory;
	private String myIP;
	private int myPort;
	
	private Server server;
	private HashMap<String, Integer> datatable;
	private ArrayList<TailStateUpdateRequest> sentRequest;
	
	private String mySerial;
	private long xid;
	private boolean isHead;
	private boolean isTail;
	
    private long curPredecessor;
    private long curSuccessor;
    private long curTail;
    private TailChainReplicaGrpc.TailChainReplicaBlockingStub myPredecessor;
    private TailChainReplicaGrpc.TailChainReplicaBlockingStub mySuccessor;
    private TailChainReplicaGrpc.TailChainReplicaBlockingStub myTail;
    
    private boolean up2date;
	
    private AtomicBoolean retryChainCheck = new AtomicBoolean();
    
	public ChainServer(String _zkServer, String _directory, String _myIP, int _myPort) throws KeeperException, IOException, InterruptedException {
		
		this.server = ServerBuilder.forPort(_myPort).addService(this).build();
		this.zookeeper_ip = _zkServer;
		this.zk_directory = _directory;
		this.myIP = _myIP;
		this.myPort = _myPort;
		
		this.datatable = new HashMap<String, Integer>();
		this.sentRequest = new ArrayList<TailStateUpdateRequest>();
		
		this.up2date = false;
		this.xid = 0;
     
     
        new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(
                () -> { if (retryChainCheck.getAndSet(false)) checkPre_Suc();},
                5,
                5,
                TimeUnit.SECONDS
        );
        
	}
	
	private void registerWithZooKeeper() throws KeeperException, InterruptedException, IOException {
        this.zk = new ZooKeeper(this.zookeeper_ip, 2181, this);
        
        while ( (this.mySerial = String.valueOf(this.zk.getSessionId())).matches("0"))
        	;
        
        this.zk.create(this.zk_directory + "/" +Long.toHexString(this.zk.getSessionId()), (this.myIP+ ":" +String.valueOf(this.myPort)).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("Create "+ this.zk_directory + "/" + Long.toHexString(this.zk.getSessionId()));  
	}
	
	private boolean issueStateTransfer() throws InterruptedException {
		TailStateTransferRequest.Builder req = TailStateTransferRequest.newBuilder();
		req.putAllState(this.datatable);
		req.setSrc(Long.valueOf(this.mySerial));
		req.addAllSent(this.sentRequest);
		req.setStateXid(this.xid);
		
		try {
			System.out.println("Send state transfer to " + this.curSuccessor);
			ChainResponse rsp = this.mySuccessor.stateTransfer(req.build());
			if ( rsp.getRc() == 0) 
				return true;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			
		}
		
		return false;
	}
	
	public void cleanSentList() {
		
		if ( this.isHead && this.isTail ) {
			System.out.println("First if");
			this.sentRequest.clear();
			return;
		} // if
		
		if ( this.isTail || this.myTail == null) {
			System.out.println("Second if");
			return;
		}
		else {
			LatestXidRequest req = LatestXidRequest.newBuilder().build();
			LatestXidResponse rsp = this.myTail.getLatestXid(req);
			System.out.println("Clean Sent List tailXid: " + rsp.getXid());
			if ( rsp.getRc() == 0 ) {
				int tail_xid = (int) rsp.getXid();
				
				ArrayList<Integer> removeList = new ArrayList<Integer>();
				for (TailStateUpdateRequest tsur: this.sentRequest ) {
					if (tsur.getXid() <= tail_xid) {
						removeList.add(this.sentRequest.indexOf(tsur));
					} // if
					else {
						System.out.printf("Re-sending request with Xid:%d since the lastest tail xid is %d\n", tsur.getXid(), tail_xid );
						ChainResponse new_rps = this.mySuccessor.proposeStateUpdate(tsur);
						if ( new_rps.getRc() == 1)
							this.checkPre_Suc();
						else
							;
					} // else 
						
				} // for
				
				for ( int index: removeList) {
					System.out.printf("Removing %d from sentRequestList since the lastest tail xid is %d\n", this.sentRequest.get(index).getXid(), tail_xid);
					this.sentRequest.remove(index);
				} // for
			} // if
			else {
				this.checkPre_Suc();
			} // else
		} // else
	}
	
	private long getNewXid() {
		return this.xid ++;
	}
	
	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
        if (event.getState().equals(Event.KeeperState.Expired) || event.getState().equals(Event.KeeperState.Closed)) {
            System.err.println("disconnected from zookeeper");
            System.exit(2);
        }
        this.checkPre_Suc();
	} // process() for ZooKeeper event
	
	private int getValue(String _key) {
		if ( ! this.datatable.containsKey(_key)) {
			return 0;
		} // if 
		else {
			return (int) this.datatable.get(_key);
		} // else
	} // returnValue()
	
	private void incValue(String _key, int _value) {
		int target;
		if ( this.datatable.containsKey(_key)) {
			target = (int) this.datatable.get(_key) + _value;
		} // if 
		else  {
			 target = _value;
		} // else
		
		this.datatable.put(_key, target);

	}
	
	private void delValue(String _key) {
		if ( this.datatable.containsKey(_key)) {
			this.datatable.remove(_key);
		} // if
		else {
			;
		} // else
	}
	
	private void setValue(String _key, int _value) {
		this.datatable.put(_key, _value);		
	}
	
    private static InetSocketAddress str2addr(String addr) {
        int colon = addr.lastIndexOf(':');
        return new InetSocketAddress(addr.substring(0, colon), Integer.parseInt(addr.substring(colon+1)));
    }
    
    private TailChainReplicaGrpc.TailChainReplicaBlockingStub getStub(long session) throws KeeperException, InterruptedException {
        byte data[] = zk.getData(this.zk_directory + "/" + Long.toHexString(session), false, null);
        InetSocketAddress addr = str2addr(new String(data).split("\n")[0]);
        System.out.printf("The address :%s the port is %s\n", new String(data).split("\n")[0], addr.getPort());
        ManagedChannel channel = ManagedChannelBuilder.forAddress(addr.getHostName(), addr.getPort()).usePlaintext().build();
        return TailChainReplicaGrpc.newBlockingStub(channel);
    }

    private void setPredecessor(long session) throws KeeperException, InterruptedException {
        if (this.curPredecessor == session) {
            return;
        } // if
        else { 
        	this.up2date = false;
	        this.myPredecessor = null;
	        this.myPredecessor = getStub(session);
	        this.curPredecessor = session;
	        System.out.println( "My new predecessor is " + String.valueOf(session));
        } // else
    }
    
    private void setSuccessor(long session) throws KeeperException, InterruptedException {
        if (this.curSuccessor == session) {
            return;
        } // if
        else { 
	        this.mySuccessor = null;
	        this.mySuccessor = getStub(session);
	        this.curSuccessor = session;
	        /*
	        while ( ! this.up2date ) {
	        	System.out.println("Waiting for state transfer data, current xid:" + this.xid ) ;
	        	Thread.sleep(500);
	        } // while */
	       
	        this.issueStateTransfer();
        } // else
        
        System.out.println( "My successor is " + String.valueOf(session));
    }
	
    private void setTail(long session ) throws KeeperException, InterruptedException {
        if (this.curTail == session) {
            return;
        } // if
        else {
	        this.myTail = null;
	        this.myTail = getStub(session);
	        this.curTail = session;    	
        } // else
        
        System.out.println( "My tail is " + String.valueOf(session));
    }
    
    private void checkPre_Suc() {
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
                    // just skip non numbers
                }
            }
            sessions.sort(Long::compare);
            
            int index = sessions.indexOf(Long.valueOf(this.mySerial));
            
            if ( index == 0 && index == sessions.size()-1) {
            	System.out.println("I'm the only node.");
            	this.up2date = true;           	
            	this.isHead = true;
            	this.isTail = true;
            	
            	this.curPredecessor = -1;
            	this.myPredecessor = null;
            	this.curSuccessor = -1;
            	this.mySuccessor = null;
            	this.curTail = -1;
            	this.myTail = null;
            } // if 'I'm the only replica'
            else if ( index == 0) {
            	System.out.println("I'm the head.");
            	this.up2date = true;
            	this.isHead = true;
            	this.isTail = false;
            	
            	this.curPredecessor = -1;
            	this.myPredecessor = null;
            	this.setSuccessor(sessions.get(index+1));
                this.setTail(sessions.get(sessions.size()-1));
            } // else if 'I'm head'
            else if ( index == (sessions.size()-1) ) {
            	System.out.println("I'm the tail.");
            	
            	this.isHead = false;
            	this.isTail = true;
            	
            	this.curSuccessor = -1;
            	this.mySuccessor = null;
            	this.curTail = -1;
            	this.myTail = null;
	            this.setPredecessor(sessions.get(index-1));
            } // else if 'I'm tail'
            else {
            	System.out.println("I'm a replica.");
            	
            	this.isHead = false;
            	this.isTail = false;
            	
	            this.setPredecessor(sessions.get(index-1));
	            this.setSuccessor(sessions.get(index+1));
	            this.setTail(sessions.get(sessions.size()-1));
            } // else 
            

        } catch (KeeperException | InterruptedException e) {
            retryChainCheck.set(true);
        }
    }
	
	@Override
    public void proposeStateUpdate(edu.sjsu.cs249.chain.TailStateUpdateRequest request,
            io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.ChainResponse> responseObserver) {
		
		if ( request.getSrc() == this.curPredecessor ) {
			if (request.getXid() < this.xid) {
				ChainResponse rsp = ChainResponse.newBuilder().setRc(0).build();
				responseObserver.onNext(rsp);
				responseObserver.onCompleted();			
				return;
			} // if 
			
			long cxid = request.getCxid();
			String key = request.getKey();
			String host = request.getHost();
			int port = request.getPort();
			int value = request.getValue();
			long xid = request.getXid();
			
			System.out.printf("Receive update(XID: %d) set %s to %d\n", xid, key, value);
			
			this.setValue(key, value);
			this.xid = (int) xid;
			
			if ( this.isTail ) {
				CxidProcessedRequest finishClient = CxidProcessedRequest.newBuilder().setCxid((int) cxid).build();
		        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
		        
		        try {
					TailClientGrpc.newBlockingStub(channel).cxidProcessed(finishClient);
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} // if 
			else {
				TailStateUpdateRequest.Builder updateReq = TailStateUpdateRequest.newBuilder();
				
				updateReq.setCxid((int) cxid);
				updateReq.setKey(key);
				updateReq.setHost(host);
				updateReq.setPort(port);
				updateReq.setXid(xid);
				updateReq.setValue(this.getValue(key));
				updateReq.setSrc(Long.valueOf(this.mySerial));
				
				try {
					this.mySuccessor.proposeStateUpdate(updateReq.build());
					this.sentRequest.add(updateReq.build());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} // else
			
			ChainResponse rsp = ChainResponse.newBuilder().setRc(0).build();
			responseObserver.onNext(rsp);
			responseObserver.onCompleted();		
		} // if 
		else {
			ChainResponse rsp = ChainResponse.newBuilder().setRc(1).build();
			responseObserver.onNext(rsp);
			responseObserver.onCompleted();				
		}
    }

	@Override
    public void getLatestXid(edu.sjsu.cs249.chain.LatestXidRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.LatestXidResponse> responseObserver) {
		
		// set to 0 for either tail or non-tail
		LatestXidResponse.Builder response = LatestXidResponse.newBuilder().setRc(0);
		
		response.setXid(this.xid);
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
	}

	@Override
    public void stateTransfer(edu.sjsu.cs249.chain.TailStateTransferRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.ChainResponse> responseObserver) {
		
		System.out.println( "Receive STATETRANSFER from " + String.valueOf(request.getSrc()) );
		System.out.println( "My predecessor is " + String.valueOf(this.curPredecessor));
		
		if ( request.getSrc() == this.curPredecessor ) {
			this.xid = (int) request.getStateXid();
			
			List<TailStateUpdateRequest> state = request.getSentList();
			if ( ! state.isEmpty())
				this.sentRequest = new ArrayList<TailStateUpdateRequest>(request.getSentList());
			else 
				;
			this.datatable = new HashMap<String, Integer>(request.getStateMap());
			
			this.up2date = true;
			ChainResponse rsp = ChainResponse.newBuilder().setRc(0).build();
			responseObserver.onNext(rsp);
			responseObserver.onCompleted();
			System.out.println("State Transfer:" + this.up2date);
		} // if 
		else {
			ChainResponse rsp = ChainResponse.newBuilder().setRc(1).build();
			responseObserver.onNext(rsp);
			responseObserver.onCompleted();
		} // else

    }

	@Override
    public void increment(edu.sjsu.cs249.chain.TailIncrementRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.HeadResponse> responseObserver) {
		if (this.isHead) {
			long cxid = request.getCxid();
			String key = request.getKey();
			String host = request.getHost();
			int port = request.getPort();
			int value = request.getIncrValue();
			
			long xid = this.getNewXid();
			
			System.out.printf("Receive INCREMENT request with cxid:%d Key:%s IncValue:%d\n", cxid, key, value);
			this.incValue(key, value);
			System.out.printf("The value now is %d\n", this.getValue(key));
			
			HeadResponse rsp = HeadResponse.newBuilder().setRc(0).build();
			responseObserver.onNext(rsp);
			responseObserver.onCompleted();
			
			TailStateUpdateRequest.Builder updateReq = TailStateUpdateRequest.newBuilder();
			
			updateReq.setCxid((int) cxid);
			updateReq.setKey(key);
			updateReq.setHost(host);
			updateReq.setPort(port);
			updateReq.setXid(xid);
			updateReq.setValue(this.getValue(key));
			updateReq.setSrc(Long.valueOf(this.mySerial));
			
			if ( this.mySuccessor != null ) {
				try {
					this.mySuccessor.proposeStateUpdate(updateReq.build());
					this.sentRequest.add(updateReq.build());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} //if
			else if ( this.isTail) {
				CxidProcessedRequest finishClient = CxidProcessedRequest.newBuilder().setCxid((int) cxid).build();
		        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
		        
		        try {
					TailClientGrpc.newBlockingStub(channel).cxidProcessed(finishClient);
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}
			

		} // if 
		else {
			HeadResponse rsp = HeadResponse.newBuilder().setRc(1).build();
			responseObserver.onNext(rsp);
			responseObserver.onCompleted();
		} // else
    }

	@Override
    public void delete(edu.sjsu.cs249.chain.TailDeleteRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.HeadResponse> responseObserver) {
		if (this.isHead) {
			long cxid = request.getCxid();
			String key = request.getKey();
			String host = request.getHost();
			int port = request.getPort();
			
			long xid = this.getNewXid();
			System.out.printf("Receive DELETE request with cxid:%d Key:%s\n", cxid, key);
			this.delValue(key);
			
			TailStateUpdateRequest.Builder updateReq = TailStateUpdateRequest.newBuilder();
			
			updateReq.setCxid((int) cxid);
			updateReq.setKey(key);
			updateReq.setHost(host);
			updateReq.setPort(port);
			updateReq.setXid(xid);
			updateReq.setValue(this.getValue(key));
			updateReq.setSrc(Long.valueOf(this.mySerial));
			
			HeadResponse rsp = HeadResponse.newBuilder().setRc(0).build();
			responseObserver.onNext(rsp);
			responseObserver.onCompleted();			
			
			if ( this.mySuccessor != null) {
				try {
					this.mySuccessor.proposeStateUpdate(updateReq.build());
					this.sentRequest.add(updateReq.build());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} // if
			else if ( this.isTail) {
				CxidProcessedRequest finishClient = CxidProcessedRequest.newBuilder().setCxid((int) cxid).build();
		        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
		        
		        try {
					TailClientGrpc.newBlockingStub(channel).cxidProcessed(finishClient);
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}			

		} // if 
		else {
			HeadResponse rsp = HeadResponse.newBuilder().setRc(1).build();
			responseObserver.onNext(rsp);
			responseObserver.onCompleted();
		} // else
    }
	
	@Override
    public void get(edu.sjsu.cs249.chain.GetRequest request,
        io.grpc.stub.StreamObserver<edu.sjsu.cs249.chain.GetResponse> responseObserver) {
		
		GetResponse.Builder response = GetResponse.newBuilder();
		
		if (this.isTail) {
			response.setRc(0);
			response.setValue(this.getValue(request.getKey()));
		} // else if 'is tail'
		else {
			response.setRc(1);
		} // else 'neither head nor tail'
		
		responseObserver.onNext(response.build());
		responseObserver.onCompleted();
    }

	public boolean getIsTail() {
		return this.isTail;
	}
	
    public static void main(String args[]) throws Exception {
    	
    	// args[0]: ZooKeeper IP
    	// args[1]: ZooKeeper Directory
    	// args[2]: My IP
    	// args[3]:	My Port
     	ChainServer server = new ChainServer(args[0], args[1], args[2], Integer.valueOf(args[3]));
     	//ChainServer server = new ChainServer("127.0.0.1", "/tailchain","172.20.10.2", 60001);
        server.server.start(); 
        server.registerWithZooKeeper();
        
        
        while (true) {
        	if ( ! server.getIsTail())
				try {
					server.cleanSentList();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        	Thread.sleep(5000);
        }
        
        // server.server.awaitTermination();
    }
}
