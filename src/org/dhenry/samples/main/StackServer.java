package org.dhenry.samples.main;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Comparator;
import java.util.Deque;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StackServer {

	public static final int STACK_MAX_SIZE = 100;
	public static final int CONN_MAX_COUNT = 100;
	public static final long TEN_MINUTES_IN_MS = 10 * 60 * 1000;
	
	private static final Logger log = Logger.getLogger(StackServer.class.getName());
	
	private Deque<Message> stack = new LinkedBlockingDeque<>();
	private Queue<ConnWaitingState> waitingQueue =
		new PriorityBlockingQueue<>(STACK_MAX_SIZE, getStateAgeComparator());
	private Server server;
	private LongAdder stackSize; // instead of stack.size which is not a constant-time op.
	private LongAdder connCount;
	private Object pushBlockLock = new Object();
	private Object popBlockLock = new Object();
	private boolean running = true;
	
	public StackServer() {
		stackSize = new LongAdder();
		connCount = new LongAdder();
		server = new Server();
		server.setDaemon(true);
	}
	
	public void run() {
		server.start();
	}

	private class Message {

		byte[] data;

		public Message(byte[] data) {
			this.data = data;
		}
	}
	
	private class ConnWaitingState {
		
		long createTime;
		Lock lock;

		public ConnWaitingState() {
			lock = new ReentrantLock();
			createTime = System.currentTimeMillis();
		}
	}

	private void handlePush(int header, InputStream in, OutputStream out) throws IOException {
		connCount.increment();
		byte[] data = new byte[header & 0x7f];
		DataInputStream din = new DataInputStream(in);
		din.readFully(data);
		Message m = new Message(data);
		ConnWaitingState state = new ConnWaitingState();
		waitingQueue.offer(state);
		int status = 0;
		synchronized (pushBlockLock) {
			while (stackSize.intValue() >= STACK_MAX_SIZE) {
				try {
					status = din.read();
				} catch (SocketTimeoutException ex) {
					status = -1;
				}
				log.log(Level.INFO, "push status: " + status);
				if (status != -1) {
					try {
						pushBlockLock.wait();
					} catch (InterruptedException e) { break; }
				} else break;
			}
		}
		if (status != -1) {
			stackSize.increment();
			stack.push(m);
			synchronized (popBlockLock) {
				popBlockLock.notifyAll();
			}
			out.write(0);
			out.flush();
			out.close();
			waitingQueue.remove(state);
			connCount.decrement();
		}
	}

	private void handlePop(int header, InputStream in, OutputStream out) throws IOException {
		connCount.increment();
		ConnWaitingState state = new ConnWaitingState();
		waitingQueue.offer(state);
		int status = 0;
		synchronized (popBlockLock) {
			while (stackSize.intValue() == 0) {
				try {
					status = in.read();
				} catch (SocketTimeoutException ex) {
					status = -1;
				}
				if (status != -1) {
					try {
						popBlockLock.wait();
					} catch (InterruptedException e) { break; }
				} else break;
			}
		}
		if (status != -1) {
			Message m = stack.pop();
			synchronized (pushBlockLock) {
				pushBlockLock.notifyAll();
			}
			OutputStream bout = new BufferedOutputStream(out);
			bout.write(m.data.length);
			bout.write(m.data);
			bout.flush();
			bout.close();
			stackSize.decrement();
			waitingQueue.remove(state);
			connCount.decrement();
		}
	}

	private void handleBusy(OutputStream out) throws IOException {
		ConnWaitingState state = new ConnWaitingState();
		state.lock.lock();
		waitingQueue.offer(state);
		out.write(0xff);
		out.flush();
		out.close();
	}

	private Comparator<ConnWaitingState> getStateAgeComparator() {
		return new Comparator<ConnWaitingState>() {

			@Override
			public int compare(ConnWaitingState o1, ConnWaitingState o2) {
				return -Long.compare(o1.createTime, o2.createTime);
			}
		};
	}

	private boolean checkConnCountIsOK() {
		boolean ok = true;
		if (connCount.intValue() >= CONN_MAX_COUNT) {
			ok = false;
			ConnWaitingState state = waitingQueue.peek();
			if (state != null && state.createTime <= System.currentTimeMillis() - TEN_MINUTES_IN_MS) {
				state = waitingQueue.poll();
				state.lock.unlock();
				ok = true;
			}
		}
		return ok;
	}
	
	public void shutdown() {
		running = false;
	}
	
	class Server extends Thread {
		
		ServerSocket srvsock;
		
		Server() {
			try {
				srvsock = new ServerSocket();
				srvsock.setSoTimeout(30000);
				srvsock.bind(new InetSocketAddress(8080));
			} catch (IOException ex) {
				log.log(Level.WARNING, "server socket", ex);
			}
		}
		
		public void run() {
			while (running) {
				try {
					Socket sock = srvsock.accept();
					sock.setSoTimeout(30000);
					Handler handler = new Handler();
					handler.handle(sock);
				} catch (IOException ex) {
					log.log(Level.WARNING, "accept", ex);
				}
			}
			try {
				srvsock.close();
			} catch (IOException ex) {
			}
		}
		
		class Handler extends Thread {
			
			public void handle(Socket sock) {
				int type = -1;
				try {
					InputStream  in = new BufferedInputStream(sock.getInputStream());
					int header = in.read();
					if (checkConnCountIsOK()) {
						log.log(Level.INFO, "result of header & 0x80: " + (header & 0x80));
						if ((header & 0x80) == 0x80) {
							handlePop(header, in, sock.getOutputStream());
							type = 2;
						} else {
							handlePush(header, in, sock.getOutputStream());
							type = 1;
						}
					} else {
						handleBusy(sock.getOutputStream());
						type = 3;
					}
					log.info(String.format("size %d conns %d type %d", new Object[] {stackSize.intValue(), connCount.intValue(), type}));
				} catch (IOException ex) {
					log.log(Level.WARNING, "connection", ex);
				} finally {
					try {
						sock.close();
					} catch (IOException ex) {
					}
				}
			}
		}
	}
	
	public static void main(String[] args) {
		StackServer s = new StackServer();
		s.run();
		System.out.println("ctrl-c to quit");
		do {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				break;
			}
		} while (true);
	}
}
