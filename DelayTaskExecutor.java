//package hello.hi;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.text.SimpleDateFormat;  

class DelayTaskExecutor {
	private ReentrantLock lock;
	private PriorityQueue<DelayTask> pq;
	private Condition condition;
	private volatile boolean isShutdown = false;
	private volatile boolean isShutdownPendingTasks = false;
	private Thread executeThread;
	private ThreadPoolExecutor executePool = (ThreadPoolExecutor)
Executors.newFixedThreadPool(30);

	public DelayTaskExecutor() {
		pq = new PriorityQueue<>(new Comparator<DelayTask>(){
			@Override
			public int compare(DelayTask a, DelayTask b) {
				Long aTime = new Long(a.scheduledTime);
				Long bTime = new Long(b.scheduledTime);
				return aTime.compareTo(bTime);
			}
		});
		lock = new ReentrantLock();
		condition = lock.newCondition();
	}
	public void put(Runnable task, long delayMs) {
		if(this.isShutdown) {
			return;
		}
		DelayTask delayTask = new DelayTask(task, System.currentTimeMillis() + delayMs);
		lock.lock();
		try {
			pq.add(delayTask);
			if(pq.peek() == delayTask) {
				condition.signal();
			}
		} finally {
			lock.unlock();
		}
			
		
	}

	

	public void startExecute() {
		executePool.submit(() -> {
			while(true) {
				if(executePool.isShutdown()) {
					break;
				}
				try {
					DelayTask task = this.take();
					if(task != null) {
						System.out.println("Time: "+ getCurrentTimeStamp());
						executePool.submit(task.task);
					}
					
				} catch (Exception e) {

				}
			}
		});

	}

	public static String getCurrentTimeStamp() {
	    SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");//dd/MM/yyyy
	    Date now = new Date();
	    String strDate = sdfDate.format(now);
	    return strDate;
	}

	public DelayTask take() {
		// waiting
		lock.lock();
		try {
			while(true) {
				try {
					if(pq.isEmpty()) {
						if(isShutdown) {
							executePool.shutdown();
							return null;
						}
						condition.await();
					} else {
						if(isShutdownPendingTasks) {
							executePool.shutdown();
							return null;
						}
						if(pq.peek().scheduledTime > System.currentTimeMillis()) {

							condition.awaitNanos(TimeUnit.MILLISECONDS.toNanos(pq.peek().scheduledTime - System.currentTimeMillis()));
						} else {
							return pq.poll();
						}
					}
				}catch (InterruptedException e) {

				}
				
			}
			
		} finally {
			lock.unlock();
		}
		

	}

	public void shutdown() {
		System.out.println("Shutdown " + getCurrentTimeStamp());
		isShutdown = true;
		if(pq.isEmpty()) {
			executePool.shutdown();
		}
 	}

	public void shutdownPendingTasks() {
		System.out.println("Shutdown PendingTasks " + getCurrentTimeStamp());
		isShutdownPendingTasks = true;
		executePool.shutdown();
	}

	public void shutdownNow() {
		System.out.println("Shutdown Now " + getCurrentTimeStamp());
		isShutdown = true;
		executePool.shutdownNow();
	}
		
	public static void main(String[] args) throws Exception{
		DelayTaskExecutor executor = new DelayTaskExecutor();
		System.out.println("Program Start Time: "+ executor.getCurrentTimeStamp());
		executor.startExecute();
		
		new Thread(() -> {
			executor.put(() -> {
				try {
					System.out.println("Task1");
					Thread.sleep(10000);
					int i = 1/0;
					System.out.println("Task1 complete");
				} catch(Exception e) {

				}
				
			}, 3000);
		}).start();
		new Thread(() -> {
			executor.put(() -> {
				System.out.println("Task2");
			}, 5000);
		}).start();
		new Thread(() -> {
			executor.put(() -> {
				try {
					System.out.println("Task3");
					Thread.sleep(10000);
					System.out.println("Task3 complete");
				} catch(Exception e) {

				}
			}, 1000);
		}).start();
		
		new Thread(() -> {
			executor.put(() -> {
				System.out.println("Task4");
			}, 4000);
		}).start();
		
		new Thread(() -> {
			executor.put(() -> {
				System.out.println("Task5");
			}, 4000);
		}).start();
		Thread.sleep(4000);
		executor.shutdownPendingTasks();
	}

	class DelayTask {
		Runnable task;
		long scheduledTime;
		DelayTask(Runnable task, long scheduledTime) {
			this.task = task;
			this.scheduledTime = scheduledTime;
		}
	}
}