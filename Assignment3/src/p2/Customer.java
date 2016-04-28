package p2;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Customers are simulation actors that have two fields: a name, and a list
 * of Food items that constitute the Customer's order.  When running, an
 * customer attempts to enter the coffee shop (only successful if the
 * coffee shop has a free table), place its order, and then leave the 
 * coffee shop when the order is complete.
 */
public class Customer implements Runnable {

    //JUST ONE SET OF IDEAS ON HOW TO SET THINGS UP...
	private final String name;
	private final List<Food> order;
	private final int orderNum;
	private final int priority;
	private CountDownLatch countDownLatch;
	
	private static int runningCounter = 0;
	

	/**
	 * You can feel free modify this constructor.  It must take at
	 * least the name and order but may take other parameters if you
	 * would find adding them useful.
	 */
	public Customer(String name, List<Food> order, int priority) {
		this.name = name;
		this.order = order;
		this.orderNum = ++runningCounter;
		this.priority = priority;
		countDownLatch = new CountDownLatch(order.size());
	}


    public String toString() {
		return name;
	}

	/** 
	 * This method defines what an Customer does: The customer attempts to
	 * enter the coffee shop (only successful when the coffee shop has a
	 * free table), place its order, and then leave the coffee shop
	 * when the order is complete.
	 */
	public void run() {
	    
		try{
            Simulation.semaphore.acquire();
		} catch (InterruptedException e) {
            e.printStackTrace();
        }
		
        Simulation.logEvent(SimulationEvent.customerEnteredCoffeeShop(this));
        
        Simulation.logEvent(SimulationEvent.customerPlacedOrder(this, order, orderNum));

        Simulation.priorityCustomerBlockingQueue.add(this);
        
        try{
            countDownLatch.await();
        } 
        catch (InterruptedException e) {
        System.out.println("Stopping the customer");
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Simulation.logEvent(SimulationEvent.customerReceivedOrder(this, order, orderNum));
        // Customer releases his permit and leaves the coffee shop
        Simulation.logEvent(SimulationEvent.customerLeavingCoffeeShop(this));
        Simulation.semaphore.release();

	}

    public int getPriority() {
        return priority;
    }
    
    public List<Food> getOrder() {
        return order;
    }
    
    public int getOrderNum() {
        return orderNum;
    }


    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }
    
    
}